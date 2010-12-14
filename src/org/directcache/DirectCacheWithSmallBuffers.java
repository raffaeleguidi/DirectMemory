package org.directcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.directcache.buffer.CacheEntryWithBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectCacheWithSmallBuffers implements IDirectCache {

	// suggested java memory settings:
	//-Xms512m -Xmx512m -XX:MaxPermSize=512m -Xss512k
	//   - win32 with 4gb - allocated 1250mb
	//   - win32 with 2gb - allocated 490mb

	// have to try this
	//-XX:MaxDirectMemorySize
	//java -d64 -XX:MaxDirectMemorySize=12g -XX:+UseLargePages com.example.MyApp	
	// see http://www.kdgregory.com/index.php?page=java.byteBuffer
	// and http://www.kdgregory.com/programming/java/ByteBuffer_JUG_Presentation.pdf
	
	private static Logger logger=LoggerFactory.getLogger(DirectCacheWithSmallBuffers.class);
	
	private Map<String, ICacheEntry> entries;

	private AtomicInteger capacity;
	private AtomicInteger usedMemory;
	private int defaultDuration=-1;
	
	private void setup(int capacity) {
		this.capacity = new AtomicInteger(capacity);
		entries = new Hashtable<String, ICacheEntry>();
		logger.info("DirectCache allocated with " + capacity + " bytes buffer");
		usedMemory = new AtomicInteger(0);
	}
		
	public DirectCacheWithSmallBuffers() {
		// defaults to 50mb
		setup(50*1024*1024);
	}
	public DirectCacheWithSmallBuffers(int capacity) {
		setup(capacity);
	}
	
	public void reset() {
		setup(this.capacity.get());
	}

	public int getDefaultDuration() {
		return defaultDuration;
	}
	public void setDefaultDuration(int defaultDuration) {
		this.defaultDuration = defaultDuration;
	}
	public Map<String, ICacheEntry> entries() {
		return this.entries;
	}

	private byte[] serializeObject(Serializable obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();
		byte[] b = baos.toByteArray();
		logger.debug("object serialized");
		return b;		
	}

	public ICacheEntry storeObject(String key, Serializable obj) throws Exception {
		return storeObject(key, obj, defaultDuration);
	}
	
	public ICacheEntry storeObject(String key, Serializable obj, int duration) throws Exception {

		logger.info("attempting to remove object with key '" + key + "' - just in case");

		removeObject(key);
		
		logger.info("serializing object with key '" + key + "'");

		byte source[] = serializeObject(obj);
		
		logger.info("object with key '" + key + "' serialized (" + source.length + ") bytes");

//			if (source.length > remaining()) {
//				collectExpired();
//			}
		
		CacheEntryWithBuffer storedEntry = null;
		
		if (source.length >= capacity.get() - usedMemory.get()) {
			ICacheEntry entryToRemove = expiredEntryLargerThan(source.length);
			if (entryToRemove == null) {
				entryToRemove = collectLRU(source.length);
			}
			removeObject(entryToRemove.getKey());
		}

		logger.info("storing object with key '" + key + "'");
		storedEntry = new CacheEntryWithBuffer(key, source, duration);
		synchronized (entries) {
			entries.put(key, storedEntry);
		}
		usedMemory.addAndGet(storedEntry.size());
		return storedEntry;
	}
	
	private ICacheEntry expiredEntryLargerThan(int size) {
	
		synchronized (entries) {
			for (ICacheEntry cacheEntry : entries.values()) {
				if (cacheEntry.size() >= size && cacheEntry.expired()) {
					logger.debug("expired entry found for size " + size);
					return cacheEntry;
				}
			}
		}
		
		logger.debug("No expired entry found for size " + size);
		return null;
	}

	
	public Serializable retrieveObject(String key) throws IOException, ClassNotFoundException {

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntryWithBuffer entry = (CacheEntryWithBuffer)entries.get(key);

		if (entry == null) {
			logger.info("could not find object with key '" + key + "'");
			return null;
		}
		
		byte[] dest = entry.getBuffer();
		try {
			Serializable obj = deserialize(dest);
			logger.info("retrieved object with key '" + key + "' (" + 
					dest.length + " bytes)");
			entry.touch();
			return obj;
		} catch (EOFException ex) {
			logger.error("EOFException deserializing object with key '"
					+ key + "' with size " + entry.size());
			return null;
		}
	}
	
	private Serializable deserialize(byte[] b) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(b);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Serializable obj = (Serializable) ois.readObject();
		ois.close();
		return obj;
	}
	
	public void collectExpired() {	
		
		logger.debug("Looking for expired entries");

//		List<CacheEntry> expiredList = filter(
//										having(on(CacheEntry.class).expired())
//										, entries.values()
//									);

		List<ICacheEntry> expiredList = new Vector<ICacheEntry>();
		
		for (ICacheEntry cacheEntry : entries.values()) {
			if (cacheEntry.expired())
				expiredList.add(cacheEntry);
		}

		logger.debug("Collecting " + expiredList.size() +  " expired entries");
		
		for (ICacheEntry expired : expiredList) {
			removeObject(expired.getKey());
		}

		logger.debug("Collected " + expiredList.size() +  " expired entries");		
	}
	
	public ICacheEntry collectLRU(int bytesToFree) {	

		logger.debug("Attempting LRU collection for " + bytesToFree + " bytes");

//		List<CacheEntry> 
//			LRUItems = 
//				sort(
//					sort(
//						filter(
//							having( 
//									on(
//											CacheEntry.class).size(), 
//											greaterThan(bytesToFree) 
//									), 
//									entries.values()
//						),
//						on(CacheEntry.class).size()
//					),
//					on(CacheEntry.class).lastUsed()
//				);
		
//		LRUItems = 
//				sort(
//					filter(
//						having( 
//								on(
//										CacheEntry.class).size(), 
//										greaterThan(bytesToFree) 
//								), 
//								entries.values()
//						),
//					on(CacheEntry.class).lastUsed()
//				);

		
		// temporary for performance reasons
		for (ICacheEntry entry  : entries.values()) {
			if (entry.size() >= bytesToFree) {
				removeObject(entry.getKey());
				logger.debug("Collected LRU entry " + entry.getKey());
				return entry;
			}			
		}
		logger.warn("No LRU entries to collect for " + bytesToFree + " bytes");
		return null;
	}
	
	
	public ICacheEntry removeObject(String key) {

		logger.info("looking for object with key '" + key + "'");
		
		synchronized (entries) {
			ICacheEntry entry = entries.get(key);
			if (entry == null) {
				logger.info("could not find object with key '" + key + "'");
				return null;
			}
			
			entries.remove(key);
			usedMemory.addAndGet(-entry.size());
			entry.dispose();
			entry = null;
			
			logger.info("object with key '" + key + "' freed");
			return entry;
		}
	}
	
	public int remaining() {
		return capacity.get()-usedMemory.get();
	}
	public int usedMemory() {
		return usedMemory.get();
	}
	public int capacity() {
		return capacity.get();
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("DirectCache {" );
		sb.append("entries: ");
		sb.append(entries().size());
		sb.append(", ");
		sb.append("capacity (mb): ");
		sb.append(capacity()/1024/1024);
		sb.append(", ");
		sb.append("size (mb): ");
		sb.append(usedMemory()/1024/1024);
		sb.append(", ");
		sb.append("remaining (mb): ");
		sb.append(remaining()/1024/1024);
		sb.append("}");
		
		return sb.toString();
	}
	
}
