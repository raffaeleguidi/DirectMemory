package org.directcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
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
	private long usedMemory;
	private int defaultDuration=-1;
	
	private void setup(int capacity) {
		this.capacity = new AtomicInteger(capacity);
		entries = new ConcurrentHashMap <String, ICacheEntry>(10000, 0.75F, 20);
		logger.info("DirectCache allocated with " + capacity + " bytes buffer");
		usedMemory = 0;
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

	public ICacheEntry storeObject(String key, Serializable obj) {
		return storeObject(key, obj, defaultDuration);
	}
	
	public ICacheEntry storeObject(String key, Serializable obj, int duration) {

		//logger.info("attempting to remove object with key '" + key + "' - just in case");

		//removeObject(key);
		
		logger.info("serializing object with key '" + key + "'");

		byte source[] = null;

		try {
			source = serializeObject(obj);
			logger.info("object with key '" + key + "' serialized (" + source.length + ") bytes");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
			logger.error("error serializing object with key '" + key + "': " + e2.getMessage());
		}
		
//  lasciamo stare fino a che non gestiamo la capacity
//		if (usedMemory.get() >= capacity.get()) {
//			ICacheEntry entryToRemove = expiredEntryLargerThan(source.length);
//			if (entryToRemove == null) {
//				logger.info("collecting LRU item - no room for entry with key '" + key + "'");
//				entryToRemove = collectLRU(source.length);
//			}
//		}
		
		CacheEntryWithBuffer storedEntry = null;
		
		try {				
			storedEntry = new CacheEntryWithBuffer(key, source, duration);
			logger.debug("created entry with key '" + key + "'");
		} catch (OutOfMemoryError e) {
			logger.debug("collecting expired item - no room for entry with key '" + key + "'");
			ICacheEntry entryToRemove = expiredEntryLargerThan(source.length);
			if (entryToRemove != null) {
				removeObject(entryToRemove.getKey());
			} else {
				logger.debug("collecting LRU item - no room for entry with key '" + key + "'");
				collectLRU(source.length);
			}
			
			try {
				storedEntry = new CacheEntryWithBuffer(key, source, duration);
				logger.debug("created entry with key '" + key + "'");
			} catch (OutOfMemoryError e1) {
				logger.debug("no room for entry with key '" + key + "' - attempting expired collection");
				collectExpired();
				logger.debug("no room for entry with key '" + key + "' - attempting expired collection");
				storedEntry = new CacheEntryWithBuffer(key, source, duration);
			}
		} 

		usedMemory+=storedEntry.size();
//		usedMemory.addAndGet(storedEntry.size());

		entries.put(key, storedEntry);

		logger.debug("stored entry with key '" + key + "'");

		return storedEntry;
	}
	
	private ICacheEntry expiredEntryLargerThan(int size) {
	
//		synchronized (entries) {
			for (ICacheEntry cacheEntry : entries.values()) {
				if (cacheEntry.size() >= size && cacheEntry.expired()) {
					logger.debug("expired entry found for size " + size);
					return cacheEntry;
				}
			}
//		}
		
		logger.debug("No expired entry found for size " + size);
		return null;
	}

	public void collectLRU(int bytesToFree) {	

		logger.debug("Attempting LRU collection for " + bytesToFree + " bytes");

		long freedBytes = 0;
		for (ICacheEntry entry : entries.values()) {
			freedBytes += entry.getSize();
			removeObject(entry.getKey());
			logger.debug("Collected LRU entry " + entry.getKey());
			if (freedBytes >= bytesToFree)
				return;
		}
		
		logger.debug("No LRU entries to collect for " + bytesToFree + " bytes");
	}
	
	public Serializable retrieveObject(String key)  {

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntryWithBuffer entry = (CacheEntryWithBuffer)entries.get(key);

		if (entry == null) {
			logger.info("could not find object with key '" + key + "'");
			return null;
		}
		
		byte[] dest = entry.getBuffer();
		
		if (dest == null) { 
			logger.error("invalid buffer");
			return null;
		}
		
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
		} catch (IOException e) {
			logger.error("IOException deserializing object with key '"
					+ key + "' with size " + entry.size());
		} catch (ClassNotFoundException e) {
			logger.error("ClassNotFoundException deserializing object with key '"
					+ key + "' with size " + entry.size());
		}
		return null;
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
		
//		synchronized (entries) {
			for (ICacheEntry cacheEntry : entries.values()) {
				if (cacheEntry.expired())
					expiredList.add(cacheEntry);
			}
//		}
		logger.debug("Collecting " + expiredList.size() +  " expired entries");
		
		for (ICacheEntry expired : expiredList) {
			removeObject(expired.getKey());
		}

		logger.debug("Collected " + expiredList.size() +  " expired entries");		
	}	
	
	public ICacheEntry removeObject(String key) {

		logger.info("trying to remove entry with key '" + key + "'");	

		ICacheEntry entry = null;
		
//		synchronized (entries) {
			entry = entries.remove(key);
//		}
		
		if (entry != null) {
//			usedMemory.addAndGet(-entry.size());
			usedMemory-=entry.size();
			entry.dispose();
			logger.info("object with key '" + key + "' disposed");
		}

		return entry;
	}
	
	public long remaining() {
		return capacity.longValue()-usedMemory;
	}
	public long usedMemory() {
		return usedMemory;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("DirectCache {" );
		sb.append("entries: ");
		sb.append(entries().size());
		sb.append(", ");
//		sb.append("capacity (mb): ");
//		sb.append(capacity()/1024/1024);
//		sb.append(", ");
		sb.append("size (mb): ");
		sb.append(usedMemory()/1024/1024);
		sb.append(", ");
		sb.append("remaining (mb): ");
		sb.append(remaining()/1024/1024);
		sb.append("}");
		
		return sb.toString();
	}

	@Override
	public long capacity() {
		// deve ritornare -XX:MaxDirectMemorySize 
		return 512*1024*1024L;
	}
	
}
