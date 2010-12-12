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

import org.directcache.buffer.ThreadSafeDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectCache {

	// suggested java memory settings:
	//-Xms512m -Xmx512m -XX:MaxPermSize=512m -Xss512k
	//   - win32 with 4gb - allocated 1250mb
	//   - win32 with 2gb - allocated 490mb

	// have to try this
	//-XX:MaxDirectMemorySize
	//java -d64 -XX:MaxDirectMemorySize=12g -XX:+UseLargePages com.example.MyApp	
	// see http://www.kdgregory.com/index.php?page=java.byteBuffer
	// and http://www.kdgregory.com/programming/java/ByteBuffer_JUG_Presentation.pdf
	
	private static Logger logger=LoggerFactory.getLogger(DirectCache.class);
	
	private ThreadSafeDirectBuffer buffer;
	private Map<String, CacheEntry> entries;
	private List<CacheEntry> freeEntries;

	private AtomicInteger capacity;
	private AtomicInteger usedMemory;
	private AtomicInteger memoryInFreeEntries;
	private int defaultDuration=-1;
	
	private void setup(int capacity) {
		this.capacity = new AtomicInteger(capacity);
		entries = new Hashtable<String, CacheEntry>();
		freeEntries = new Vector<CacheEntry>();
		if (buffer != null) buffer.clear();
		buffer = new ThreadSafeDirectBuffer(capacity);
		logger.info("DirectCache allocated with " + capacity + " bytes buffer");
		freeEntries.add(new CacheEntry("directcachefirstitem",capacity,0));
		memoryInFreeEntries = this.capacity;
		usedMemory = new AtomicInteger(0);
	}
		
	public DirectCache() {
		// defaults to 50mb
		setup(50*1024*1024);
	}
	public DirectCache(int capacity) {
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
	public Map<String, CacheEntry> entries() {
		return entries;
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

	public CacheEntry storeObject(String key, Serializable obj) throws Exception {
		return storeObject(key, obj, defaultDuration);
	}
	
	public CacheEntry storeObject(String key, Serializable obj, int duration) throws Exception {

		logger.info("attempting to remove object with key '" + key + "' - just in case");

		removeObject(key);
		
		logger.info("serializing object with key '" + key + "'");

		byte source[] = serializeObject(obj);
		
		logger.info("object with key '" + key + "' serialized (" + source.length + ") bytes");
		logger.info("buffer size is " + buffer.remaining() + " garbage size is " + memoryInFreeEntries);

		synchronized (buffer) {
			if (source.length > remaining()) {
				collectExpired();
			}
			
			CacheEntry storedEntry = null;
			
			if (source.length <= memoryInFreeEntries.get()) {
				logger.info("storing object with key '" + key + "'");
				storedEntry = storeUsingFreeEntries(key, source, duration);	
			} else {
				logger.error("there's no room for object with key '" + key + "'");
				// how's that?!?...
			}
			if (storedEntry != null) {
				usedMemory.addAndGet(storedEntry.size);
			}
			return storedEntry;
		}
	}

	private CacheEntry freeEntryLargerThan(int size) {

//		logger.debug("Executing lambda query for objects larger than " + size);

//		List<CacheEntry> entriesThatFit =
//				filter(
//					having( 
//							on(
//									CacheEntry.class).size(), 
//									greaterThan(size) 
//							), 
//					freeEntries
//				);
		
		for (CacheEntry cacheEntry : freeEntries) {
			if (cacheEntry.size >= size) {
				logger.debug("Free entry found for size " + size);
				return cacheEntry;
			}
		}
		
		logger.debug("No free entry found for size " + size);
		return null;
		
//		List<CacheEntry> entriesThatFit =
//			sort(
//				filter(
//					having( 
//							on(
//									CacheEntry.class).size(), 
//									greaterThan(size) 
//							), 
//					freeEntries
//				)
//				,
//				on(CacheEntry.class).size()
//			);

//		logger.debug("Finished lambda query");
//		
//		return entriesThatFit;
	}
	
	private CacheEntry storeUsingFreeEntries(String key, byte[] source, int duration) throws Exception {

		logger.debug("storing object with key '" + key + "'");

		CacheEntry freeEntry = freeEntryLargerThan(source.length);
		
		if (freeEntry == null) {
			logger.warn("No entries for " + source.length + " bytes, trying LRU");
			freeEntry = collectLRU(source.length);
		}
		
		if (freeEntry == null) {
			logger.warn("Obtained no LRU entries that fit - exiting");
			return null;
		}
		
		CacheEntry entry = new CacheEntry(key, source.length, freeEntry.position, duration);
		entries.put(key, entry);
		freeEntry.size -= source.length;
		memoryInFreeEntries.addAndGet(-source.length);
		freeEntry.position += source.length;
		buffer.put(source, entry.position);
		
		// not really an optimized garbage collection algorythm but...
		if (freeEntry.size == 0) 
			freeEntries.remove(freeEntry);

		logger.info("Reusing entry " + key);

		return entry;
		
	}
	
	public Serializable retrieveObject(String key) throws IOException, ClassNotFoundException {

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntry entry = entries.get(key);

		if (entry == null) {
			logger.info("could not find object with key '" + key + "'");
			return null;
		}

		// mah...
		synchronized (buffer) {
			byte[] dest = buffer.get(entry.position, entry.size);
			try {
				Serializable obj = deserialize(dest);
				logger.info("retrieved object with key '" + key + "' (" + 
						dest.length + " bytes)");
				entry.touch();
				return obj;
			} catch (EOFException ex) {
				logger.error("EOFException deserializing object with key '"
						+ key + "' at position " + entry.position
						+ " with size " + entry.size);
				return null;
			}
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

		List<CacheEntry> expiredList = new Vector<CacheEntry>();
		
		for (CacheEntry cacheEntry : entries.values()) {
			if (cacheEntry.expired())
				expiredList.add(cacheEntry);
		}

		logger.debug("Collecting " + expiredList.size() +  " expired entries");
		
		for (CacheEntry expired : expiredList) {
			removeObject(expired.getKey());
		}

		logger.debug("Collected " + expiredList.size() +  " expired entries");		
	}
	
	public CacheEntry collectLRU(int bytesToFree) {	

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

		for (CacheEntry entry  : entries.values()) {
			if (entry.size >= bytesToFree) {
				removeObject(entry.getKey());
				logger.debug("Collected LRU entry " + entry.getKey());
				return entry;
			}			
		}
		logger.warn("No LRU entries to collect for " + bytesToFree + " bytes");
		return null;
	}
	
	
	public CacheEntry removeObject(String key) {

		logger.info("looking for object with key '" + key + "'");
		
		synchronized (buffer) {
			CacheEntry entry = entries.get(key);
			if (entry == null) {
				logger.info("could not find object with key '" + key + "'");
				return null;
			}
			
			entries.remove(key);
			freeEntries.add(entry);
			memoryInFreeEntries.addAndGet(entry.size);
			usedMemory.addAndGet(-entry.size);
			
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
		return buffer.capacity();
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
