package org.directcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.directcache.buffer.ThreadSafeDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectCache implements IDirectCache {

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
	private Map<String, ICacheEntry> entries;
	private List<CacheEntry> freeEntries;

	private AtomicInteger capacity;
	private AtomicInteger usedMemory;
	private AtomicInteger memoryInFreeEntries;
	private int defaultDuration=-1;
	
	private void setup(int capacity) {
		this.capacity = new AtomicInteger(capacity);
		entries = new Hashtable<String, ICacheEntry>();
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
	
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#reset()
	 */
	@Override
	public void reset() {
		setup(this.capacity.get());
	}

	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#getDefaultDuration()
	 */
	@Override
	public int getDefaultDuration() {
		return defaultDuration;
	}
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#setDefaultDuration(int)
	 */
	@Override
	public void setDefaultDuration(int defaultDuration) {
		this.defaultDuration = defaultDuration;
	}
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#entries()
	 */
	@Override
	public Map<String, ICacheEntry> entries() {
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

	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#storeObject(java.lang.String, java.io.Serializable)
	 */
	@Override
	public ICacheEntry storeObject(String key, Serializable obj) throws Exception {
		return storeObject(key, obj, defaultDuration);
	}
	
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#storeObject(java.lang.String, java.io.Serializable, int)
	 */
	@Override
	public ICacheEntry storeObject(String key, Serializable obj, int duration) throws Exception {

		logger.info("attempting to remove object with key '" + key + "' - just in case");

		removeObject(key);
		
		logger.info("serializing object with key '" + key + "'");

		byte source[] = serializeObject(obj);
		
		logger.info("object with key '" + key + "' serialized (" + source.length + ") bytes");
		logger.info("buffer size is " + buffer.remaining() + " garbage size is " + memoryInFreeEntries);

		synchronized (buffer) {
//			if (source.length > remaining()) {
//				collectExpired();
//			}
			
			CacheEntry storedEntry = null;
			
			if (source.length <= memoryInFreeEntries.get()) {
				logger.info("storing object with key '" + key + "'");
				storedEntry = (CacheEntry)storeUsingFreeEntries(key, source, duration);	
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
	
	private ICacheEntry expiredEntryLargerThan(int size) {
	
		for (ICacheEntry cacheEntry : entries.values()) {
			if (cacheEntry.size() >= size && cacheEntry.expired()) {
				logger.debug("expired entry found for size " + size);
				return cacheEntry;
			}
		}
		
		logger.debug("No expired entry found for size " + size);
		return null;
	}

	private ICacheEntry storeUsingFreeEntries(String key, byte[] source, int duration) throws Exception {

		logger.debug("storing object with key '" + key + "'");

		CacheEntry freeEntry = (CacheEntry)freeEntryLargerThan(source.length);
		
		if (freeEntry == null) {
			logger.warn("No entries for " + source.length + " bytes, trying LRU");
			freeEntry = (CacheEntry)expiredEntryLargerThan(source.length);
		}

		if (freeEntry == null) {
			logger.warn("No entries for " + source.length + " bytes, trying LRU");
			freeEntry = (CacheEntry)collectLRU(source.length);
		}
		
		if (freeEntry == null) {
			logger.warn("Obtained no LRU entries that fit - exiting");
			return null;
		}
		
		CacheEntry entry = new CacheEntry(key, source.length, freeEntry.getPosition(), duration);
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
	
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#retrieveObject(java.lang.String)
	 */
	@Override
	public Serializable retrieveObject(String key) throws IOException, ClassNotFoundException {

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntry entry = (CacheEntry)entries.get(key);

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
	
	@SuppressWarnings("unused")
	private void collectExpired() {	
		
		logger.debug("Looking for expired entries");

//		List<CacheEntry> expiredList = filter(
//										having(on(CacheEntry.class).expired())
//										, entries.values()
//									);

		List<ICacheEntry> expiredList = new ArrayList<ICacheEntry>();
		
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
	
	private ICacheEntry collectLRU(int bytesToFree) {	

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
	
	
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#removeObject(java.lang.String)
	 */
	@Override
	public ICacheEntry removeObject(String key) {

		logger.info("looking for object with key '" + key + "'");
		
		synchronized (buffer) {
			CacheEntry entry = (CacheEntry)entries.get(key);
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
	
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#remaining()
	 */
	@Override
	public long remaining() {
		return capacity.get()-usedMemory.get();
	}
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#usedMemory()
	 */
	@Override
	public long usedMemory() {
		return usedMemory.get();
	}
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#capacity()
	 */
	@Override
	public long capacity() {
		return buffer.capacity();
	}
	/* (non-Javadoc)
	 * @see org.directcache.IDirectCache#toString()
	 */
	
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
