package org.directcache.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.directcache.ICacheEntry;
import org.directcache.ICacheSupervisor;
import org.directcache.IDirectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectCacheImpl implements IDirectCache {

	// suggested java memory settings:
	//-Xms512m -Xmx512m -XX:MaxPermSize=512m -Xss512k
	//   - win32 with 4gb - allocated 1250mb
	//   - win32 with 2gb - allocated 490mb

	// have to try this
	//-XX:MaxDirectMemorySize
	//java -d64 -XX:MaxDirectMemorySize=12g -XX:+UseLargePages com.example.MyApp	
	// see http://www.kdgregory.com/index.php?page=java.byteBuffer
	// and http://www.kdgregory.com/programming/java/ByteBuffer_JUG_Presentation.pdf
	
	private static Logger logger=LoggerFactory.getLogger(DirectCacheImpl.class);
	
	private Map<String, ICacheEntry> entries;
	private long capacity;
	private AtomicLong usedMemory = new AtomicLong(0);
	private int defaultDuration=-1;
	
	private ICacheSupervisor supervisor;
	
	private void setup(long capacity) {
		this.capacity = capacity;
		// these params make things considerably worse
//		entries = new ConcurrentHashMap <String, ICacheEntry>(60000, 0.75F, 30);
		entries = new ConcurrentHashMap <String, ICacheEntry>();
		logger.info("DirectCache allocated with " + capacity + " bytes buffer");
		usedMemory = new AtomicLong(0);
		if (supervisor == null) {
			supervisor = new SimpleCacheSupervisor();
		} else {
			supervisor.signalReset();
		}
	}
		
	@Override
	public void dispose() {
		entries.clear();
		usedMemory.set(0);
		// something more to free buffers?
	}
		
	public DirectCacheImpl() {
		// defaults to 50mb
		setup(50*1024*1024);
	}
	public DirectCacheImpl(int capacity) {
		setup(capacity);
	}
	
	public void reset() {
		setup(capacity);
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

	public ICacheEntry storeObject(String key, Serializable obj) throws IOException {
		return storeObject(key, obj, defaultDuration);
	}
	
	public ICacheEntry storeObject(String key, Serializable obj, int duration) throws IOException {

		logger.info("serializing object with key '" + key + "'");

		byte source[] = null;

		try {
			source = serializeObject(obj);
			logger.info("object with key '" + key + "' serialized (" + source.length + ") bytes");
		} catch (IOException e) {
			logger.error("error serializing object with key '" + key + "': " + e.getMessage());
			throw new IOException();
		}
		
		if (usedMemory.get() + source.length >= capacity) {
			logger.debug("we are over capacity: removing one expired entry - no room for entry with key '" + key + "'");
			makeRoomForObject(source.length);
		}
		
		CacheEntryImpl newEntry = null;
		
		while  (newEntry == null) {
			logger.debug("trying to create entry for object with key '" + key + "'");
			newEntry = CacheEntryImpl.allocate(key, source, duration);
			if (newEntry == null) {
				makeRoomForObject(source.length);
			}
		}

		entries.put(key, newEntry);
		signalWeInserted(newEntry);
		usedMemory.addAndGet(source.length);

		source = null;

		logger.debug("stored entry with key '" + key + "'");

		return newEntry;
	}

	private void makeRoomForObject(long size) {

		long bytesFreed = signalCollectExpiredNeeded(size);

		if ( bytesFreed >= size || bytesFreed == -1 ) {
			return;
		} else {
			logger.debug("collecting LRU item");
			signalLRUCollectionNeeded(this, size- bytesFreed);
		}
	}
	
//	private ICacheEntry expiredEntryLargerThan(int size) {
//	
//		for (ICacheEntry cacheEntry : entries.values()) {
//			if (cacheEntry.size() >= size && cacheEntry.expired()) {
//				logger.debug("expired entry found for size " + size);
//				return cacheEntry;
//			}
//		}
//		
//		logger.debug("No expired entry found for size " + size);
//		return null;
//	}

	public Serializable retrieveObject(String key)  {

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntryImpl entry = (CacheEntryImpl)entries.get(key);

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
			
			signalWeRetrevied(entry);

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
	
	public long  signalCollectExpiredNeeded(long bytesToFree) {	
		
		logger.debug("Looking for expired entries");

		long expiredSize = supervisor.signalCollectExpiredNeeded(this, bytesToFree);

		logger.debug("Collected " + expiredSize +  " bytes of expired entries");		
		
		return expiredSize;
	}	
	
	public ICacheEntry removeObject(String key) {

		logger.info("trying to remove entry with key '" + key + "'");	

		ICacheEntry entry = null;
		
		entry = entries.remove(key);
		signalWeDeleted(key);
		
		if (entry != null) {
			usedMemory.addAndGet(-entry.size());
			entry.dispose();
			logger.info("object with key '" + key + "' disposed");
		}

		return entry;
	}
	
	public long remaining() {
		return capacity-usedMemory.get();
	}
	public long usedMemory() {
		return usedMemory.get();
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("DirectCacheImpl {" );
		sb.append("supervisor: ");
		sb.append(supervisor.getClass());
		sb.append(", entries: ");
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

	@Override
	public long capacity() {
		return capacity;
	}

	public void setSupervisor(ICacheSupervisor supervisor) {
		this.supervisor = supervisor;
	}

	public ICacheSupervisor getSupervisor() {
		return supervisor;
	}
	
	private void signalWeDeleted(String key) {
		supervisor.signalWeDeleted(key);
	}

	private void signalWeInserted(CacheEntryImpl newEntry) {
		supervisor.signalWeInserted(newEntry);
	}

	private void signalWeRetrevied(ICacheEntry entry) {
		supervisor.signalWeRetrevied(entry);
	}

	public void signalLRUCollectionNeeded(IDirectCache cache, long bytesToFree) {	

		// not really LRU - that will have to wait
		logger.debug("Signalinc LRU collection needed for " + bytesToFree + " bytes");
		
		long freedBytes = supervisor.signalLRUCollectionNeeded(this, bytesToFree);

		logger.debug("" + freedBytes + " bytes collected out " + bytesToFree + " needed");
	}

}
