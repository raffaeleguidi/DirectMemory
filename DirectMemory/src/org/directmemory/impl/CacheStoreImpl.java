package org.directmemory.impl;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.directmemory.ICacheEntry;
import org.directmemory.ICacheStore;
import org.directmemory.ICacheSupervisor;
import org.directmemory.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheStoreImpl implements ICacheStore {

	// suggested java memory settings:
	//-Xms512m -Xmx512m -XX:MaxPermSize=512m -Xss512k
	//   - win32 with 4gb - allocated 1250mb
	//   - win32 with 2gb - allocated 490mb

	// have to try this
	//-XX:MaxDirectMemorySize
	//java -d64 -XX:MaxDirectMemorySize=12g -XX:+UseLargePages com.example.MyApp	
	// see http://www.kdgregory.com/index.php?page=java.byteBuffer
	// and http://www.kdgregory.com/programming/java/ByteBuffer_JUG_Presentation.pdf
	
	private static Logger logger=LoggerFactory.getLogger(CacheStoreImpl.class);
	
	private Map<String, ICacheEntry> entries;
	private long offHeapLimit;
	private long inHeapEntriesLimit;
	private AtomicLong usedMemory = new AtomicLong(0);
	private int defaultDuration=-1;
	
	private ICacheSupervisor supervisor;
	
	private void setup(long inHeapEntriesLimit, long offHeapLimit) {
		this.inHeapEntriesLimit = inHeapEntriesLimit;
		this.offHeapLimit = offHeapLimit;
		// these params make things considerably worse
		// entries = new ConcurrentHashMap <String, ICacheEntry>(60000, 0.75F, 30);
		entries = new ConcurrentHashMap <String, ICacheEntry>();
		logger.info("DirectCache allocated with " + offHeapLimit + " bytes buffer");
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
		
	public CacheStoreImpl() {
		// defaults to 50mb
		setup(-1, 50*1024*1024);
	}
	public CacheStoreImpl(long inHeapEntriesLimit, long offHeapLimit) {
		setup(inHeapEntriesLimit, offHeapLimit);
	}
	
	public void reset() {
		setup(inHeapEntriesLimit, offHeapLimit);
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

	public ICacheEntry put(String key, Serializable obj) throws IOException {
		return put(key, obj, defaultDuration);
	}
	
	public ICacheEntry put(String key, Serializable obj, int duration) throws IOException {

		// RpG
		// modify to insert in heap and then call entry.moveOffheap
		// the latter could also be performed later by the supervisor
		
		logger.info("serializing object with key '" + key + "'");

		byte source[] = null;

		try {
			source = SerializationUtils.serializeObject(obj);
			logger.info("object with key '" + key + "' serialized (" + source.length + ") bytes");
		} catch (IOException e) {
			logger.error("error serializing object with key '" + key + "': " + e.getMessage());
			throw new IOException();
		}
		
		if (usedMemory.get() + source.length >= offHeapLimit) {
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

	public Serializable get(String key)  {

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntryImpl entry = (CacheEntryImpl)entries.get(key);

		if (entry == null) {
			logger.info("could not find object with key '" + key + "'");
			return null;
		}
		
		try {
			Serializable obj = null;
			
			if (entry.offHeap()) {
				obj = entry.getPayload();
				logger.info("retrieved object with key '" + key + "' from heap");
			} else {
				byte[] dest = entry.getBuffer();
				if (dest == null) { 
					logger.error("invalid buffer");
					return null;
				}
				obj = SerializationUtils.deserialize(dest);
				logger.info("retrieved object with key '" + key + "' (" + 
						dest.length + " bytes)");
			} 
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
	
	public long  signalCollectExpiredNeeded(long bytesToFree) {	
		
		logger.debug("Looking for expired entries");

		long expiredSize = supervisor.signalCollectExpiredNeeded(this, bytesToFree);

		logger.debug("Collected " + expiredSize +  " bytes of expired entries");		
		
		return expiredSize;
	}	
	
	public ICacheEntry delete(String key) {

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
		return offHeapLimit-usedMemory.get();
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
		sb.append("limit (mb): ");
		sb.append(offHeapLimit()/1024/1024);
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
	public long offHeapLimit() {
		return offHeapLimit;
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
		supervisor.signalWeRetrieved(entry);
	}

	public void signalLRUCollectionNeeded(ICacheStore cache, long bytesToFree) {	
		logger.debug("Signalint LRU collection needed for " + bytesToFree + " bytes");
		long freedBytes = supervisor.signalLRUCollectionNeeded(this, bytesToFree);
		logger.debug("" + freedBytes + " bytes collected out " + bytesToFree + " needed");
	}

}
