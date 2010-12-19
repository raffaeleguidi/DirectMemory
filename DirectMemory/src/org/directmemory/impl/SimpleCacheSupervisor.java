package org.directmemory.impl;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javolution.util.FastMap;

import org.directmemory.ICacheEntry;
import org.directmemory.ICacheSupervisor;
import org.directmemory.ICacheStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleCacheSupervisor implements ICacheSupervisor {
	
	private static Logger logger=LoggerFactory.getLogger(SimpleCacheSupervisor.class);
	private FastMap<String, ICacheEntry> entriesInInsertOrder = new FastMap<String, ICacheEntry>();
//	private LinkedHashMap<String, ICacheEntry> entriesInInsertOrder = new LinkedHashMap<String, ICacheEntry>();
	private AtomicInteger countLruRequests = new AtomicInteger(0);
	private AtomicInteger countCollectExpiredRequests = new AtomicInteger(0);
	private AtomicLong totalBytesToFree = new AtomicLong(0);
	
	@SuppressWarnings("unused")
	private long fakeButFastCollectLRU(ICacheStore store, long bytesToFree) {	

		// not really LRU - that will have to wait
		logger.debug("Attempting LRU collection for " + bytesToFree + " bytes");

		long freedBytes = 0;
		
		for (ICacheEntry entry : store.entries().values()) {
			store.delete(entry.getKey());
			freedBytes += entry.getSize();
			logger.debug("Collected LRU entry " + entry.getKey());
			if (freedBytes >= bytesToFree)
				return freedBytes;
		}
		
		logger.debug("No LRU entries to collect for " + bytesToFree + " bytes");
		return freedBytes;

	}

	private long realButSlowCollectLRU(ICacheStore store, long bytesToFree) {	

		long freedBytes = 0;
		
		synchronized (entriesInInsertOrder) {
			for (ICacheEntry entry : entriesInInsertOrder.values()) {
				store.delete(entry.getKey());
				freedBytes += entry.getSize();
//				logger.debug("Collected LRU entry " + entry.getKey());
				if (freedBytes >= bytesToFree)
					return freedBytes;
			}
		}
		return 0;
	}

	
	/* (non-Javadoc)
	 * @see org.directcache.impl.ICacheSupervisor#signalLRUCollectionNeeded(org.directcache.impl.DirectCacheImpl, int)
	 */
	@Override
	public long signalLRUCollectionNeeded(ICacheStore store, long bytesToFree) {
		if (countLruRequests.compareAndSet(1000, 0)) {
			logger.debug("Collecting LRU for " + bytesToFree);
			return totalBytesToFree.addAndGet(-realButSlowCollectLRU(store, totalBytesToFree.longValue()));
//			return totalBytesToFree.addAndGet(-fakeButFastCollectLRU(cache, totalBytesToFree.longValue()));
		} else {
//			logger.debug("Asking for collecting of LRU for " + bytesToFree);
			countLruRequests.addAndGet(1);
			totalBytesToFree.addAndGet(bytesToFree);
			return 0;
		}
	}


	/* (non-Javadoc)
	 * @see org.directcache.impl.ICacheSupervisor#signalWeDeleted(java.lang.String)
	 */
	@Override
	public void signalWeDeleted(String key) {
		synchronized (entriesInInsertOrder) {
			entriesInInsertOrder.remove(key);
		}
	}


	/* (non-Javadoc)
	 * @see org.directcache.impl.ICacheSupervisor#signalWeInserted(org.directcache.impl.CacheEntryImpl)
	 */
	@Override
	public void signalWeInserted(ICacheEntry newEntry) {
		synchronized (entriesInInsertOrder) {
			entriesInInsertOrder.put(newEntry.getKey(), newEntry);
		}
	}


	/* (non-Javadoc)
	 * @see org.directcache.impl.ICacheSupervisor#signalWeRetrevied(org.directcache.ICacheEntry)
	 */
	@Override
	public void signalWeRetrieved(ICacheEntry entry) {
		synchronized (entriesInInsertOrder) {
			// put it in the bottom of the LRU queue
			entriesInInsertOrder.remove(entry.getKey());
			entriesInInsertOrder.put(entry.getKey(), entry);
		}
		// really useful with the above?
		entry.touch();
	}
	/* (non-Javadoc)
	 * @see org.directcache.impl.ICacheSupervisor#signalReset()
	 */
	@Override
	public void signalReset(){
		entriesInInsertOrder.reset();
		totalBytesToFree.set(0);
		countLruRequests.set(0);
		countCollectExpiredRequests.set(0);
	}

	private long collectExpired(ICacheStore store, long bytesToFree) {
		List<ICacheEntry> expiredList = new Vector<ICacheEntry>();
		
		long bytesFreed = 0;
		
		for (ICacheEntry cacheEntry : store.entries().values()) {
			if (cacheEntry.expired() && bytesFreed < bytesToFree) {
				expiredList.add(cacheEntry);
				bytesToFree+=cacheEntry.getSize();
			}
		}

		logger.debug("Collecting " + expiredList.size() +  " expired entries");
		
		for (ICacheEntry expired : expiredList) {
			store.delete(expired.getKey());
		}
		
		return bytesFreed;
	}
	
	@Override
	public long signalCollectExpiredNeeded(ICacheStore store, long bytesToFree) {
		if (countCollectExpiredRequests.compareAndSet(1000, 0)) {
			logger.debug("expired: removing " + bytesToFree + " bytes");
			return totalBytesToFree.addAndGet(-collectExpired(store, totalBytesToFree.longValue()));
		} else {
			logger.debug("expired: asking for " + bytesToFree + " bytes to free");
			countCollectExpiredRequests.addAndGet(1);
			totalBytesToFree.addAndGet(bytesToFree);
			return 0;
		}
	}



}
