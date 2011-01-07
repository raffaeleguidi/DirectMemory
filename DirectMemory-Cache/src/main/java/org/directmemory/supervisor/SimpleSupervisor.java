package org.directmemory.supervisor;


import org.directmemory.CacheManager;
import org.directmemory.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSupervisor implements Supervisor {

	private static Logger logger=LoggerFactory.getLogger(SimpleSupervisor.class);
	
	private int checkForExpiredEvery = 100;
	private int count = 0;
	

	/* (non-Javadoc)
	 * @see org.directmemory.supervisor.Supervisor#checkLimits(org.directmemory.CacheStore)
	 */
	public void disposeOverflow(CacheManager cache) {
		logger.debug("disposing overflow");
		cache.heapStore().overflowToNext();
		cache.offHeapStore().overflowToNext();
//		cache.disposeHeapOverflow();
//		cache.disposeOffHeapOverflow();
		
		if (count >= checkForExpiredEvery) {
			count = 0;
			logger.debug("checking expired entries");
			cache.disposeExpired();
		} else {
			count++;
		}
		logger.debug("disposing overflow - done");
	}

	@Override
	public void signalOverflow(Storage storage) {
		storage.overflowToNext();
	}
}
