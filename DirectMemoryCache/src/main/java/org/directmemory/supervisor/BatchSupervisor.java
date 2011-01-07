package org.directmemory.supervisor;


import org.directmemory.CacheStore;
import org.directmemory.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class BatchSupervisor implements Supervisor {

	private static Logger logger=LoggerFactory.getLogger(BatchSupervisor.class);

	public long batchSize = 0;
	long totalCalls = 0;
	
	public BatchSupervisor(int batchSize) {
		this.batchSize = batchSize;
	}

	/* (non-Javadoc)
	 * @see org.directmemory.supervisor.Supervisor#checkLimits(org.directmemory.CacheStore)
	 */
	public void disposeOverflow(CacheStore cache) {
		if (totalCalls++ >= batchSize) {
			totalCalls = 0;
			logger.debug("checking memory limits");
			cache.disposeHeapOverflow();
			cache.disposeOffHeapOverflow();
			logger.debug("checking expired entries");
			cache.disposeExpired();
		}
	}
	
	@Override
	public void signalOverflow(Storage storage) {
		throw new NotImplementedException();
	}
}
