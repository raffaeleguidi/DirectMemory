package org.directmemory.supervisor;


import org.directmemory.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public void disposeOverflow(CacheManager cache) {
		if (totalCalls++ >= batchSize) {
			totalCalls = 0;
			logger.debug("checking memory limits");
			cache.disposeOverflow();
			logger.debug("checking expired entries");
			cache.disposeExpired();
		}
	}
}
