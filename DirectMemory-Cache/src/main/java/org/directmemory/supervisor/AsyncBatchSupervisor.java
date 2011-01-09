package org.directmemory.supervisor;


import org.directmemory.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncBatchSupervisor implements Supervisor {

	private static Logger logger=LoggerFactory.getLogger(AsyncBatchSupervisor.class);

	public long batchSize = 0;
	long totalCalls = 0;
	
	private abstract class ThreadUsingCache extends Thread {
		public ThreadUsingCache(CacheManager cache) {
			super();
			this.cache = cache;
		}

		public CacheManager cache;
	}
	
	public AsyncBatchSupervisor(int batchSize) {
		this.batchSize = batchSize;
	}

	/* (non-Javadoc)
	 * @see org.directmemory.supervisor.Supervisor#checkLimits(org.directmemory.CacheStore)
	 */
	public void disposeOverflow(CacheManager cache) {
		if (totalCalls++ >= batchSize) {
			totalCalls = 0;
			new ThreadUsingCache(cache) {
				public void run() {
					logger.debug("checking memory limits");
					cache.disposeOverflow();
					logger.debug("checking expired entries");
					cache.disposeExpired();
				}
			}.start();
		}
	}
}
