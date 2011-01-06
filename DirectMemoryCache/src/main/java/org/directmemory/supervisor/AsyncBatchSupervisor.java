package org.directmemory.supervisor;


import org.directmemory.CacheStore;
import org.directmemory.storage.Storage;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class AsyncBatchSupervisor implements Supervisor {

	private static Logger logger=LoggerFactory.getLogger(AsyncBatchSupervisor.class);

	public long batchSize = 0;
	long totalCalls = 0;
	
	private abstract class ThreadUsingCache extends Thread {
		public ThreadUsingCache(CacheStore cache) {
			super();
			this.cache = cache;
		}

		public CacheStore cache;
	}
	
	public AsyncBatchSupervisor(int batchSize) {
		this.batchSize = batchSize;
	}

	/* (non-Javadoc)
	 * @see org.directmemory.supervisor.Supervisor#checkLimits(org.directmemory.CacheStore)
	 */
	public void disposeOverflow(CacheStore cache) {
        Stopwatch stopWatch = SimonManager.getStopwatch("supervisor.asyncbatch.checkLimits");
		Split split = stopWatch.start();
		
		if (totalCalls++ >= batchSize) {
			totalCalls = 0;
			new ThreadUsingCache(cache) {
				public void run() {
					logger.debug("checking memory limits");
					cache.disposeHeapOverflow();
					cache.disposeOffHeapOverflow();
					logger.debug("checking expired entries");
					cache.disposeExpired();
				}
			}.start();
		}
		split.stop();
	}
	
	@Override
	public void signalOverflow(Storage storage) {
		throw new NotImplementedException();
	}
}
