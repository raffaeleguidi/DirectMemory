package org.directmemory.supervisor;


import org.directmemory.CacheStore;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
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
	@Override
	public void checkLimits(CacheStore cache) {
        Stopwatch stopWatch = SimonManager.getStopwatch("supervisor.batch.checkLimits");
		Split split = stopWatch.start();
		
		if (totalCalls++ >= batchSize) {
			totalCalls = 0;
			logger.debug("checking memory limits");
			cache.checkHeapMemory();
			cache.checkOffHeapMemory();
		}
		split.stop();
	}
}
