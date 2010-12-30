package org.directmemory.supervisor;


import java.util.Date;

import org.directmemory.CacheStore;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimedSupervisor implements Supervisor {

	private static Logger logger=LoggerFactory.getLogger(TimedSupervisor.class);

	public long batchInterval = 0;
	Date lastCheck = new Date();
			
	private abstract class ThreadUsingCache extends Thread {
		public ThreadUsingCache(CacheStore cache) {
			super();
			this.cache = cache;
		}

		public CacheStore cache;
	}
	
	public TimedSupervisor (long batchInterval) {
		this.batchInterval = batchInterval;
		
	}
	
	/* (non-Javadoc)
	 * @see org.directmemory.supervisor.Supervisor#checkLimits(org.directmemory.CacheStore)
	 */
	@Override
	public void disposeOverflow(CacheStore cache) {
		Stopwatch stopWatch = SimonManager.getStopwatch("supervisor.timed.checkLimits");
		Split split = stopWatch.start();
		long passed = new Date().getTime() - lastCheck.getTime(); 
		if (passed >= batchInterval) {
			lastCheck = new Date();
			new ThreadUsingCache(cache) {
				public void run() {
					logger.debug("checking memory limits");
					cache.disposeHeapOverflow();
					cache.disposeOffHeapOverflow();
				}
			}.start();
		}
		split.stop();
	}
	
}
