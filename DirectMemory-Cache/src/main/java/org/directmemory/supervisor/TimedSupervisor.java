package org.directmemory.supervisor;


import java.util.Date;

import org.directmemory.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimedSupervisor implements Supervisor {

	private static Logger logger=LoggerFactory.getLogger(TimedSupervisor.class);

	public long batchInterval = 0;
	Date lastCheck = new Date();
			
	private abstract class ThreadUsingCache extends Thread {
		public ThreadUsingCache(CacheManager cache) {
			super();
			this.cache = cache;
		}

		public CacheManager cache;
	}
	
	public TimedSupervisor (long batchInterval) {
		this.batchInterval = batchInterval;
	}
	
	/* (non-Javadoc)
	 * @see org.directmemory.supervisor.Supervisor#checkLimits(org.directmemory.CacheStore)
	 */
	public void disposeOverflow(CacheManager cache) {
		long passed = new Date().getTime() - lastCheck.getTime(); 
		if (passed >= batchInterval) {
			lastCheck = new Date();
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
