package org.directmemory.supervisor;


import org.directmemory.CacheStore;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSupervisor implements Supervisor {

	private static Logger logger=LoggerFactory.getLogger(SimpleSupervisor.class);

	/* (non-Javadoc)
	 * @see org.directmemory.supervisor.Supervisor#checkLimits(org.directmemory.CacheStore)
	 */
	public void disposeOverflow(CacheStore cache) {
        Stopwatch stopWatch = SimonManager.getStopwatch("supervisor.asimple.checkLimits");
		Split split = stopWatch.start();
		
		logger.debug("checking memory limits");
		cache.disposeHeapOverflow();
		cache.disposeOffHeapOverflow();
		logger.debug("checking expired entries");
		cache.disposeExpired();
		split.stop();
	}
}
