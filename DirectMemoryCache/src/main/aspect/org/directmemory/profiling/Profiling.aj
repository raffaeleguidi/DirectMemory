package org.directmemory.profiling;

import org.directmemory.CacheEntry;
import org.directmemory.CacheStore;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public aspect Profiling {
	private static Logger logger=LoggerFactory.getLogger(CacheStore.class);
	
	public static int conta = 0;

	pointcut putPointcut(String key, Object object) : call(* org.directmemory.CacheStore.put(..)) && args(key, object);
	pointcut getPointcut(String key) : call(* org.directmemory.CacheStore.get(..)) && args(key);
	pointcut removePointcut(String key) : call(* org.directmemory.CacheStore.remove(..)) && args(key);
	
	CacheEntry around(String key, Object object): putPointcut(key, object) {
		logger.info("entering around put advice");
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.put");
		Split split = stopWatch.start();
		CacheEntry entry = proceed(key, object);
		split.stop();
		return entry;
    }
	
	Object around(String key) : getPointcut(key) {
		logger.info("entering around get advice");
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.get");
		Split split = stopWatch.start();
		Object object = proceed(key);
		split.stop();
		return object;
	}
	
	Object around(String key) : removePointcut(key) {
		logger.info("entering around remove advice");
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.remove");
		Split split = stopWatch.start();
		Object object = proceed(key);
		split.stop();
		return object;
	}
}
