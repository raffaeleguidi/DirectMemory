package org.directmemory.profiling;

import java.text.DecimalFormat;

import org.directmemory.CacheEntry;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public aspect Profiling {
	private static Logger logger=LoggerFactory.getLogger(Profiling.class);
	
	pointcut putPointcut(String key, Object object) : 
		execution(CacheEntry org.directmemory.CacheStore.put(String, Object)) && 
		args(key, object);
	
	pointcut getPointcut(String key) : 
		execution(Object org.directmemory.CacheStore.get(String)) && 
		args(key);
	
	pointcut removePointcut(String key) : 
		execution(CacheEntry org.directmemory.CacheStore.remove(String)) && 
		args(key);

	pointcut disposeExpiredPointcut() : 
		execution(void org.directmemory.CacheStore.disposeExpired());
	
	pointcut moveOffHeapPointcut(CacheEntry entry) : 
		execution(boolean org.directmemory.storage.OffHeapStorage.moveIn(CacheEntry)) && 
		args(entry);

	pointcut moveToDiskPointcut(CacheEntry entry) : 
		execution(boolean org.directmemory.storage.FileStorage.moveIn(CacheEntry)) && 
		args(entry);

	pointcut fromOffHeapPointcut(CacheEntry entry) : 
		execution(boolean org.directmemory.storage.OffHeapStorage.moveToHeap(CacheEntry)) && 
		args(entry);

	pointcut fromDiskPointcut(CacheEntry entry) : 
		execution(boolean org.directmemory.storage.FileStorage.moveToHeap(CacheEntry)) && 
		args(entry);

	pointcut disposeOffHeapOverflowPointcut() : 
		execution(void org.directmemory.CacheStore.disposeOffHeapOverflow());

	pointcut moveInHeapPointcut(CacheEntry entry) : 
		execution(boolean org.directmemory.CacheStore.moveInHeap(CacheEntry)) && args(entry);

	pointcut displayTimingsPointcut() : 
		execution(void org.directmemory.CacheStore.displayTimings());
	
	Object around(String key) : getPointcut(key) {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.get");
		Split split = stopWatch.start();
		Object object = proceed(key);
		split.stop();
		return object;
	}
	
	CacheEntry around(String key, Object object): putPointcut(key, object) {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.put");
		Split split = stopWatch.start();
		CacheEntry entry = proceed(key, object);
		split.stop();
		return entry;
    }
	
	Object around(String key) : removePointcut(key) {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.remove");
		Split split = stopWatch.start();
		Object object = proceed(key);
		split.stop();
		return object;
	}

	Object around() : disposeExpiredPointcut() {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.disposeExpired");
		Split split = stopWatch.start();
		Object object = proceed();
		split.stop();
		return object;
	}

	boolean around(CacheEntry entry) : moveOffHeapPointcut(entry) {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.moveOffHeap");
		Split split = stopWatch.start();
		boolean retVal = proceed(entry);
		split.stop();
		return retVal;
	}

	boolean around(CacheEntry entry) : moveToDiskPointcut(entry) {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.moveToDisk");
		Split split = stopWatch.start();
		boolean retVal = proceed(entry);
		split.stop();
		return retVal;
	}
	
	boolean around(CacheEntry entry) : fromDiskPointcut(entry) {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.moveInHeapfromDisk");
		Split split = stopWatch.start();
		boolean retVal = proceed(entry);
		split.stop();
		return retVal;
	}
	
	boolean around(CacheEntry entry) : fromOffHeapPointcut(entry) {
		logger.debug("check: " + thisJoinPoint.toShortString());
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.moveInHeapfromOffHeap");
		Split split = stopWatch.start();
		boolean retVal = proceed(entry);
		split.stop();
		return retVal;
	}
	
	private static String getTiming(Stopwatch sw) {
		Double average = ((double)sw.getTotal() / (double)sw.getCounter() /1000000);
		DecimalFormat df = new DecimalFormat("#.##");
		return 
			(sw.getName() + " " + sw.getCounter() + " hits - average " + df.format(average) + " - max active:" + sw.getMaxActive() + " total time " + (sw.getTotal()/1000000))
			+ " - " + sw.toString();
	}
	
	void around() : displayTimingsPointcut() {
		logger.debug("check: " + thisJoinPoint.toShortString());
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.put")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.get")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.remove")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.disposeExpired")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.moveOffHeap")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.moveToDisk")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.moveInHeapfromOffHeap")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.moveInHeapfromDisk")));
		sb.append("\r\n}");
		logger.info(sb.toString());
	}
}
