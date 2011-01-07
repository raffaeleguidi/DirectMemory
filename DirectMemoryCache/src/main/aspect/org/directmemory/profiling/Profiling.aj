package org.directmemory.profiling;

import java.text.DecimalFormat;

import org.directmemory.CacheEntry;
import org.directmemory.CacheStore;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public aspect Profiling {
	private static Logger logger=LoggerFactory.getLogger(CacheStore.class);
	
	pointcut putPointcut(String key, Object object) : call(* org.directmemory.CacheStore.put(..)) && args(key, object);
	pointcut getPointcut(String key) : call(* org.directmemory.CacheStore.get(..)) && args(key);
	pointcut removePointcut(String key) : call(* org.directmemory.CacheStore.remove(..)) && args(key);

	pointcut disposeExpiredPointcut() : call(* org.directmemory.CacheStore.disposeExpired(..));
	pointcut disposeHeapOverflowPointcut() : call(* org.directmemory.CacheStore.disposeHeapOverflow(..));
	pointcut disposeOffHeapOverflowPointcut() : call(* org.directmemory.CacheStore.disposeOffHeapOverflow(..));

	pointcut moveInHeapPointcut(CacheEntry entry) : call(* org.directmemory.CacheStore.moveInHeap(..)) && args(entry);

	pointcut displayTimingsPointcut() : call(* org.directmemory.CacheStore.displayTimings(..));
	
	Object around(String key) : getPointcut(key) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.get");
		Split split = stopWatch.start();
		Object object = proceed(key);
		split.stop();
		return object;
	}
	
	CacheEntry around(String key, Object object): putPointcut(key, object) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.put");
		Split split = stopWatch.start();
		CacheEntry entry = proceed(key, object);
		split.stop();
		return entry;
    }
	
	Object around(String key) : removePointcut(key) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.remove");
		Split split = stopWatch.start();
		Object object = proceed(key);
		split.stop();
		return object;
	}

	Object around() : disposeExpiredPointcut() {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.disposeExpired");
		Split split = stopWatch.start();
		Object object = proceed();
		split.stop();
		return object;
	}

	Object around() : disposeHeapOverflowPointcut() {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.disposeHeapOverflow");
		Split split = stopWatch.start();
		Object object = proceed();
		split.stop();
		return object;
	}

	Object around() : disposeOffHeapOverflowPointcut() {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.disposeOffHeapOverflow");
		Split split = stopWatch.start();
		Object object = proceed();
		split.stop();
		return object;
	}
	
//	void around(CacheEntry entry): moveInHeapPointcut(entry) {
//        Stopwatch stopWatch = SimonManager.getStopwatch("cache.detail.moveInHeap");
//		Split split = stopWatch.start();
//		proceed(entry);
//		split.stop();
//    }	
	

	private static String getTiming(Stopwatch sw) {
		
		Double average = ((double)sw.getTotal() / (double)sw.getCounter() /1000000);
		DecimalFormat df = new DecimalFormat("#.##");
		return 
			(sw.getName() + " " + sw.getCounter() + " hits - average " + df.format(average) + " - max active:" + sw.getMaxActive() + " total time " + (sw.getTotal()/1000000))
			+ " - " + sw.toString();
	}
	
	void around() : displayTimingsPointcut() {
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
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.disposeHeapOverflow")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.disposeOffHeapOverflow")));
		sb.append("\r\n   ");
		sb.append(getTiming(SimonManager.getStopwatch("cache.detail.moveInHeap")));
		sb.append("\r\n}");
		logger.info(sb.toString());
	}
}
