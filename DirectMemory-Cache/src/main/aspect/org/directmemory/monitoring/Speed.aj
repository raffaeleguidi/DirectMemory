package org.directmemory.monitoring;

import java.text.DecimalFormat;

import org.directmemory.cache.CacheEntry;
import org.javasimon.SimonManager;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public aspect Speed {

	public static final String cache_put = "cache.put"; 
	public static final String cache_get = "cache.get"; 
	public static final String cache_remove = "cache.remove"; 
	
    Stopwatch putWatch = SimonManager.getStopwatch(cache_put);
    Stopwatch getWatch = SimonManager.getStopwatch(cache_put);
    Stopwatch removeWatch = SimonManager.getStopwatch(cache_put);
		
	public static final String cache_detail_disposeExpired = "cache.detail.disposeExpired"; 
	public static final String cache_detail_disposeOverflow = "cache.detail.disposeOverflow"; 
	public static final String cache_detail_moveInHeap = "cache.detail.moveInHeap"; 
	public static final String cache_detail_fromHeap = "cache.detail.fromHeap"; 
	public static final String cache_detail_moveOffHeap = "cache.detail.moveOffHeap"; 
	public static final String cache_detail_moveInHeapfromOffHeap = "cache.detail.moveInHeapfromOffHeap"; 
	public static final String cache_detail_getFromHeap = "cache.detail.getFromHeap"; 
	public static final String cache_detail_moveToDisk = "cache.detail.moveToDisk"; 
	public static final String cache_detail_moveInHeapfromDisk = "cache.detail.moveInHeapfromDisk"; 
	public static final String cache_detail_moveToOrientDB = "cache.detail.moveToOrientDB"; 
	public static final String cache_detail_moveInHeapfromOrientDB = "cache.detail.moveInHeapfromOrientDB"; 
	public static final String cache_detail_moveToOrientDBBinary = "cache.detail.moveToOrientDBBinary"; 
	public static final String cache_detail_moveInHeapfromOrientDBBinary = "cache.detail.moveInHeapfromOrientDBBinary"; 
	
	private static Logger logger = LoggerFactory.getLogger(Speed.class);
	
	pointcut putPointcut(String key, Object object) : 
		execution(CacheEntry org.directmemory.cache.CacheManager2.put(String, Object)) && 
		args(key, object);
	
	pointcut getPointcut(String key) : 
		execution(Object org.directmemory.cache.CacheManager2.get(String)) && 
		args(key);
	
	pointcut removePointcut(String key) : 
		execution(CacheEntry org.directmemory.cache.CacheManager2.remove(String)) && 
		args(key);
	
	private long getHits;
	private long getTotalTime;
	private long putHits;
	private long putTotalTime;
	private long removeHits;
	private long removeTotalTime;

	Object around(String key) : getPointcut(key) {
		final long start = System.currentTimeMillis();
		Object object = proceed(key);
		final long end = System.currentTimeMillis();
		getTotalTime+=(end-start);
		getHits++;
		return object;
	}
	
	CacheEntry around(String key, Object object): putPointcut(key, object) {
		final long start = System.currentTimeMillis();
		CacheEntry entry = proceed(key, object);
		final long end = System.currentTimeMillis();
		putTotalTime+=(end-start);
		putHits++;
		return entry;
    }
	
	Object around(String key) : removePointcut(key) {
		final long start = System.currentTimeMillis();
		Object object = proceed(key);
		final long end = System.currentTimeMillis();
		removeTotalTime+=(end-start);
		removeHits++;
		return object;
	}

	pointcut getMeasures() : 
		execution(String org.directmemory.cache.CacheManager2.measures());

	String around() : getMeasures() {
		logger.trace("check: " + thisJoinPoint.toShortString());
		StringBuffer sb = new StringBuffer();
		sb.append("DirectMemory Cache performance: {");

		sb.append("\r\n   ");
		sb.append(getMeasure("put", putHits, putTotalTime));
		sb.append("\r\n   ");
		sb.append(getMeasure("get", getHits, getTotalTime));
		sb.append("\r\n   ");
		sb.append(getMeasure("remove", removeHits, removeTotalTime));

		sb.append("\r\n}");
		return sb.toString();
	}
	
	private static StringBuffer getMeasure(String name, long hits, long totalTime) {
		StringBuffer sb = new StringBuffer();
		sb.append(name);
		sb.append(" hits: ");
		sb.append(hits);
		sb.append(" average time: ");
		if (hits == 0) { 
			sb.append("n/a");
		} else {
			DecimalFormat df = new DecimalFormat("#.##ms");
			sb.append(df.format((double)totalTime/(hits)));
		}
		sb.append(" total time: ");
		sb.append(totalTime);
		sb.append("ms");
		return sb;
	}
}
