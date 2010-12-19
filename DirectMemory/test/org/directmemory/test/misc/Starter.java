package org.directmemory.test.misc;

import java.io.IOException;
import java.util.Calendar;

import org.directmemory.ICacheSupervisor;
import org.directmemory.ICacheStore;
import org.directmemory.impl.CacheStoreImpl;
import org.directmemory.impl.SimpleCacheSupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Starter {

	private static Logger logger=LoggerFactory.getLogger(Starter.class);


	private static void belowLimit(int cacheSize, ICacheSupervisor supervisor) throws IOException, ClassNotFoundException {

		Long startedAt = Calendar.getInstance().getTimeInMillis();
		
		ICacheStore cache = new CacheStoreImpl(cacheSize);
		cache.setSupervisor(supervisor);

		logger.debug("started");
	    for (int i = 0; i < cacheSize / 1024 / 1.25; i++) {
	    	cache.put("test" + i, new DummyObject("test"+i, 1024));
//	    	DummyObject retrObj = (DummyObject)cache.retrieveObject("test"+i);
//		    logger.debug(retrObj.getName());
	    }
	    
	    Long finishedAt = Calendar.getInstance().getTimeInMillis();
		
	    logger.debug("finished in " + (finishedAt - startedAt) + " msecs");
	    logger.debug("" + cache.toString());

	    cache.dispose();

	    logger.debug("" + cache.toString());
	}
	
	private static void aboveLimit(int cacheSize, ICacheSupervisor supervisor) throws IOException, ClassNotFoundException {

		Long startedAt = Calendar.getInstance().getTimeInMillis();
		
		ICacheStore cache = new CacheStoreImpl(cacheSize);
		cache.setSupervisor(supervisor);

		logger.debug("started");
	    for (int i = 0; i < cacheSize / 1024; i++) {
	    	cache.put("test" + i, new DummyObject("test"+i, 1024));
//	    	DummyObject retrObj = (DummyObject)cache.retrieveObject("test"+i);
//		    logger.debug(retrObj.getName());
	    }
	    
	    Long finishedAt = Calendar.getInstance().getTimeInMillis();
		
	    logger.debug("finished in " + (finishedAt - startedAt) + " msecs");
	    logger.debug("" + cache.toString());

	    cache.dispose();

	    logger.debug("" + cache.toString());
	}
	
	
	public static void main(String[] args) throws Exception {
		int cacheSize = 100*1024*1024;

//		belowLimit(cacheSize, new NoopCacheSupervisor());
//		aboveLimit(cacheSize, new NoopCacheSupervisor());
		belowLimit(cacheSize, new SimpleCacheSupervisor());
		aboveLimit(cacheSize, new SimpleCacheSupervisor());
	    
//		
//		Thread last = null;
//		
//	    for (int i = 0; i < 10; i++) {
//			MyThread thread = new MyThread(cache, 12000, objectSize, "thread"+i);
//			thread.start();
//			last = thread;
//			Reader reader = new Reader(cache, "thread"+i, 10000);
//			reader.start();
//		}
//	    
//	   
//	    while (last.isAlive()) {
//	    	Thread.yield();
//	    }
//	    
//    	logger.debug("entries " + cache.entries().size() + " mb " + objectSize*2048/1024/1024);
//    	logger.debug(cache.toString());
	}



}
