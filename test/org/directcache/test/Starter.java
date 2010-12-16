package org.directcache.test;

import org.directcache.IDirectCache;
import org.directcache.impl.DirectCache2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Starter {

	private static Logger logger=LoggerFactory.getLogger(Starter.class);

	private static int cacheSize = 10*1024*1024;
	public static void main(String[] args) throws Exception {
		IDirectCache cache = new DirectCache2(cacheSize);
	    //cache.setDefaultDuration(1000);
	    
		
		int objectSize = 1024*4;
		
		Thread last = null;
		
	    for (int i = 0; i < 10; i++) {
			MyThread thread = new MyThread(cache, 12000, objectSize, "thread"+i);
			thread.start();
			last = thread;
			Reader reader = new Reader(cache, "thread"+i, 10000);
			reader.start();
		}
	    
	   
	    while (last.isAlive()) {
	    	Thread.yield();
	    }
	    
    	logger.debug("entries " + cache.entries().size() + " mb " + objectSize*2048/1024/1024);
    	logger.debug(cache.toString());
	}

}
