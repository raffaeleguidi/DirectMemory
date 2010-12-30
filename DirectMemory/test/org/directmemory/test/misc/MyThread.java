package org.directmemory.test.misc;

import java.io.IOException;
import java.util.Random;

import org.directmemory.ICacheStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyThread extends Thread {
	
	private ICacheStore cache = null;
	private int howMany = 0;
	private int objectsSize = 2048;
	private String prefix = "";
	
	private static Logger logger=LoggerFactory.getLogger(MyThread.class);
	
	static Random generator = new Random();
	
	private DummyObject randomObject(int number) {
    	String key = prefix+number;
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize];
		return dummy;
	}
	
	MyThread(ICacheStore cache, int howMany, int objectsSize, String prefix) {
		this.cache = cache;
		this.howMany = howMany;
		this.objectsSize = objectsSize;
		this.prefix = prefix;
	}
	
	public void run() {
		logger.debug("started");
	    for (int i = 0; i < howMany; i++) {
		    DummyObject randomObject = randomObject(i);
			try {
				cache.put(randomObject.getName(), randomObject);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		    logger.debug("" + i + " of " + howMany + " stored");
			Thread.yield();
			try {
				Thread.sleep(generator.nextInt(2));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	    logger.debug("finished: total = " + cache.entries().size());
	}
	
}
