package org.directcache.test;

import java.io.IOException;
import java.util.Random;

import org.directcache.IDirectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reader extends Thread {
	
	private IDirectCache cache = null;
	private int howMany = 0;
	private String prefix = "";

	private static Logger logger=LoggerFactory.getLogger(Reader.class);
	
	static Random generator = new Random();
	
	private String randomKey() {
		return prefix+generator.nextInt(this.howMany);
	}

	Reader(IDirectCache cache, String prefix, int howMany) {
		this.cache = cache;
		this.howMany = howMany;
		this.prefix = prefix;
	}
	
	public void run() {
	    for (int i = 0; i < howMany; i++) {
		    try {
				Thread.sleep(generator.nextInt(2));
				Thread.yield();
		    	String key = randomKey();
				logger.debug("looking for " + key);
				DummyObject obj = (DummyObject)cache.retrieveObject(key);
				if (obj != null) {
					logger.debug("got " + key);
					if (!obj.getName().equals(key)) {
						logger.error("expected: " + key + " found " + obj.getName());
					}
				}
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
		}
		logger.debug("reader done: " + howMany + " objects read");
	}
	
}
