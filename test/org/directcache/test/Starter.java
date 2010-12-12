package org.directcache.test;

import java.util.Random;

import org.directcache.DirectCache;
import org.directcache.IDirectCache;

public class Starter {

	private static int objectsSize = 2048;
	private static int cacheSize = 10*1024*1024;
	static Random generator = new Random();
	
	private static String randomKey() {
		return "key"+generator.nextInt(cacheSize/objectsSize);
	}

	public static DummyObject randomObject() {
    	String key = randomKey();
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
		return dummy;
	}
	
	public static void main(String[] args) throws Exception {
		IDirectCache cache = new DirectCache(cacheSize);
	    cache.setDefaultDuration(1000);
	    DummyObject firstObject = new DummyObject("key0", 10000); // a random object with a 10kb payload
	    cache.storeObject(firstObject.getName(), firstObject);
	    @SuppressWarnings("unused")
		DummyObject retrievedObject = (DummyObject)cache.retrieveObject("key0");
	    cache.removeObject("key0");
	    
	    for (int i = 0; i < 10000; i++) {
		    DummyObject randomObject = randomObject();
			cache.storeObject(randomObject.getName(), randomObject);
		}
	    
	}

}
