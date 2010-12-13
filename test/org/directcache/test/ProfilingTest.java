package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.directcache.DirectCache;
import org.directcache.ICacheEntry;
import org.directcache.IDirectCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProfilingTest {
	
	private static Logger logger=LoggerFactory.getLogger(ProfilingTest.class);
    
	static IDirectCache cache = null;
	static int objectsSize = 2048;
	static Random generator = new Random();
	static int cacheSize = 490*1024*1024;
	
	public static void allocateMemory() {
		String mb2useFromCommandLine = System.getProperty("mb2use");
		if (mb2useFromCommandLine != null) {
			logger.debug("using mb2useFromCommandLine=" + mb2useFromCommandLine);
			cacheSize = new Integer(mb2useFromCommandLine)*1024*1024;
			logger.debug("cacheSize=" + cacheSize);
		}
		cache = new DirectCache(cacheSize);
		cache.setDefaultDuration(1000);
		logger.debug(cache.toString());
	}
		
	@BeforeClass
	public static void setup() throws Exception {
		allocateMemory();
	}

	private String randomKey() {
		return "key"+generator.nextInt(cacheSize/objectsSize);
	}

	public DummyObject randomObject() {
    	String key = randomKey();
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
		return dummy;
	}
	
	
    public void testAll() throws IOException, ClassNotFoundException {
		Map<String, ICacheEntry> entries = cache.entries();
		
    	Iterator<ICacheEntry> iter = entries.values().iterator();
		while (iter.hasNext()) {
			ICacheEntry entry = iter.next();
			if (!entry.expired()) {
				DummyObject dummy = (DummyObject) cache.retrieveObject(entry.getKey());
				assertNotNull(dummy);
				assertEquals(entry.getKey(), dummy.getName());
			}
		}

		logger.debug("all objects checked");
		logger.debug(cache.toString());	
    }

    @Test
    public void fillCacheUpToOneTenthCapacity() throws Exception {	
		while (cache.usedMemory() < cache.capacity() / 10) {
			DummyObject dummy = new DummyObject(randomKey(), objectsSize*generator.nextInt(5));
			@SuppressWarnings("unused")
			ICacheEntry entry = cache.storeObject(dummy.getName(), dummy);
		}
		logger.debug(cache.toString());
    }
	
    @Test
    public void testAllAgain() throws IOException, ClassNotFoundException {
		testAll();	
    }

    @Test
    public void onlyReads() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    }
    
	private void doSomeReads(int howMany) throws IOException, ClassNotFoundException {
		for (int i = 0; i < howMany; i++) {
    		@SuppressWarnings("unused")
			DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
			
		}
	}
	@Test
	public void onlyWrites() throws Exception {
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);		
	}

	@Test
    public void tenReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(10);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }


    @Test
    public void fiveReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(5);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }
    
    @Test
    public void twoReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(2);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }    


    @Test
    public void oneReadOneWriteOneDelete() throws Exception { 	
    	doSomeReads(1);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }
    

    @Test
    public void testAllOnceAgain() throws IOException, ClassNotFoundException {
    	testAll();
    }

    @AfterClass
    public static void checkBuffer() {
		logger.debug(cache.toString());
   }

}