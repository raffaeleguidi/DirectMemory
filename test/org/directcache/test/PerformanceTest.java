package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directcache.ICacheEntry;
import org.directcache.IDirectCache;
import org.directcache.impl.DirectCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PerformanceTest {
	
	private static Logger logger=LoggerFactory.getLogger(PerformanceTest.class);

	@Rule
    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
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
	
    @Test
    @Required(max = 200)
    public void firstAndLargestItem() throws Exception { 	
    	DummyObject firstObject = new DummyObject("key0", objectsSize*5);
    	cache.storeObject(firstObject.getName(), firstObject);
    	assertEquals(cache.entries().size(), 1);
    	DummyObject retrievedObject = (DummyObject)cache.retrieveObject("key0");
    	assertEquals(firstObject.getName(), retrievedObject.getName());
		logger.debug(cache.toString());
    }
    
	//@Test
    public void fillCacheUpTo90Percent() throws Exception {	
		while (cache.usedMemory() < cache.capacity() - (cache.capacity()/100*10)) {
			DummyObject dummy = new DummyObject(randomKey(), objectsSize*generator.nextInt(5));
			@SuppressWarnings("unused")
			ICacheEntry entry = cache.storeObject(dummy.getName(), dummy, -1); //no expiry
		}
		logger.debug(cache.toString());
		cache.reset();
    }
	
    @Test
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
    public void fillCacheUpToHalfCapacity() throws Exception {	
		while (cache.usedMemory() < cache.capacity() / 2) {
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
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 650, average = 7)
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
    @PerfTest(duration = 500, threads = 5)
	public void onlyWrites() throws Exception {
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);		
	}

	@Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 4.5)
    public void tenReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(10);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }


    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 9)
    public void fiveReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(5);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 2500, average = 15)
    public void twoReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(2);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }    


    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1000, average = 17)
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