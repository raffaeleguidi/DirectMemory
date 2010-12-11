package org.directcache.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directcache.CacheEntry;
import org.directcache.DirectCache;
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
    
	static DirectCache cache = null;
	static int objectsSize = 2048;
	static int mb2use = 490*1024*1024;
	static Random generator = new Random();
	
	public static void allocateMemory() {
		cache = new DirectCache(mb2use);
		cache.setDefaultDuration(1000);
		logger.debug(cache.toString());
	}
		
	@BeforeClass
	public static void setup() throws Exception {
		allocateMemory();
	}

	private String randomKey() {
		return "key"+generator.nextInt(mb2use/objectsSize);
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
    
	
	@Test
    public void fillUpHalfCache() throws Exception {	
		while (cache.usedMemory() < cache.capacity() / 2) {
			DummyObject dummy = new DummyObject("key" + cache.entries().size(), objectsSize*generator.nextInt(5));
			@SuppressWarnings("unused")
			CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
		}
		logger.debug(cache.toString());
    }

//    @Test
//    public void fillUp() throws Exception {
//    	int objects=0;
//    	
//		while (cache.remaining() > objectsSize*5) {
//			DummyObject dummy = new DummyObject("key" + objects);
//			dummy.obj = new Object[objectsSize*generator.nextInt(5)];
//			@SuppressWarnings("unused")
//			CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
//			objects++;
//		}
//		
//		assertEquals(objects, cache.entries().size());
//		
//		logger.debug("" + objects + " added");
//		logger.debug(cache.toString());
//		
//    }
	
    @Test
    public void testAll() throws IOException, ClassNotFoundException {
		Map<String, CacheEntry> entries = cache.entries();
		
		synchronized (entries) {
	    	Iterator<CacheEntry> iter = entries.values().iterator();
			while (iter.hasNext()) {
				CacheEntry entry = iter.next();
				if (!entry.expired()) {
					DummyObject dummy = (DummyObject) cache.retrieveObject(entry.getKey());
					assertNotNull(dummy);
					assertEquals(entry.getKey(), dummy.getName());
				}
			}
		}
		logger.debug("all objects checked");
		logger.debug(cache.toString());	
    }

//	@Test
//    public void testAllAgain() throws IOException, ClassNotFoundException {
//    	testAll();
//    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 650, average = 7)
    public void onlyRead() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 1.5)
    public void fourReadsOneWriteOneDelete() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 1.5)
    public void tenReadsOneWriteOneDelete() throws Exception { 	
    	for (int i = 0; i < 10; i++) {
    		@SuppressWarnings("unused")
			DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
			
		}
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 2500, average = 7)
    public void twoReadsOneWriteOneDelete() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }    


    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1000, average = 12)
    public void oneReadOneWriteOneDelete() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
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