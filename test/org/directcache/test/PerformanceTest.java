package org.directcache.test;

import static org.junit.Assert.*;

import java.util.Iterator;
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
	
	private static Logger logger=LoggerFactory.getLogger("org.directcache");

	@Rule
    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
	static DirectCache cache = null;
	static int objectsToStore = 10000;
	static int objectsSize = 2500;
	static int mb2use = 100;
	static Random generator = new Random();
	
	public static void allocateMemory() {
		cache = new DirectCache(mb2use);
		cache.setDefaultDuration(1);
		System.out.println("allocated " + mb2use + " mb");
	}
		
	@BeforeClass
	public static void setup() throws Exception {
		allocateMemory();
	}

	private String randomKey() {
		return "key"+generator.nextInt(objectsToStore);
	}

	public DummyObject randomObject() {
    	String key = randomKey();
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
		return dummy;
	}
	
    @Test
    public void fillUpAndTestAll() throws Exception {
    	
		while (size < cache.capacity() / 2) {
			DummyObject dummy = new DummyObject("key" + objects);
			dummy.obj = new Object[objectsSize*generator.nextInt(5)];
			CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
			size += entry.getSize();
			objects++;
		}
		
		logger.debug("cache size=" + size + " for " + objects + " objects");
		
		Iterator<CacheEntry> iter = cache.getAllocationTable().values().iterator();
		
		while (iter.hasNext()) {
			CacheEntry entry = iter.next();
			DummyObject dummy = (DummyObject) cache.retrieveObject(entry.getKey());
			assertNotNull(dummy);
			assertEquals(entry.getKey(), dummy.getName());
		}
		
		logger.debug("all objects checked");
		
		allocateMemory();
		
    }
	

    @Test
    @Required(max = 30)
    public void firstAndLargestItem() throws Exception { 	
    	DummyObject firstObject = new DummyObject("key0", objectsSize);
    	cache.storeObject(firstObject.getName(), firstObject);
		System.out.println("cache size is " + cache.size() + " bytes");
		System.out.println();
		System.out.println(cache.toString());
    }
    
	static int size = 0;
	static int objects = 0;

	@Test
    public void fillUpHalfCache() throws Exception {		
		while (size < cache.capacity() / 2) {
			DummyObject dummy = new DummyObject("key" + i);
			dummy.obj = new Object[objectsSize*generator.nextInt(5)];
			CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
			size += entry.getSize();
		}
    }

    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 150, average = 7)
    public void oneReadOneWriteOneDelete() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 150, average = 7)
    public void twoReadsOneWriteOneDelete() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 120, average = 10.5)
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
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 150, average = 2.5)
    public void onlyRead() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    }
    
    @AfterClass
    public static void checkBuffer() {
		System.out.println();
		System.out.println(cache.toString());
		cache = null;
   }

}
