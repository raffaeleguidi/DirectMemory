package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.directcache.CacheEntry;
import org.directcache.DirectCache;
import org.directcache.exceptions.DirectCacheException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FunctionalTest {
	
	private static Logger logger=LoggerFactory.getLogger(FunctionalTest.class);

//	@Rule
//    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
	static DirectCache cache = null;
	static int objectsToStore = 10000;
	static int objectsSize = 2500;
	static int mb2use = 15;
	static Random generator = new Random();
	

	private String randomKey() {
		return "key"+generator.nextInt(objectsToStore);
	}

	public DummyObject randomObject() {
    	String key = randomKey();
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
		return dummy;
	}
	
	@Before
	public void allocateMemory() {
		cache = new DirectCache(mb2use);
		logger.info("allocated " + mb2use + " mb");
	}
		
	static int size = 0;
	static int objects = 0;

	@Test
    public void fillUp() throws Exception {
		int size = 0;
		int objects = 0;
		
		while (size < cache.capacity() - objectsSize * 5) {
			DummyObject dummy = new DummyObject("key" + objects);
			dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
			CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
			size += entry.getSize();
			objects++;
		}
		
		logger.debug("cache size=" + size + " for " + objects + " objects");
				
    }
	
	@Test
    public void fillUpAndOver() throws Exception {
		int size = 0;
		int objects = 0;
		
		while (size < cache.capacity() - objectsSize * 5) {
			DummyObject dummy = new DummyObject("key" + objects);
			dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
			CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
			size += entry.getSize();
			objects++;
		}
		
		logger.info("cache size=" + size + " for " + objects + " objects");
		
		int goover = 0;
		
		while (goover < 50) {
			goover++;
			DummyObject dummy = new DummyObject("key" + objects);
			dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
			try {
				CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
				size += entry.getSize();
				objects++;
				logger.info(entry.getSize() + " still fits size=" + size + " for " + objects + " objects");
			} catch (DirectCacheException e) {
				logger.info(e.getClass() + " object of size " + e.exceeding + " doesn't fit size=" + size + " for " + objects + " objects");
			}
		}

		logger.debug("cache size=" + size + " for " + objects + " objects");

	}

//    @Test
//    @Required(max = 30)
//    public void firstAndLargestItem() throws Exception { 	
//    	DummyObject firstObject = new DummyObject("key0", objectsSize);
//    	cache.storeObject(firstObject.getName(), firstObject);
//		System.out.println("cache size is " + cache.size() + " bytes");
//		System.out.println();
//		System.out.println(cache.toString());
//    }
    
//
//	@Test
//    public void fillUpHalfCache() throws Exception {		
//		while (size < cache.capacity() / 2) {
//			DummyObject dummy = new DummyObject("key" + i);
//			dummy.obj = new Object[objectsSize*generator.nextInt(5)];
//			CacheEntry entry = cache.storeObject(dummy.getName(), dummy);
//			size += entry.getSize();
//		}
//    }
//
//    @Test
//    @PerfTest(duration = 10000, threads = 10)
//    @Required(max = 150, average = 7)
//    public void oneReadOneWriteOneDelete() throws Exception { 	
//    	@SuppressWarnings("unused")
//		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    	DummyObject object2add = randomObject();
//    	cache.storeObject(object2add.getName(), object2add);
//    	cache.removeObject(randomKey());
//    }
//    
//    @Test
//    @PerfTest(duration = 10000, threads = 10)
//    @Required(max = 150, average = 7)
//    public void twoReadsOneWriteOneDelete() throws Exception { 	
//    	@SuppressWarnings("unused")
//		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    	DummyObject object2add = randomObject();
//    	cache.storeObject(object2add.getName(), object2add);
//    	cache.removeObject(randomKey());
//    }
//    
//    @Test
//    @PerfTest(duration = 10000, threads = 10)
//    @Required(max = 120, average = 10.5)
//    public void fourReadsOneWriteOneDelete() throws Exception { 	
//    	@SuppressWarnings("unused")
//		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    	DummyObject object2add = randomObject();
//    	cache.storeObject(object2add.getName(), object2add);
//    	cache.removeObject(randomKey());
//    }
//    
//    @Test
//    @PerfTest(duration = 10000, threads = 10)
//    @Required(max = 150, average = 2.5)
//    public void onlyRead() throws Exception { 	
//    	@SuppressWarnings("unused")
//		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
//    }
    
    @After
    public void checkBuffer() throws IOException, ClassNotFoundException {
		Iterator<CacheEntry> iter = cache.getAllocationTable().values().iterator();
		
		while (iter.hasNext()) {
			CacheEntry entry = iter.next();
			DummyObject dummy = (DummyObject) cache.retrieveObject(entry.getKey());
			assertNotNull(dummy);
			assertEquals(entry.getKey(), dummy.getName());
		}
		
		logger.info("all " + cache.getAllocationTable().size() + " objects checked");
		logger.info(cache.toString());
   }

}
