package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Random;

import org.directmemory.CacheEntry;
import org.directmemory.CacheStore;
import org.directmemory.misc.DummyPojo;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicSingleThreadedTest {

	private static Logger logger=LoggerFactory.getLogger(BasicSingleThreadedTest.class);
	
	private Random random = new Random();

	private int randomSize() {
		return 1024 + random.nextInt(1024);
	}
	
	@Test
	public void addAndRetrieve() {
		CacheStore cache = new CacheStore(1, CacheStore.MB(1), 1);
		cache.put("test1", new DummyPojo("test1", randomSize()));
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(0, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		
		@SuppressWarnings("unused")
		CacheEntry entry = cache.put("test2", new DummyPojo("test2", randomSize()));
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(1, cache.offHeapEntriesCount());
		assert(cache.usedMemory() > 0);
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug(cache.toString());
		
		cache.put("test3", new DummyPojo("test3", randomSize()));
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(2, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug(cache.toString());
		
		DummyPojo pojo1 = (DummyPojo)cache.get("test1");
		logger.debug(cache.toString());
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(2, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		
		DummyPojo pojo2 = (DummyPojo)cache.get("test2");
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(2, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());

		DummyPojo pojo3 = (DummyPojo)cache.get("test3");
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(2, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());

		assertNotNull(pojo1);
		assertEquals("test1", pojo1.name);
		assertNotNull(pojo2);
		assertEquals("test2", pojo2.name);
		assertNotNull(pojo3);
		assertEquals("test3", pojo3.name);

		logger.debug("addAndRetrieve " + cache.toString());
		CacheStore.displayTimings();
		
		cache.reset();
		assertEquals(0, cache.heapEntriesCount());
		assertEquals(0, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		assertEquals(0, cache.usedMemory());
	}
	
	
	@Test public void goOverTheOffheapLimitPutAndGet() {
		int limit = 1000;
		CacheStore cache = new CacheStore(limit, CacheStore.MB(2), 1);
//		cache.serializer = new ProtoStuffSerializer();
		for (int i = 1; i <= limit * 2; i++) {
			DummyPojo pojo = new  DummyPojo("test" + i, randomSize());
			cache.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(i, cache.heapEntriesCount());
			} else {
				assertEquals(limit, cache.heapEntriesCount());
			}
		}

		logger.debug("goOverTheLimitPutAndGet " + cache.toString());
		
		for (int i = 1; i <= limit * 2; i++) {
			String key = "test" + i;
			DummyPojo newPojo = (DummyPojo)cache.get(key);
			assertNotNull(newPojo);
			assertEquals("test"+i, newPojo.name);
		}
		logger.debug("finally " + cache.toString());
		cache.reset();
	}	
	

	
	@Test
	public void removeLast() {
		CacheStore cache = new CacheStore(-1, 1 * 1024 * 1024, 1);
		cache.put("test1", new DummyPojo("test1", 1024));
		cache.put("test2", new DummyPojo("test2", 1024));
		cache.put("test3", new DummyPojo("test3", 1024));
		cache.put("test4", new DummyPojo("test4", 1024));
		cache.put("test5", new DummyPojo("test5", 1024));
		CacheEntry last = cache.removeLast(); 
		// should be the first one inserted
		assertEquals("test1", last.key);
		cache.get("test2"); 
		// accessing an element should put it back at the beginning of the list
		last = cache.removeLast();
		// so the last should be now test3
		assertEquals("test3", last.key);
		cache.reset();
	}
	
	@Test
	public void remove() {
		CacheStore cache = new CacheStore(-1, 1 * 1024 * 1024, 1);
		cache.put("test1", new DummyPojo("test1", 1024));
		CacheEntry entry = cache.remove("test1");
		assertEquals("test1", entry.key);
		entry = cache.getEntry("test1");
		assertNull(entry);
		cache.reset();
	}
	
	@Test public void reachLimit() {
		int limit = 10;
		CacheStore cache = new CacheStore(limit, 1 * 1024 * 1024, 1);
		
		for (int i = 1; i <= limit; i++) {
			cache.put("test" + i, new DummyPojo("test" + 1, 1024));
			if (i < limit) {
				assert(limit >= cache.heapEntriesCount());
			}
			logger.debug("reachLimit " + cache);
		}
		cache.reset();
	}
	
	@Test public void goOverTheLimit() {
		int limit = 10;
		CacheStore cache = new CacheStore(limit, 1 * 1024 * 1024, 1);
		for (int i = 1; i <= limit * 2; i++) {
			DummyPojo pojo = new  DummyPojo("test" + i, 1024);
			cache.put("test" + i, pojo);
			if (i <= limit ) {
				assertEquals(i, cache.heapEntriesCount());
			} else {
				assertEquals(limit, cache.heapEntriesCount());
			}
			logger.debug("goOverTheLimit " + cache);
		}
		cache.reset();		
	}
	
	
	@Test public void goOverTheLimitPutAndGet() {
		int limit = 1000;
		CacheStore cache = new CacheStore(limit, 10 * 1024 * 1024, 1);
		for (int i = 1; i <= limit * 1.5; i++) {
			DummyPojo pojo = new  DummyPojo("test" + i, 1024);
			cache.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(i, cache.heapEntriesCount());
			} else {
				assertEquals(limit, cache.heapEntriesCount());
			}
		}

		logger.debug("goOverTheLimitPutAndGet " + cache.toString());
		
		for (int i = 1; i <= limit * 1.5; i++) {
			@SuppressWarnings("unused")
			DummyPojo pojo = new  DummyPojo("test" + i, 1024);
			DummyPojo newPojo = (DummyPojo)cache.get("test" + i);
			assertNotNull(newPojo);
			assertEquals("test"+i, newPojo.name);
	}
		assertEquals(limit, cache.heapEntriesCount());
		cache.reset();
	}	

	
	@AfterClass
	public static void checkPerformance() {
		CacheStore.displayTimings();
	}
}
