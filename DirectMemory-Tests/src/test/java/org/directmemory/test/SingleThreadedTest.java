package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;

import org.directmemory.CacheStore;
import org.directmemory.misc.DummyPojo;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleThreadedTest {

	private static Logger logger=LoggerFactory.getLogger(SingleThreadedTest.class);
	
	private Random random = new Random();

	private int randomSize() {
		return 1024 + random.nextInt(1024);
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
			logger.debug("done " + i + " of " + (limit * 2));
			assertNotNull(newPojo);
			assertEquals("test"+i, newPojo.name);
		}
		logger.debug("finally " + cache.toString());
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
