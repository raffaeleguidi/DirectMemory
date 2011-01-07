package org.directmemory.test;

import static org.junit.Assert.assertEquals;

import org.directmemory.CacheManager;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicProtostuffSingleThreadedTest {

	private static Logger logger=LoggerFactory.getLogger(BasicProtostuffSingleThreadedTest.class);
	
	@Test public void goOverTheLimitWithProtostuff() {
		int limit = 10;
		CacheManager store = new CacheManager(limit, 1 * 1024 * 1024, 1);
		store.setSerializer(new ProtoStuffSerializer());
		for (int i = 1; i <= limit * 2; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			store.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(store.heapEntriesCount(), i);
			} else {
				assertEquals(limit, store.heapEntriesCount());
			}
			logger.debug("goOverTheLimit " + store);
		}
		CacheManager.displayTimings();
	}
	
	
	@Test public void goOverTheLimitPutAndGetWithProtostuff() {
		int limit = 1000;
		CacheManager cache = new CacheManager(limit, 10 * 1024 * 1024, 1);
		cache.setSerializer(new ProtoStuffSerializer());
		for (int i = 1; i <= limit * 1.5; i++) {
			DummyPojo pojo = new  DummyPojo("test" + i, 1024);
			cache.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(cache.heapEntriesCount(), i);
			} else {
				assertEquals(limit, cache.heapEntriesCount());
			}
		}

		logger.debug("goOverTheLimitPutAndGet " + cache.toString());
		
		for (int i = 1; i <= limit * 1.5; i++) {
			@SuppressWarnings("unused")
			DummyPojo pojo = new  DummyPojo("test" + i, 1024);
			@SuppressWarnings("unused")
			DummyPojo newPojo = (DummyPojo)cache.get("test" + i);
		}
		
		assertEquals(limit, cache.heapEntriesCount());
//		assertEquals(518500, cache.usedMemory());
	}
	
	
	@AfterClass
	public static void checkPerformance() {
		CacheManager.displayTimings();
	}
}
