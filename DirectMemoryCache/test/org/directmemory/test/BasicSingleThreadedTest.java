package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.directmemory.CacheEntry;
import org.directmemory.CacheStore;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicSingleThreadedTest {

	private static Logger logger=LoggerFactory.getLogger(BasicSingleThreadedTest.class);
	
	@Test
	public void addAndRetrieve() {
		DummyPojo pojo = new DummyPojo("test", 1024);
		CacheStore store = new CacheStore(-1, 1 * 1024 * 1024, 1);
		store.put("test", pojo);
		DummyPojo pojo2 = (DummyPojo)store.get("test");
		assertNotNull(pojo2);
		assertEquals(pojo, pojo2);
	}
	
	@Test
	public void removeLast() {
		CacheStore store = new CacheStore(-1, 1 * 1024 * 1024, 1);
		store.put("test1", new DummyPojo("test1", 1024));
		store.put("test2", new DummyPojo("test2", 1024));
		store.put("test3", new DummyPojo("test3", 1024));
		store.put("test4", new DummyPojo("test4", 1024));
		store.put("test5", new DummyPojo("test5", 1024));
		CacheEntry last = store.removeLast(); 
		// should be the first one inserted
		assertEquals("test1", last.key);
		store.get("test2"); 
		// accessing an element should put it back at the beginning of the list
		last = store.removeLast();
		// so the last should be now test3
		assertEquals("test3", last.key);
	}
	
	@Test
	public void remove() {
		CacheStore store = new CacheStore(-1, 1 * 1024 * 1024, 1);
		store.put("test1", new DummyPojo("test1", 1024));
		CacheEntry entry = store.remove("test1");
		assertEquals("test1", entry.key);
		entry = store.getEntry("test1");
		assertNull(entry);
	}
	
	@Test public void reachLimit() {
		int limit = 10;
		CacheStore store = new CacheStore(limit, 1 * 1024 * 1024, 1);
		
		for (int i = 1; i <= limit; i++) {
			store.put("test" + i, new DummyPojo("test" + 1, 1024));
			if (i <= limit) {
				assertEquals(store.heapEntriesCount(), i);
			}
			logger.debug("reachLimit " + store);
		}
		
	}
	
	@Test public void goOverTheLimit() {
		int limit = 10;
		CacheStore store = new CacheStore(limit, 1 * 1024 * 1024, 1);
		for (int i = 1; i <= limit * 2; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			store.put("test" + i, pojo);
			if (i <= limit ) {
				assertEquals(i, store.heapEntriesCount());
			} else {
				assertEquals(limit, store.heapEntriesCount());
			}
			logger.debug("goOverTheLimit " + store);
		}
		
	}
	
	@Test public void goOverTheLimitWithProtostuff() {
		int limit = 10;
		CacheStore store = new CacheStore(limit, 1 * 1024 * 1024, 1);
		store.serializer = new ProtoStuffSerializer();
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
		CacheStore.displayTimings();
	}
	
	@Test public void goOverTheLimitPutAndGet() {
		int limit = 1000;
		CacheStore cache = new CacheStore(limit, 10 * 1024 * 1024, 1);
		for (int i = 1; i <= limit * 1.5; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
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
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			@SuppressWarnings("unused")
			DummyPojo newPojo = (DummyPojo)cache.get("test" + i);
		}
		assertEquals(limit, cache.heapEntriesCount());
		assertEquals(570500, cache.usedMemory());
	}
	
	@Test public void goOverTheLimitPutAndGetWithProtostuff() {
		int limit = 1000;
		CacheStore cache = new CacheStore(limit, 10 * 1024 * 1024, 1);
		cache.serializer = new ProtoStuffSerializer();
		for (int i = 1; i <= limit * 1.5; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
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
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			@SuppressWarnings("unused")
			DummyPojo newPojo = (DummyPojo)cache.get("test" + i);
		}
		
		assertEquals(limit, cache.heapEntriesCount());
		assertEquals(518500, cache.usedMemory());
	}
	
	
	@AfterClass
	public static void checkPerformance() {
		CacheStore.displayTimings();
	}
}
