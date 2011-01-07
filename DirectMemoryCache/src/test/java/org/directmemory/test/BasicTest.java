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

public class BasicTest {

	private static Logger logger=LoggerFactory.getLogger(BasicTest.class);
	
	private Random random = new Random();

	private int randomSize() {
		return 1024 + random.nextInt(1024);
	}
	
	@Test
	public void simple() {
		CacheStore cache = new CacheStore(1, CacheStore.MB(1), 1);
		DummyPojo pojo = new DummyPojo("test1", 500);
		Object retVal = cache.put("test1", pojo);
		assertNotNull(retVal);
		assertEquals(1, cache.heapStore().count());
		DummyPojo check = (DummyPojo)cache.get("test1");
		assertNotNull(check);
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(pojo, check);
	}

	@Test
	public void addAndRetrieve() {
		CacheStore cache = new CacheStore(1, CacheStore.MB(1), 1);
		cache.put("test1", new DummyPojo("test1", randomSize()));
		assertEquals(1, cache.heapStore().count());
		assertEquals(0, cache.offHeapStore().count());
		assertEquals(0, cache.diskStore().count());
		logger.debug(cache.toString());
		
		@SuppressWarnings("unused")
		CacheEntry entry = cache.put("test2", new DummyPojo("test2", randomSize()));
		logger.debug(cache.toString());
		assertEquals(1, cache.heapStore().count());
		assertEquals(1, cache.offHeapStore().count());
		assert(cache.usedMemory() > 0);
		assertEquals(0, cache.diskStore().count());
		logger.debug(cache.toString());
		
		cache.put("test3", new DummyPojo("test3", randomSize()));
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(2, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug(cache.toString());
		
		DummyPojo pojo1 = (DummyPojo)cache.get("test1");
		logger.debug(cache.toString());
		
		for (CacheEntry ohEntry : cache.offHeapStore().entries().values()) {
			logger.debug(ohEntry.key + " is offheap? " + ohEntry.offHeap());
		}
		
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
	
	@AfterClass
	public static void checkPerformance() {
		CacheStore.displayTimings();
	}
}
