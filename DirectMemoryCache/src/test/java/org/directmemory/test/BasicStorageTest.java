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

public class BasicStorageTest {

	private static Logger logger=LoggerFactory.getLogger(BasicStorageTest.class);
	
	@SuppressWarnings("unused")
	private Random random = new Random();

	private int fixedSize() {
		return 2048;
	}
	
	@Test
	public void addAndRetrieve() {
		CacheStore cache = new CacheStore(1, CacheStore.KB(4), 1);
		cache.put("test1", new DummyPojo("test1", fixedSize()));
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(0, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug("put test1 " + cache.toString());
		
		cache.put("test2", new DummyPojo("test2", fixedSize()));
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(1, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug("put test2 " + cache.toString());

		cache.put("test3", new DummyPojo("test3", fixedSize()));
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(1, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug("put test3 " + cache.toString());

		logger.debug("ask for test1 " + cache.toString());
		DummyPojo pojo1 = (DummyPojo)cache.get("test1");
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(1, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug("got test1 " + cache.toString());

		DummyPojo pojo2 = (DummyPojo)cache.get("test2");
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(1, cache.offHeapEntriesCount());
		assertEquals(0, cache.onDiskEntriesCount());
		logger.debug("got test2 " + cache.toString());

		DummyPojo pojo3 = (DummyPojo)cache.get("test3");
		assertEquals(1, cache.heapEntriesCount());
		assertEquals(1, cache.offHeapEntriesCount());
		assertEquals(1, cache.onDiskEntriesCount());
		logger.debug("got test3 " + cache.toString());

		assertNotNull(pojo1);
		assertEquals("test1", pojo1.name);
		assertNotNull(pojo2);
		assertEquals("test2", pojo2.name);
		assertNotNull(pojo3);
		assertEquals("test3", pojo3.name);
		logger.debug("addAndRetrieve " + cache.toString());
		cache.reset();
	}
	
		
	@AfterClass
	public static void checkPerformance() {
		CacheStore.displayTimings();
	}
}
