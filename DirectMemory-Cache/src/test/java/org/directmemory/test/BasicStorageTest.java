package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.directmemory.CacheEntry;
import org.directmemory.CacheManager;
import org.directmemory.misc.DummyPojo;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.OrientDBStorage;
import org.directmemory.storage.Storage;
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
	
	public void genericStorageTest(Storage storage) {
		CacheEntry entry = new CacheEntry();
		entry.object = new DummyPojo("test", 1024);
		entry.key = ((DummyPojo)entry.object).name;
		storage.put(entry);
		
		CacheEntry entry2 = storage.get("test");
		assertNotNull("entry has not been found", entry);
		assertNotNull("entry object has not been loaded", entry.object);
		assertEquals(entry.key, entry2.key);
		
		entry.object = new DummyPojo("test2", 1024);
		entry.key = ((DummyPojo)entry.object).name;
		storage.put(entry);
		
		CacheEntry deleted = storage.delete("test2");
		assertEquals("test2", deleted.key);
		
		CacheEntry check = storage.get("test2");
		assertNull("test2 has not been deleted", check);
		
		storage.reset();

		entry.object = new DummyPojo("test2", 1024);
		entry.key = ((DummyPojo)entry.object).name;
		storage.put(entry);
		
	}
	
	@Test
	public void fileStorage() {
		genericStorageTest(new FileStorage());
	}
	
	@Test
	public void orientDBStorage() {
		genericStorageTest(new OrientDBStorage());
	}
	
	@Test
	public void putAndGet() throws Exception {
		CacheManager cache = new CacheManager(1, CacheManager.KB(4), 1);
		cache.put("test1", new DummyPojo("test1", fixedSize()));
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(0L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		logger.debug("put test1 " + cache.toString());
		
		cache.put("test2", new DummyPojo("test2", fixedSize()));
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(1L, cache.offHeapEntriesCount());
		if (cache.usedMemory() <= 0L) {
			throw new Exception();
		}
		assertEquals(0L, cache.onDiskEntriesCount());
		logger.debug("put test2 " + cache.toString());

		cache.put("test3", new DummyPojo("test3", fixedSize()));
		logger.debug("stored test3: " + cache.toString());
		assertEquals(3, cache.heapStore().entries().size());
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(1L, cache.offHeapEntriesCount());
		assertEquals(1L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);

		logger.debug("ask for test1 " + cache.toString());
		DummyPojo pojo1 = (DummyPojo)cache.get("test1");
		logger.debug("got test1 " + cache.toString());
		assertEquals("test1", pojo1.name);
		assertEquals(3, cache.heapStore().entries().size());
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(1L, cache.offHeapEntriesCount());
		assertEquals(1L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);

		DummyPojo pojo2 = (DummyPojo)cache.get("test2");
		logger.debug("got test2 " + cache.toString());
		assertEquals("test2", pojo2.name);
		assertEquals(3, cache.heapStore().entries().size());
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(1L, cache.offHeapEntriesCount());
		assertEquals(1L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);

		DummyPojo pojo3 = (DummyPojo)cache.get("test3");
		logger.debug("got test3 " + cache.toString());
		assertEquals("test3", pojo3.name);
		assertEquals(3, cache.heapStore().entries().size());
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(1L, cache.offHeapEntriesCount());
		assertEquals(1L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);

		logger.debug("used memory " + cache.usedMemory());
		logger.debug("on disk " + cache.onDiskEntriesCount());
		assertEquals(1L, cache.onDiskEntriesCount());
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
		CacheManager.displayTimings();
	}
}
