package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.HeapStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.OrientDBStorage;
import org.directmemory.storage.Storage;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoragePerformanceTest {

	private static Logger logger=LoggerFactory.getLogger(StoragePerformanceTest.class);
	
	@SuppressWarnings("unused")
	private Random random = new Random();

	private int payloadSize = Ram.Kb(2);
	private int howMany = 100000;
	private int pages = 1;
	private int ramSize = (int)(payloadSize * howMany * 2.5) / pages ;

	@Test
	public void manyInsertsInHeap() {
		repeatedInsert(new HeapStorage(), howMany);
	}
	
//	@Test
	public void manyInsertsOffHeap() {
		logger.debug("Buffer size=" + Ram.inMb(ramSize*pages));
		repeatedInsert(new OffHeapStorage(ramSize, pages), howMany);
	}
	
//	@Test
	public void manyInsertsOnDisk() {
		repeatedInsert(new FileStorage(), howMany);
	}
	
//	@Test
	public void manyInsertsOnOrientDB() {
		repeatedInsert(new OrientDBStorage(), howMany);
	}
	
	public void repeatedInsert(Storage storage, int howMany) {		
		logger.debug("starting with " + storage.getClass().toString());
		logger.debug("before: " + storage.toString());
		for (int i = 0; i < howMany; i++) {
			CacheEntry entry = new CacheEntry();
			entry.object = new DummyPojo("test" + i, payloadSize);
			entry.key = ((DummyPojo)entry.object).name;
			assertTrue(storage.put(entry));
		}
		logger.debug("inserted " + howMany + " entries");
		logger.debug("after insert: " + storage.toString());
		for (int i = 0; i < howMany; i++) {
			CacheEntry check = storage.get("test" + i);
			assertNotNull("entry not found: test" + i, check);
			assertEquals("test" + i, check.key);
		}
		logger.debug("read " + howMany + " entries");
		logger.debug("after read: " + storage.toString());
		logger.debug("overall performance: " + storage.performance());	
	}
	
//	private int fixedSize() {
//		return 2048;
//	}
//	
//	@Test
//	public void putAndGet() throws Exception {
//		CacheManager cache = new CacheManager(1, CacheManager.KB(4), 1);
//		cache.put("test1", new DummyPojo("test1", fixedSize()));
//		assertEquals(1L, cache.heapEntriesCount());
//		assertEquals(0L, cache.offHeapEntriesCount());
//		assertEquals(0L, cache.onDiskEntriesCount());
//		logger.debug("put test1 " + cache.toString());
//		
//		cache.put("test2", new DummyPojo("test2", fixedSize()));
//		assertEquals(1L, cache.heapEntriesCount());
//		assertEquals(1L, cache.offHeapEntriesCount());
//		if (cache.usedMemory() <= 0L) {
//			throw new Exception();
//		}
//		assertEquals(0L, cache.onDiskEntriesCount());
//		logger.debug("put test2 " + cache.toString());
//
//		cache.put("test3", new DummyPojo("test3", fixedSize()));
//		logger.debug("stored test3: " + cache.toString());
//		assertEquals(3, cache.heapStore().entries().size());
//		assertEquals(1L, cache.heapEntriesCount());
//		assertEquals(1L, cache.offHeapEntriesCount());
//		assertEquals(1L, cache.onDiskEntriesCount());
//		assertTrue(cache.usedMemory() > 0L);
//
//		logger.debug("ask for test1 " + cache.toString());
//		DummyPojo pojo1 = (DummyPojo)cache.get("test1");
//		logger.debug("got test1 " + cache.toString());
//		assertEquals("test1", pojo1.name);
//		assertEquals(3, cache.heapStore().entries().size());
//		assertEquals(1L, cache.heapEntriesCount());
//		assertEquals(1L, cache.offHeapEntriesCount());
//		assertEquals(1L, cache.onDiskEntriesCount());
//		assertTrue(cache.usedMemory() > 0L);
//
//		DummyPojo pojo2 = (DummyPojo)cache.get("test2");
//		logger.debug("got test2 " + cache.toString());
//		assertEquals("test2", pojo2.name);
//		assertEquals(3, cache.heapStore().entries().size());
//		assertEquals(1L, cache.heapEntriesCount());
//		assertEquals(1L, cache.offHeapEntriesCount());
//		assertEquals(1L, cache.onDiskEntriesCount());
//		assertTrue(cache.usedMemory() > 0L);
//
//		DummyPojo pojo3 = (DummyPojo)cache.get("test3");
//		logger.debug("got test3 " + cache.toString());
//		assertEquals("test3", pojo3.name);
//		assertEquals(3, cache.heapStore().entries().size());
//		assertEquals(1L, cache.heapEntriesCount());
//		assertEquals(1L, cache.offHeapEntriesCount());
//		assertEquals(1L, cache.onDiskEntriesCount());
//		assertTrue(cache.usedMemory() > 0L);
//
//		logger.debug("used memory " + cache.usedMemory());
//		logger.debug("on disk " + cache.onDiskEntriesCount());
//		assertEquals(1L, cache.onDiskEntriesCount());
//		logger.debug("got test3 " + cache.toString());
//
//		assertNotNull(pojo1);
//		assertEquals("test1", pojo1.name);
//		assertNotNull(pojo2);
//		assertEquals("test2", pojo2.name);
//		assertNotNull(pojo3);
//		assertEquals("test3", pojo3.name);
//		logger.debug("addAndRetrieve " + cache.toString());
//
//		cache.reset();
//	}
	
		
	@AfterClass
	public static void checkPerformance() {
		CacheManager.displayTimings();
	}
}
