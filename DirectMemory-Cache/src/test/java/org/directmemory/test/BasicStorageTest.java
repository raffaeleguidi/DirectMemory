package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Random;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.OffHeapStorage;
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

	@Test
	public void offHeapStorage() {
		genericStorageTest(new OffHeapStorage(Ram.Mb(10), 1));
	}

	@Test
	public void fileStorage() {
		genericStorageTest(new FileStorage());
	}

	@Test
	public void orientDBStorage() {
		genericStorageTest(new OrientDBStorage());
	}

	public void genericStorageTest(Storage storage) {
		
		logger.debug("before: " + storage.toString());

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
		
		logger.debug("before: " + storage.toString());
		logger.debug("overall performance: " + storage.performance());	

		storage.reset();
//
//		entry.object = new DummyPojo("test2", 1024);
//		entry.key = ((DummyPojo)entry.object).name;
//		storage.put(entry);
	}
		
	@AfterClass
	public static void checkPerformance() {
		CacheManager.displayTimings();
	}
}
