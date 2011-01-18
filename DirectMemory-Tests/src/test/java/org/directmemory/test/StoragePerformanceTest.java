package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.OldProtoStuffSerializer;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.HeapStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.OrientDBBinaryStorage;
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
	private int howMany = 50000;
	private int pages = 1;
	private int ramSize = (int)(payloadSize * howMany * 2.5) / pages ;

//	@Test
	public void manyInsertsInHeap() throws InterruptedException {
		repeatedInsert(new HeapStorage(), howMany);
	}
	
//	@Test
	public void manyInsertsOffHeap() throws InterruptedException {
		logger.debug("Buffer size=" + Ram.inMb(ramSize*pages));
		repeatedInsert(new OffHeapStorage(ramSize, pages), howMany);
	}
	
//	@Test
	public void manyInsertsOnDisk() throws InterruptedException {
		repeatedInsert(new FileStorage(), howMany);
	}
	
	@Test
	public void manyInsertsOnOrientDBBinary() throws InterruptedException {
		repeatedInsert(new OrientDBBinaryStorage(), howMany);
	}
	
	@Test
	public void manyInsertsOnOrientDB() throws InterruptedException {
		repeatedInsert(new OrientDBStorage(), howMany);
	}
	
	public void repeatedInsert(Storage storage, int howMany) throws InterruptedException {		
		logger.debug("starting with " + storage.getClass().toString());
		storage.serializer = new OldProtoStuffSerializer();
		Runtime.getRuntime().gc();
		Thread.sleep(1000);
		Runtime.getRuntime().gc();
		logger.debug("Free memory: " + Runtime.getRuntime().freeMemory());	
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
		storage.dispose();
	}
	
		
	@AfterClass
	public static void checkPerformance() {
		CacheManager.displayTimings();
	}
}
