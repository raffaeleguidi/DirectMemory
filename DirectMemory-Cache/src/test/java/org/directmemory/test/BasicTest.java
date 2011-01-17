package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager;
import org.directmemory.measures.Expires;
import org.directmemory.measures.For;
import org.directmemory.measures.Heap;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
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
	public void putAndGetWithJavaSerialization() {
		putAndGet(new StandardSerializer());
	}
	
	@Test
	public void putAndGetWithProtostuffSerialization() {
		putAndGet(new ProtoStuffSerializer());
	}
	
	public void putAndGet(Serializer serializer) {
		logger.debug("putAndGet with " + serializer.toString());
		CacheManager cache = new CacheManager(Heap.unlimited(), Ram.Mb(1), 1);
		cache.setSerializer(serializer);
		DummyPojo pojo = new DummyPojo("test1", 500);
		Object retVal = cache.put("test1", pojo);
		assertNotNull(retVal);
		assertEquals(1L, cache.heapEntriesCount());
		DummyPojo check = (DummyPojo)cache.get("test1");
		assertNotNull(check);
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(pojo, check);
	}

	@Test
	public void evictionWithJavaSerialization() {
		// temporarily disabled - looking for a nastier problem
		eviction(new StandardSerializer());
	}

	@Test
	public void evictionWithProtostuffSerialization() {
		// temporarily disabled - looking for a nastier problem
		eviction(new ProtoStuffSerializer());
	}

	public void eviction(Serializer serializer) {
		logger.debug("eviction with " + serializer.toString());
		CacheManager cache = new CacheManager(1, Ram.Mb(1), 1);
		cache.setSerializer(serializer);
		cache.put("test1", new DummyPojo("test1", randomSize()));
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(0L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() == 0L);
		logger.debug(cache.toString());
		
		@SuppressWarnings("unused")
		CacheEntry entry = cache.put("test2", new DummyPojo("test2", randomSize()));
		logger.debug(cache.toString());
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(1L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);
		logger.debug(cache.toString());
		
		cache.put("test3", new DummyPojo("test3", randomSize()));
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(2L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);
		logger.debug(cache.toString());
		
		DummyPojo pojo1 = (DummyPojo)cache.get("test1");
		logger.debug(cache.toString());
		
		
		// TODO: perform state check
//		for (CacheEntry ohEntry : cache.offHeapStore().entries().values()) {
//			logger.debug(ohEntry.key + " is offheap? " + ohEntry.offHeap());
//		}
		
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(2L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);
		
		DummyPojo pojo2 = (DummyPojo)cache.get("test2");
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(2L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		assertTrue(cache.usedMemory() > 0L);

		logger.debug(cache.toString());

		DummyPojo pojo3 = (DummyPojo)cache.get("test3");
		
		logger.debug(cache.toString());
		assertEquals(1L, cache.heapEntriesCount());
		assertEquals(2L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		assertTrue("Used memory should be greater than zero" , cache.usedMemory() > 0L);

		assertNotNull(pojo1);
		assertEquals("test1", pojo1.name);
		assertNotNull(pojo2);
		assertEquals("test2", pojo2.name);
		assertNotNull(pojo3);
		assertEquals("test3", pojo3.name);
		assertTrue("Used memory should be greater than zero" , cache.usedMemory() > 0L);

		logger.debug("addAndRetrieve " + cache.toString());
		CacheManager.displayTimings();
		
		cache.dispose();
		assertEquals(0L, cache.heapEntriesCount());
		assertEquals(0L, cache.offHeapEntriesCount());
		assertEquals(0L, cache.onDiskEntriesCount());
		assertEquals(0L, cache.usedMemory());
	}
		
//	@Test
//	public void removeLastWithJavaSerialization() {
//		removeLast(new StandardSerializer());
//	}
//
//	@Test
//	public void removeLastWithProtostuffSerialization() {
//		removeLast(new ProtoStuffSerializer());
//	}
//
//	public void removeLast(Serializer serializer) {
//		logger.debug("removeLast with " + serializer.toString());
//		CacheManager cache = new CacheManager(-1, 1 * 1024 * 1024, 1);
//		cache.setSerializer(serializer);
//		cache.put("test1", new DummyPojo("test1", 1024));
//		cache.put("test2", new DummyPojo("test2", 1024));
//		cache.put("test3", new DummyPojo("test3", 1024));
//		cache.put("test4", new DummyPojo("test4", 1024));
//		cache.put("test5", new DummyPojo("test5", 1024));
//		CacheEntry last = cache.removeLast(); 
//		// should be the first one inserted
//		assertEquals("test1", last.key);
//		cache.get("test2"); 
//		// accessing an element should put it back at the beginning of the list
//		last = cache.removeLast();
//		// so the last should be now test3
//		assertEquals("test3", last.key);
//		cache.reset();
//	}
	
	@Test
	public void removeWithJavaSerialization() {
		remove(new StandardSerializer());
	}

	@Test
	public void removeWithProtostuffSerialization() {
		remove(new ProtoStuffSerializer());
	}

	public void remove(Serializer serializer) {
		logger.debug("remove with " + serializer.toString());
		CacheManager cache = new CacheManager(-1, 1 * 1024 * 1024, 1);
		cache.setSerializer(serializer);
		cache.put("test1", new DummyPojo("test1", 1024));
		CacheEntry entry = cache.remove("test1");
		assertEquals("test1", entry.key);
		entry = cache.getEntry("test1");
		assertNull("entry has not been removed", entry);
		cache.dispose();
	}
	
	@Test
	public void expiry() throws InterruptedException {
		CacheManager cache = new CacheManager(Heap.unlimited(), Ram.Mb(1), 1);
		cache.setDefaultExpirationTime(Expires.in(0.5).seconds());
		cache.put("test1", new DummyPojo("test1", 1024));
		Thread.sleep(For.exactly(1).seconds());
		Object obj = cache.get("test1");
		assertNull("entry has not expired", obj);
	}
	
	@AfterClass
	public static void checkPerformance() {
		CacheManager.displayTimings();
	}
}
