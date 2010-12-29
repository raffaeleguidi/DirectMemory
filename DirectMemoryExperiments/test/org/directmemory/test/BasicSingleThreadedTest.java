package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.directmemory.CacheEntry;
import org.directmemory.CacheStore;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.javasimon.SimonManager;
import org.javasimon.Stopwatch;
import org.junit.AfterClass;
import org.junit.Test;

public class BasicSingleThreadedTest {

	@Test
	public void addAndRetrieve() {
		DummyPojo pojo = new DummyPojo("test", 1024);
		CacheStore store = new CacheStore(-1, 1 * 1024 * 1024);
		store.put("test", pojo);
		DummyPojo pojo2 = (DummyPojo)store.get("test");
		assertNotNull(pojo2);
		assertEquals(pojo, pojo2);
	}
	
	@Test
	public void removeLast() {
		CacheStore store = new CacheStore(-1, 1 * 1024 * 1024);
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
		CacheStore store = new CacheStore(-1, 1 * 1024 * 1024);
		store.put("test1", new DummyPojo("test1", 1024));
		CacheEntry entry = store.remove("test1");
		assertEquals("test1", entry.key);
		entry = store.getEntry("test1");
		assertNull(entry);
	}
	
	@Test public void reachLimit() {
		int limit = 10;
		CacheStore store = new CacheStore(limit, 1 * 1024 * 1024);
		
		for (int i = 1; i <= limit; i++) {
			store.put("test" + i, new DummyPojo("test" + 1, 1024));
			if (i <= limit) {
				assertEquals(store.heapEntriesCount(), i);
			}
			System.out.println("reachLimit " + store);
		}
		
	}
	
	@Test public void goOverTheLimit() {
		int limit = 10;
		CacheStore store = new CacheStore(limit, 1 * 1024 * 1024);
		for (int i = 1; i <= limit * 2; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			store.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(store.heapEntriesCount(), i);
			} else {
				assertEquals( store.heapEntriesCount(), limit);
			}
			System.out.println("goOverTheLimit " + store);
		}
		
	}
	@Test public void goOverTheLimitWithProtostuff() {
		int limit = 10;
		CacheStore store = new CacheStore(limit, 1 * 1024 * 1024, new ProtoStuffSerializer());
		for (int i = 1; i <= limit * 2; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			store.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(store.heapEntriesCount(), i);
			} else {
				assertEquals( store.heapEntriesCount(), limit);
			}
			System.out.println("goOverTheLimit " + store);
		}
		
	}
	@Test public void goOverTheLimitPutAndGet() {
		int limit = 1000;
		CacheStore store = new CacheStore(limit, 10 * 1024 * 1024);
		for (int i = 1; i <= limit * 1.5; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			store.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(store.heapEntriesCount(), i);
			} else {
				assertEquals( store.heapEntriesCount(), limit);
			}
		}

		System.out.println("goOverTheLimitPutAndGet " + store);
		
		for (int i = 1; i <= limit * 1.5; i++) {
			@SuppressWarnings("unused")
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			@SuppressWarnings("unused")
			DummyPojo newPojo = (DummyPojo)store.get("test" + i);
		}
		assertEquals(limit, store.heapEntriesCount());
		assertEquals(570500, store.usedMemory());
	}
	
	@Test public void goOverTheLimitPutAndGetWithProtostuff() {
		int limit = 1000;
		CacheStore store = new CacheStore(limit, 10 * 1024 * 1024, new ProtoStuffSerializer());
		for (int i = 1; i <= limit * 1.5; i++) {
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			store.put("test" + i, pojo);
			if (i <= limit) {
				assertEquals(store.heapEntriesCount(), i);
			} else {
				assertEquals( store.heapEntriesCount(), limit);
			}
		}

		System.out.println("goOverTheLimitPutAndGet " + store);
		
		for (int i = 1; i <= limit * 1.5; i++) {
			@SuppressWarnings("unused")
			DummyPojo pojo = new  DummyPojo("test" + 1, 1024);
			@SuppressWarnings("unused")
			DummyPojo newPojo = (DummyPojo)store.get("test" + i);
		}
		
		assertEquals(limit, store.heapEntriesCount());
		assertEquals(518500, store.usedMemory());
	}
	
	public static void showAverage(Stopwatch sw) {
		double average = ((double)sw.getTotal() / (double)sw.getCounter() /1000000);
		System.out.println(sw.getName() + " - average " + average);
	}
	
	
	@AfterClass
	public static void checkPerformance() {
		showAverage(SimonManager.getStopwatch("get"));
		showAverage(SimonManager.getStopwatch("put"));
		showAverage(SimonManager.getStopwatch("remove"));
		showAverage(SimonManager.getStopwatch("protostuff-serialize"));
		showAverage(SimonManager.getStopwatch("protostuff-deserialize"));
		showAverage(SimonManager.getStopwatch("java-serialize"));
		showAverage(SimonManager.getStopwatch("java-deserialize"));
	}
}
