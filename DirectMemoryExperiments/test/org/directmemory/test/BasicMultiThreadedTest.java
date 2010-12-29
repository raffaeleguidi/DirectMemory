package org.directmemory.test;

import org.directmemory.CacheStore;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.javasimon.SimonManager;
import org.javasimon.Stopwatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicMultiThreadedTest {
	
	private abstract class ThreadUsingCache extends Thread {
		public ThreadUsingCache(ThreadGroup group, String name, CacheStore cache) {
			super(group, name);
			this.cache = cache;
		}

		public CacheStore cache;
	}
	
	public static CacheStore cache = null;
	
	@BeforeClass
	public static void setup() {
//		cache = new CacheStore(100, 10 * 1024 * 1024, new ProtoStuffSerializer());
		cache = new CacheStore(100, 10 * 1024 * 1024);
	}

	
	
	@Test
	public void put () {
		ThreadGroup group = new ThreadGroup("test");
		
		int numThreads = 20;
		
		for (int i = 0; i < numThreads; i++) {
			new ThreadUsingCache(group , "test" + i, cache) {
				public void run() {
					int i = 0;
					try {
						int numOps = 100;
						while (++i < numOps) {
							DummyPojo pojo = new DummyPojo(getName() + "-" + i, 1024);
							cache.put(pojo.name, pojo);
//							DummyPojo otherPojo = (DummyPojo)cache.get(getName() + "-" + i);
//							if (otherPojo == null)
//								System.out.println("errore");;
							int pause = 10;
							sleep(pause); 
					    }
					} catch (InterruptedException ex) {
						
					}
				}
			}.start();
		}

		while (group.activeCount() > 0)
			Thread.yield();
		
		System.out.println(cache);
	}
	
	@Test
	public void get() {

		ThreadGroup group = new ThreadGroup("test");
		
		int numThreads = 20;
		
		for (int i = 0; i < numThreads; i++) {
			new ThreadUsingCache(group , "test" + i, cache) {
				public void run() {
					int i = 0;
					try {
						int numOps = 100;
						while (++i < numOps) {
							@SuppressWarnings("unused")
							DummyPojo pojo = (DummyPojo)cache.get(getName() + "-" + i);
							int pause = 10;
							sleep(pause); 
					    }
					} catch (InterruptedException ex) {
						
					}
				}
			}.start();
		}
		
		while (group.activeCount() > 0)
			Thread.yield();		

		System.out.println(cache);
	}
	
	public static void showAverage(Stopwatch sw) {
		double average = ((double)sw.getTotal() / (double)sw.getCounter() /1000000);
		System.out.println(sw.getName() + " total time: " + (sw.getTotal()/1000000) + " " + sw.getCounter() + " hits - average " + average + " - max active:" + sw.getMaxActive());
	}
	
	
	@AfterClass
	public static void checkPerformance() {
		showAverage(SimonManager.getStopwatch("put"));
		showAverage(SimonManager.getStopwatch("checkheaplimits"));
		showAverage(SimonManager.getStopwatch("get"));
		showAverage(SimonManager.getStopwatch("remove"));
		showAverage(SimonManager.getStopwatch("protostuff-serialize"));
		showAverage(SimonManager.getStopwatch("protostuff-deserialize"));
		showAverage(SimonManager.getStopwatch("java-serialize"));
		showAverage(SimonManager.getStopwatch("java-deserialize"));
		
	}
}
