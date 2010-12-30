package org.directmemory.test;

import org.directmemory.CacheStore;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.directmemory.supervisor.AsyncBatchSupervisor;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicMultiThreadedTest {

	private static Logger logger=LoggerFactory.getLogger(BasicMultiThreadedTest.class);

	private abstract class ThreadUsingCache extends Thread {
		public ThreadUsingCache(ThreadGroup group, String name, CacheStore cache) {
			super(group, name);
			this.cache = cache;
		}

		public CacheStore cache;
	}
	
	public static CacheStore cache = null;
	public static Split wholeTestSplit = null;
	
	@BeforeClass
	public static void setup() {
        Stopwatch stopWatch = SimonManager.getStopwatch("test");
        wholeTestSplit = stopWatch.start();
		cache = new CacheStore(100, 10 * 1024 * 1024, 1);
		cache.serializer = new ProtoStuffSerializer();
		cache.supervisor = new AsyncBatchSupervisor(500);
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
		
		logger.debug(cache.toString());
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

		logger.debug(cache.toString());
	}
	
	@AfterClass
	public static void checkPerformance() {
		wholeTestSplit.stop();
		CacheStore.displayTimings();
	}
}
