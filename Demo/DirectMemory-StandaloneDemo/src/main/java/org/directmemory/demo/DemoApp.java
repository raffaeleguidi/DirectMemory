package org.directmemory.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager;
import org.directmemory.cache.CacheManager2;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.recipes.CacheRecipes;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.directmemory.storage.HeapStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.OrientDBStorage;
import org.directmemory.store.SimpleOffHeapStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class DemoApp 
{
	private static Logger logger=LoggerFactory.getLogger(DemoApp.class);

	static CacheManager cache = null;
	static Properties properties = new Properties();

	static String demoToRun = "";
	static int howMany = 0;
	static int threadCount = 0;
	static int entriesInHeap = -1;
	static int entriesOffHeap = -1;
	static int payloadSize = 0;
	static int pstuffBufferSizeInKb = 0;
	static int pageSize = 0;
	static int maxPages = 0;
	static int batchSize = 0;
	static int logEvery = 0;
	static int showStatusEvery = 0;
	static int writersPercentage = 0;
	static int writingThreads = 0;
	static int readingThreads = 0;

	static int errors = 0;
	static int misses = 0;

	static void loadProperties(String fileName) {
		try {
			File file = new File(fileName + ".properties");
			logger.info("loading file '" + file.getAbsolutePath() + "'");
		    properties.load(new FileInputStream(file));
			logger.info("loaded file '" + fileName + ".properties'");
		} catch (IOException e) {
			logger.warn("file '" + fileName + ".properties' not found, using defaults");
		}

		howMany = new Integer(properties.getProperty("demo.numberOfEntries","10000")).intValue();
		demoToRun = properties.getProperty("demo.demoToRun","default");
		threadCount = new Integer(properties.getProperty("demo.threadCount","10")).intValue();
		entriesInHeap = new Integer(properties.getProperty("demo.entriesInHeap","1000")).intValue();
		entriesOffHeap = new Integer(properties.getProperty("demo.entriesOffHeap","-1")).intValue();
		payloadSize = Ram.Kb(new Integer(properties.getProperty("demo.payloadInKb","2")).intValue());
		pstuffBufferSizeInKb = Ram.Kb(new Integer(properties.getProperty("demo.pstuffBufferSizeInKb","8")).intValue());
		pageSize = Ram.Mb(new Integer(properties.getProperty("demo.pageSizeInMb","256")).intValue());
		maxPages = new Integer(properties.getProperty("demo.maxPages","1")).intValue();
		batchSize = new Integer(properties.getProperty("demo.batchSize","100")).intValue();
		logEvery = new Integer(properties.getProperty("demo.logEvery","100")).intValue();
		showStatusEvery = new Integer(properties.getProperty("demo.showStatusEvery","1000")).intValue();
		writersPercentage = new Integer(properties.getProperty("demo.writersPercentage","50")).intValue();
	}
	
	
	
	
    public static void main( String[] args ) throws InterruptedException 
    {
		logger.info("DirectMemory Cache - Standalone Demo Starting");
//
//		logger.info("Log check - if you see this as INFO it is fine");
//		logger.warn("Log check - if you see this as WARNING it is fine");
//		logger.debug("Log check - if you see this as DEBUG it is fine");
//		logger.error("Log check - if you see this as ERROR it is fine");

		loadProperties("conf/demo");
		
		if (demoToRun.equals("default")) {
			cacheManager2MTTest();
		} else if (demoToRun.equals("cachemanager2mt")) {
			cacheManager2MTTest();
		} else if (demoToRun.equals("cachemanager2")) {
			cacheManager2Test();
		} else if (demoToRun.equals("multithreaded")) {
			offHeapMultiThreadedTest();
		} else if (demoToRun.equals("mixedandmultithreaded")) {
			mixedAndMultiThreadedTest();
		} else if (demoToRun.equals("singlethreaded")) {
			offHeapSingleThreaded();
		} else if (demoToRun.equals("cachemanager")) {
			cacheManagerTest();
		} else if (demoToRun.equals("singlethreadednopojoreuse")) {
			singleThreadedNoPojoReuse();
		}

    }
    
    
	public static void cacheManager2MTTest() throws InterruptedException {		
		
		logger.info("Starting test with " + howMany + " entries");

		logger.info("demo.demoToRun=" + demoToRun);
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.entriesOffHeap=" + entriesOffHeap);
		logger.info("demo.threadCount=" + threadCount);
		logger.info("demo.writersPercentage=" + writersPercentage);
		logger.info("demo.payload=" + payloadSize);
		logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);

		long startedAt = Calendar.getInstance().getTimeInMillis();
		SimpleOffHeapStore secondLevel = new SimpleOffHeapStore();
		secondLevel.queueSize = 100;
		secondLevel.serializer = new ProtoStuffSerializer(payloadSize + Ram.Kb(1));
//		secondLevel.serializer = new StandardSerializer();
//		secondLevel.serializer = new DummyPojoSerializer();
		CacheManager2 cache = new CacheManager2(entriesInHeap, secondLevel, entriesOffHeap);
		logger.info(cache.toString());
		
		ThreadGroup putters = new ThreadGroup("putter");
		
		for (int i = 0; i < threadCount; i++) {
			new Thread(putters, "run-" + i) {
				public Thread withCache(CacheManager2 cache, String name) {
					this.cache = cache;
					this.name = name;
					return this;
				}
				private CacheManager2 cache;
				private String name;
				@Override
				public void run() {
					int paging = 0;
					for (int i = 1; i <= howMany/threadCount; i++) {
						DummyPojo pojo = new DummyPojo(name+"-"+i, payloadSize);
						cache.put(pojo.name, pojo);
						logger.info("put pojo with key " + pojo.name);
						Thread.yield();
						paging++;
						if (paging==(howMany/threadCount/10)) {
							logger.info("thread " + name + " putting " + ((int)((double)i/howMany*1000)) + "% done");
							paging = 0;
						}
					}
				}
			}.withCache(cache, "entry-" + i).start();
		}

		while (putters.activeCount() > 0)
			Thread.yield();		
		
		long finishedPutting = Calendar.getInstance().getTimeInMillis();
		logger.info("Created, serialized and put " + howMany + " DummyPojos in " + cache.uptime() + " milliseconds");
		logger.info(cache.toString());
		logger.info(cache.measures());
		
		Thread.sleep(500);

		ThreadGroup mixed = new ThreadGroup("mixed");
		
		writingThreads = (int)(threadCount*((double)writersPercentage/100));
		readingThreads = threadCount-writingThreads;
		
		logger.info("Starting " + readingThreads + " reading threads");
		
		for (int i = 0; i < readingThreads; i++) {
			new Thread(mixed, "run-" + i) {
				public Thread withCache(CacheManager2 cache, String name) {
					this.cache = cache;
					this.name = name;
					return this;
				}
				private CacheManager2 cache;
				private String name;
				@Override
				public void run() {
					Random rnd = new Random();
					int paging = 0;
					for (int i = 1; i <= howMany/readingThreads; i++) {
						int num = rnd.nextInt(howMany/threadCount)+1;
						int threadNumber = rnd.nextInt(threadCount);
						String pojoName = "entry-" + threadNumber + "-" + num;
						DummyPojo pojo = (DummyPojo)cache.get(pojoName);
						if (pojo == null) {
							logger.error("no pojo for key " + pojoName);
							misses++;
						} else {
							if (!pojo.name.equals(pojoName)) {
								logger.error("bad pojo for key " + pojoName);
								errors++;
							}
						}
						Thread.yield();
					}
				}
			}.withCache(cache, "read-" + i).start();
		}
		
		logger.info("Starting " + writingThreads + " writing threads");

		for (int i = 0; i < writingThreads; i++) {
			new Thread(mixed, "run-" + i) {
				public Thread withCache(CacheManager2 cache, String name) {
					this.cache = cache;
					this.name = name;
					return this;
				}
				private CacheManager2 cache;
				private String name;
				@Override
				public void run() {
					Random rnd = new Random();
					for (int i = 1; i <= howMany/writingThreads; i++) {
						int num = rnd.nextInt(howMany)+1;
						int threadNumber = rnd.nextInt(threadCount);
						String pojoName = "entry-" + threadNumber + "-" + num;
						DummyPojo pojo = new DummyPojo(pojoName, payloadSize);
						cache.put(pojo.name, pojo);
						Thread.yield();
					}
				}
			}.withCache(cache, "write-" + i).start();
		}

					
		while (mixed.activeCount() > 0)
			Thread.yield();		
		
		logger.info(cache.toString());
		logger.info(cache.measures());
		logger.info("Got and deserialized " + howMany + " entries (with " + readingThreads + " readers vs " + writingThreads + " writers) in " + (System.currentTimeMillis() - finishedPutting) + " milliseconds");
		logger.info(errors + " errors and " + misses);
		cache.dispose();
		logger.info("Done in " + (System.currentTimeMillis() - startedAt) + " milliseconds");
	}

 
	public static void cacheManager2Test() throws InterruptedException {		
		logger.info("Starting test with " + howMany + " entries");
	
		logger.info("demo.demoToRun=" + demoToRun);
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.threadCount=" + threadCount);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.entriesOffHeap=" + entriesOffHeap);
		logger.info("demo.payload=" + payloadSize);
		logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);
		logger.info("demo.pageSize=" + pageSize);
		logger.info("demo.maxPages=" + maxPages);
		logger.info("demo.batchSize=" + batchSize);
		logger.info("demo.logEvery=" + logEvery);
		logger.info("demo.showStatusEvery=" + showStatusEvery);

		long startedAt = Calendar.getInstance().getTimeInMillis();
		SimpleOffHeapStore secondLevel = new SimpleOffHeapStore();
		secondLevel.queueSize = 100;
		secondLevel.serializer = new ProtoStuffSerializer(payloadSize + Ram.Kb(1));
//		secondLevel.serializer = new StandardSerializer();
//		secondLevel.serializer = new DummyPojoSerializer();
		CacheManager2 cache = new CacheManager2(entriesInHeap, secondLevel, entriesOffHeap);
		logger.info(cache.toString());
		
		int paging = 0;
		
		for (int i = 1; i <= howMany; i++) {
			DummyPojo pojo = new DummyPojo("test", payloadSize);
			pojo.name = "test" + i;
			cache.put(pojo.name, pojo);
			paging++;
			if (paging==(howMany/10)) {
				logger.info("putting " + ((double)i/howMany*100) + "% done");
				paging = 0;
			}
		}

		long finishedPutting = Calendar.getInstance().getTimeInMillis();
		logger.info("Created, serialized and put " + howMany + " DummyPojos in " + cache.uptime() + " milliseconds");
		logger.info(cache.toString());
		logger.info(cache.measures());

		paging = 0;
		for (int i = 1; i <= howMany; i++) {
			DummyPojo pojo = (DummyPojo)cache.get("test" + i);
			if (pojo == null) {
				logger.error("bad pojo for key test" + i);
			}
			paging++;
			if (paging==(howMany/10)) {
				logger.info("getting " + ((double)i/howMany*100) + "% done");
				paging = 0;
			}
		}
		
		logger.info(cache.toString());
		logger.info(cache.measures());
		logger.info("Got and deserialized " + howMany + " entries in " + (System.currentTimeMillis() - finishedPutting) + " milliseconds");
		cache.dispose();
		logger.info("Done in " + (System.currentTimeMillis() - startedAt) + " milliseconds");
	}



	private static void offHeapMultiThreadedTest() {
    	long startedAt = new Date().getTime();
    	
    	OffHeapStorage storage = new OffHeapStorage(pageSize, maxPages);

        logger.info("Starting inserting " + howMany + " entries");
		
		logger.info("demo.demoToRun=" + demoToRun);
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.threadCount=" + threadCount);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.entriesOffHeap=" + entriesOffHeap);
		logger.info("demo.payload=" + payloadSize);
		logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);
		logger.info("demo.pageSize=" + pageSize);
		logger.info("demo.maxPages=" + maxPages);
		logger.info("demo.batchSize=" + batchSize);
		logger.info("demo.logEvery=" + logEvery);
		logger.info("demo.showStatusEvery=" + showStatusEvery);

		logger.info("Heap size: " + Ram.inMb(Runtime.getRuntime().maxMemory())+ " free: " + Ram.inMb(Runtime.getRuntime().freeMemory()));
        
        ThreadGroup group = new ThreadGroup("test");
        
        try {
        	for (int i = 0; i < threadCount; i++) {
            	MyThread thread = new MyThread(group, "thread-"+i) {
    				public void run() {
    			        int partial = 0;
    			        int partialShow = 0;
    			        
    			        for (int i = 0; i < (howMany/threadCount); i++) {
    			        	CacheEntry entry = new CacheEntry();
    						Thread.yield();		
    			        	pojo.name = name + "-" + i;
    			        	entry.key = pojo.name;
    			        	entry.object = pojo;
    						storage.put(entry);
    						if (partial++ == logEvery) {
    							logger.debug("entry " + pojo.name + " inserted");
    							partial = 0;
    						}
    						if (partialShow++ == showStatusEvery) {
    					        logger.info(storage.toString());
    					        logger.info(CacheManager.getTimings());
    							partialShow = 0;
    						}
    					}
    				}
            	};
            	thread.pojo = new DummyPojo("test", payloadSize);
            	thread.storage = storage;
            	thread.index = i;
            	thread.start();
			}

			while (group.activeCount() > 0)
				Thread.yield();		

        	logger.info("Cache after " + howMany + " inserts: " + storage.toString());
	        logger.info(CacheManager.getTimings());

	    	long finishedInsertAt = new Date().getTime();

	        
	        logger.info("Beginning check reads");

        	for (int i = 0; i < threadCount; i++) {
            	MyThread thread = new MyThread(group, "thread-"+i) {
    				public void run() {
    			        int partial = 0;
    			        int partialShow = 0;
    			        
    			        for (int i = 0; i < (howMany/threadCount); i++) {
	    		        	CacheEntry checkEntry = storage.get(name+"-" + i);
	    		        	if (checkEntry != null) {
	    						DummyPojo check = (DummyPojo)checkEntry.object;
	    						Thread.yield();		
	    						if (check != null) {
	    							if (!check.name.equals(name+"-" + i)) {
	    						        logger.error("check " + check.name + " doesn't match");
	    						        errors++;
	    							}
	    						}
	    					} else {
	    						misses++;
	    					}
	    					if (partial++ == logEvery) {
	    						logger.debug("entry " + i + " read");
	    						partial = 0;
	    					}
	    					if (partialShow++ == showStatusEvery) {
	    				        logger.info(storage.toString());
	    						logger.info("Errors=" + errors + " and misses=" + misses);
	    				        logger.info(CacheManager.getTimings());
	    						partialShow = 0;
	    					}    				
	    				}
    				}
            	};
            	thread.pojo = new DummyPojo("test", payloadSize);
            	thread.storage = storage;
            	thread.index = i;
            	thread.start();
			}

			while (group.activeCount() > 0)
				Thread.yield();		

	    	long finishedAllAt = new Date().getTime();

	    	logger.info("Cache after " + howMany + " reads: " + storage.toString());
	        logger.info(CacheManager.getTimings());
	        storage.dispose();
	    	logger.info("Insert done in " + (finishedInsertAt-startedAt)/1000d + " seconds");
	    	logger.info("Read done in " + (finishedAllAt-finishedInsertAt)/1000d + " seconds");
	    	logger.info("Total time " + (finishedAllAt-startedAt)/1000d + " seconds");
	    	logger.info("DirectMemory Cache - Goodbye!");
		} catch (Exception e) {
			logger.error("Exception catched:",  e);
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory:",  e);
		}
	}

    
    private static void mixedAndMultiThreadedTest() {
    	long startedAt = new Date().getTime();
    	
        HeapStorage storage = new HeapStorage(entriesInHeap);
        storage.next = new OffHeapStorage(pageSize, maxPages);
        storage.next.first = storage;
        storage.next.next = new OrientDBStorage();
        storage.next.next.first = storage;

        logger.info("Starting inserting " + howMany + " entries");
		
		logger.info("demo.demoToRun=" + demoToRun);
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.threadCount=" + threadCount);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.entriesOffHeap=" + entriesOffHeap);
		logger.info("demo.payload=" + payloadSize);
		logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);
		logger.info("demo.pageSize=" + pageSize);
		logger.info("demo.maxPages=" + maxPages);
		logger.info("demo.batchSize=" + batchSize);
		logger.info("demo.logEvery=" + logEvery);
		logger.info("demo.showStatusEvery=" + showStatusEvery);
        
        logger.info("Heap size: " + Ram.inMb(Runtime.getRuntime().maxMemory())+ " free: " + Ram.inMb(Runtime.getRuntime().freeMemory()));
        
        ThreadGroup group = new ThreadGroup("test");
        
        try {
        	for (int i = 0; i < threadCount; i++) {
            	MyThread thread = new MyThread(group, "thread-"+i) {
    				public void run() {
    			        int partial = 0;
    			        int partialShow = 0;
    			        
    			        for (int i = 0; i < (howMany/threadCount); i++) {
    			        	CacheEntry entry = new CacheEntry();
    						Thread.yield();		
    			        	pojo.name = name + "-" + i;
    			        	entry.key = pojo.name;
    			        	entry.object = pojo;
    						storage.put(entry);
    						if (partial++ == logEvery) {
    							logger.debug("entry " + pojo.name + " inserted");
    							partial = 0;
    						}
    						if (partialShow++ == showStatusEvery) {
    					        logger.info(storage.toString());
    					        logger.info(CacheManager.getTimings());
    							partialShow = 0;
    						}
    					}
    				}
            	};
            	thread.pojo = new DummyPojo("test", payloadSize);
            	thread.storage = storage;
            	thread.index = i;
            	thread.start();
			}

			while (group.activeCount() > 0)
				Thread.yield();		

        	logger.info("Cache after " + howMany + " inserts: \r\n   " + storage.toString() + "\r\n   " + storage.next.toString() + "\r\n   " + storage.next.next.toString());
        	logger.info(storage.next.toString());
	        logger.info(CacheManager.getTimings());

	    	long finishedInsertAt = new Date().getTime();

	        
	        logger.info("Beginning check reads");

        	for (int i = 0; i < threadCount; i++) {
            	MyThread thread = new MyThread(group, "thread-"+i) {
    				public void run() {
    			        int partial = 0;
    			        int partialShow = 0;
    			        
    			        for (int i = 0; i < (howMany/threadCount); i++) {
	    		        	CacheEntry checkEntry = storage.get(name+"-" + i);
	    		        	if (checkEntry != null) {
	    						DummyPojo check = (DummyPojo)checkEntry.object;
	    						Thread.yield();		
	    						if (check != null) {
	    							if (!check.name.equals(name+"-" + i)) {
	    						        logger.error("check " + check.name + " doesn't match");
	    						        errors++;
	    							}
	    						}
	    					} else {
	    						misses++;
	    					}
	    					if (partial++ == logEvery) {
	    						logger.debug("entry " + i + " read");
	    						partial = 0;
	    					}
	    					if (partialShow++ == showStatusEvery) {
	    				        logger.info(storage.toString());
	    						logger.info("Errors=" + errors + " and misses=" + misses);
	    				        logger.info(CacheManager.getTimings());
	    						partialShow = 0;
	    					}    				
	    				}
    				}
            	};
            	thread.pojo = new DummyPojo("test", payloadSize);
            	thread.storage = storage;
            	thread.index = i;
            	thread.start();
			}

			while (group.activeCount() > 0)
				Thread.yield();		

	    	long finishedAllAt = new Date().getTime();

	    	logger.info("Cache after " + howMany + " reads: \r\n   " + storage.toString() + "\r\n   " + storage.next.toString() + "\r\n   " + storage.next.next.toString());
	        logger.info(CacheManager.getTimings());
	        storage.dispose();
	    	logger.info("Insert done in " + (finishedInsertAt-startedAt)/1000d + " seconds");
	    	logger.info("Read done in " + (finishedAllAt-finishedInsertAt)/1000d + " seconds");
	    	logger.info("Total time " + (finishedAllAt-startedAt)/1000d + " seconds");
	    	logger.info("DirectMemory Cache - Goodbye!");
		} catch (Exception e) {
			logger.error("Exception catched:",  e);
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory:",  e);
		}
	}

    
    private static void offHeapSingleThreaded() {
		int errors = 0;
    	int misses = 0;

    	OffHeapStorage cache = new OffHeapStorage(pageSize, maxPages);
        int partial = 0;
        int partialShow = 0;
        
        logger.info("Starting inserting " + howMany + " entries");
		
		logger.info("demo.demoToRun=" + demoToRun);
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.threadCount=" + threadCount);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.entriesOffHeap=" + entriesOffHeap);
		logger.info("demo.payload=" + payloadSize);
		logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);
		logger.info("demo.pageSize=" + pageSize);
		logger.info("demo.maxPages=" + maxPages);
		logger.info("demo.batchSize=" + batchSize);
		logger.info("demo.logEvery=" + logEvery);
		logger.info("demo.showStatusEvery=" + showStatusEvery);
        logger.info("Heap size: " + Ram.inMb(Runtime.getRuntime().maxMemory())+ " free: " + Ram.inMb(Runtime.getRuntime().freeMemory()));
        
    	DummyPojo pojo = new DummyPojo("test", payloadSize);
        try {
	        for (int i = 0; i < howMany; i++) {
	        	CacheEntry entry = new CacheEntry();
	        	pojo.name = "test" + i;
	        	entry.key = pojo.name;
	        	entry.object = pojo;
				cache.put(entry);
				if (partial++ == logEvery) {
					logger.debug("entry " + i + " inserted");
					partial = 0;
				}
				if (partialShow++ == showStatusEvery) {
			        logger.info(cache.toString());
			        logger.info(CacheManager.getTimings());
					partialShow = 0;
				}
			}
	        logger.info("Cache after " + howMany + " inserts - " + cache.toString());
	        logger.info(CacheManager.getTimings());
	
	        logger.info("Beginning check reads");
	        for (int i = 0; i < howMany; i++) {
	        	CacheEntry checkEntry = cache.get("test" + i);
	        	if (checkEntry != null) {
					DummyPojo check = (DummyPojo)checkEntry.object;
					if (check != null) {
						if (!check.name.equals("test"+i)) {
					        logger.error("check " + check.name + " doesn't match");
					        errors++;
						}
					}
				} else {
					misses++;
				}
				if (partial++ == logEvery) {
					logger.debug("entry " + i + " read");
					partial = 0;
				}
				if (partialShow++ == showStatusEvery) {
			        logger.info(cache.toString());
					logger.info("Errors=" + errors + " and misses=" + misses);
			        logger.info(CacheManager.getTimings());
					partialShow = 0;
				}
			}
	        logger.info("Cache after " + howMany + " reads - " + cache.toString());
	        logger.info(CacheManager.getTimings());
	        cache.dispose();
	    	logger.info("DirectMemory Cache - Goodbye!");
		} catch (Exception e) {
			logger.error("Exception catched:",  e);
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory:",  e);
		}
	}

    
    private static void singleThreadedNoPojoReuse() {
		int errors = 0;
    	int misses = 0;

    	OffHeapStorage cache = new OffHeapStorage(pageSize, maxPages);
        int partial = 0;
        int partialShow = 0;
        
        logger.info("Starting inserting " + howMany + " entries");

		
		logger.info("demo.demoToRun=" + demoToRun);
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.threadCount=" + threadCount);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.entriesOffHeap=" + entriesOffHeap);
		logger.info("demo.payload=" + payloadSize);
		logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);
		logger.info("demo.pageSize=" + pageSize);
		logger.info("demo.maxPages=" + maxPages);
		logger.info("demo.batchSize=" + batchSize);
		logger.info("demo.logEvery=" + logEvery);
		logger.info("demo.showStatusEvery=" + showStatusEvery);

		logger.info("Heap size: " + Ram.inMb(Runtime.getRuntime().maxMemory())+ " free: " + Ram.inMb(Runtime.getRuntime().freeMemory()));
        
        try {
	        for (int i = 0; i < howMany; i++) {
	        	DummyPojo pojo = new DummyPojo("test" + i, payloadSize);
	        	CacheEntry entry = new CacheEntry();
	        	entry.key = pojo.name;
	        	entry.object = pojo;
				cache.put(entry);
				if (partial++ == logEvery) {
					logger.debug("entry " + i + " inserted");
					partial = 0;
				}
				if (partialShow++ == showStatusEvery) {
			        logger.info(cache.toString());
			        logger.info(CacheManager.getTimings());
					partialShow = 0;
				}
			}
	        logger.info("Cache after " + howMany + " inserts - " + cache.toString());
	        logger.info(CacheManager.getTimings());
	
	        logger.info("Beginning check reads");
	        for (int i = 0; i < howMany; i++) {
	        	CacheEntry checkEntry = cache.get("test" + i);
	        	if (checkEntry != null) {
					DummyPojo check = (DummyPojo)checkEntry.object;
					if (check != null) {
						if (!check.name.equals("test"+i)) {
					        logger.error("check " + check.name + " doesn't match");
					        errors++;
						}
					}
				} else {
					misses++;
				}
				if (partial++ == logEvery) {
					logger.debug("entry " + i + " read");
					partial = 0;
				}
				if (partialShow++ == showStatusEvery) {
			        logger.info(cache.toString());
					logger.info("Errors=" + errors + " and misses=" + misses);
			        logger.info(CacheManager.getTimings());
					partialShow = 0;
				}
			}
	        logger.info("Cache after " + howMany + " reads - " + cache.toString());
	        logger.info(CacheManager.getTimings());
	        cache.dispose();
	    	logger.info("DirectMemory Cache - Goodbye!");
		} catch (Exception e) {
			logger.error("Exception catched:",  e);
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory:",  e);
		}
	}

	private static void cacheManagerTest() {
		int errors = 0;
    	int misses = 0;
    	try {
	    	cache = CacheRecipes.CreateYourOwn(entriesInHeap, pageSize, maxPages, pstuffBufferSizeInKb, batchSize);        
	        logger.info("Cache initialized - " + cache.toString());
	        
			
			logger.info("demo.demoToRun=" + demoToRun);
			logger.info("demo.numberOfEntries=" + howMany);
			logger.info("demo.threadCount=" + threadCount);
			logger.info("demo.entriesInHeap=" + entriesInHeap);
			logger.info("demo.entriesOffHeap=" + entriesOffHeap);
			logger.info("demo.payload=" + payloadSize);
			logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);
			logger.info("demo.pageSize=" + pageSize);
			logger.info("demo.maxPages=" + maxPages);
			logger.info("demo.batchSize=" + batchSize);
			logger.info("demo.logEvery=" + logEvery);
			logger.info("demo.showStatusEvery=" + showStatusEvery);

			int partial = 0;
	        int partialShow = 0;
	        
	        logger.info("Starting inserting " + howMany + " entries");
	        for (int i = 0; i < howMany; i++) {
				cache.put("test" + i, new DummyPojo("test" + i, payloadSize));
				if (partial++ == logEvery) {
					logger.debug("entry " + i + " inserted");
					partial = 0;
				}
				if (partialShow++ == showStatusEvery) {
			        logger.info(cache.toString());
			        logger.info(CacheManager.getTimings());
					partialShow = 0;
				}
			}
	        logger.info("Cache after " + howMany + " inserts - " + cache.toString());
	        logger.info(CacheManager.getTimings());
	
	        logger.info("Beginning check reads");
	        for (int i = 0; i < howMany; i++) {
				DummyPojo check = (DummyPojo)cache.get("test" + i);
				if (partial++ == logEvery) {
					logger.debug("entry " + i + " read");
					partial = 0;
				}
				if (partialShow++ == showStatusEvery) {
			        logger.info(cache.toString());
					logger.info("Errors=" + errors + " and misses=" + misses);
			        logger.info(CacheManager.getTimings());
					partialShow = 0;
				}
				
				if (check != null) {
					if (!check.name.equals("test"+i)) {
				        logger.error("check " + check.name + " doesn't match");
				        errors++;
					}
				} else {
					misses++;
				}
			}
	        logger.info("Cache after " + howMany + " read and writes - " + cache.toString());
	        logger.info(CacheManager.getTimings());
	        cache.dispose();
	    	logger.info("DirectMemory Cache - Goodbye!");
    	} catch (Exception e) {
    		logger.error("Exception catched:",  e);
    	} catch (OutOfMemoryError e) {
    		logger.error("Out of memory:",  e);
    	}
	}
}
