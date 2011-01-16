package org.directmemory.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.recipes.CacheRecipes;
import org.directmemory.storage.OffHeapStorage;
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

	static int howMany = 0;
	static int threadCount = 0;
	static int entriesInHeap = 0;
	static int payloadSize = 0;
	static int pstuffBufferSizeInKb = 0;
	static int pageSize = 0;
	static int maxPages = 0;
	static int batchSize = 0;
	static int logEvery = 0;
	static int showStatusEvery = 0;

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
		threadCount = new Integer(properties.getProperty("demo.threadCount","10")).intValue();
		entriesInHeap = new Integer(properties.getProperty("demo.entriesInHeap","1000")).intValue();
		payloadSize = Ram.Kb(new Integer(properties.getProperty("demo.payloadInKb","2")).intValue());
		pstuffBufferSizeInKb = Ram.Kb(new Integer(properties.getProperty("demo.pstuffBufferSizeInKb","8")).intValue());
		pageSize = Ram.Mb(new Integer(properties.getProperty("demo.pageSizeInMb","256")).intValue());
		maxPages = new Integer(properties.getProperty("demo.maxPages","1")).intValue();
		batchSize = new Integer(properties.getProperty("demo.batchSize","100")).intValue();
		logEvery = new Integer(properties.getProperty("demo.logEvery","100")).intValue();
		showStatusEvery = new Integer(properties.getProperty("demo.showStatusEvery","1000")).intValue();
		
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.threadCount=" + threadCount);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.payload=" + payloadSize);
		logger.info("demo.pstuffBufferSize=" + pstuffBufferSizeInKb);
		logger.info("demo.pageSize=" + pageSize);
		logger.info("demo.maxPages=" + maxPages);
		logger.info("demo.batchSize=" + batchSize);
		logger.info("demo.logEvery=" + logEvery);
		logger.info("demo.showStatusEvery=" + showStatusEvery);
	}
	
	
	
	
    public static void main( String[] args ) 
    {
		logger.info("DirectMemory Cache - Standalone Demo Starting");

		logger.info("Log check - if you see this it is fine");
		logger.warn("Log check - if you see this it is fine");
		logger.debug("Log check - if you see this it is fine");
		logger.error("Log check - if you see this it is fine");

		loadProperties("conf/demo");
		offHeapMultiThreadedTest();
//		cacheManagerTest();
    }
    
    
    private static void offHeapMultiThreadedTest() {
    	long startedAt = new Date().getTime();
    	
        OffHeapStorage storage = new OffHeapStorage(pageSize, maxPages);
        
        logger.info("Starting inserting " + howMany + " entries");
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

        	logger.info("Cache after " + howMany + " inserts - " + storage.toString());
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

	    	logger.info("Cache after " + howMany + " reads - " + storage.toString());
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

    
    private static void singleThreading() {
		int errors = 0;
    	int misses = 0;

    	OffHeapStorage cache = new OffHeapStorage(pageSize, maxPages);
        int partial = 0;
        int partialShow = 0;
        
        logger.info("Starting inserting " + howMany + " entries");
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

    
    private static void offHeapTest() {
		int errors = 0;
    	int misses = 0;

    	OffHeapStorage cache = new OffHeapStorage(pageSize, maxPages);
        int partial = 0;
        int partialShow = 0;
        
        logger.info("Starting inserting " + howMany + " entries");
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

	@SuppressWarnings("unused")
	private static void cacheManagerTest() {
		int errors = 0;
    	int misses = 0;
    	try {
	    	cache = CacheRecipes.CreateYourOwn(entriesInHeap, pageSize, maxPages, pstuffBufferSizeInKb, batchSize);        
	        logger.info("Cache initialized - " + cache.toString());
	        
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
}
