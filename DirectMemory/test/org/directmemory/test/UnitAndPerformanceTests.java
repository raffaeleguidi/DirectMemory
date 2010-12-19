package org.directmemory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directmemory.ICacheEntry;
import org.directmemory.ICacheStore;
import org.directmemory.ICacheSupervisor;
import org.directmemory.impl.CacheStoreImpl;
import org.directmemory.impl.SimpleCacheSupervisor;
import org.directmemory.test.misc.DummyObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnitAndPerformanceTests {
	
	private static Logger logger=LoggerFactory.getLogger(UnitAndPerformanceTests.class);

	@Rule
    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
	static CacheStoreImpl cache = null;
	static int objectsSize = 2048*2;
	static Random generator = new Random();
	static int cacheSize = 490*1024*1024;
	
	public static void allocateMemory() {
		String mb2useFromCommandLine = System.getProperty("mb2use");
		if (mb2useFromCommandLine != null) {
			logger.debug("using mb2useFromCommandLine=" + mb2useFromCommandLine);
			cacheSize = new Integer(mb2useFromCommandLine)*1024*1024;
			logger.debug("cacheSize=" + cacheSize);
		}
		cache = new CacheStoreImpl(cacheSize);
//		cache.setDefaultDuration(1000);
		logger.debug(cache.toString());
	}
		
	@BeforeClass
	public static void setup() throws Exception {
		allocateMemory();
	}

	private void belowLimit(int cacheSize, ICacheSupervisor supervisor) throws IOException, ClassNotFoundException {

		Long startedAt = Calendar.getInstance().getTimeInMillis();
		
		ICacheStore cache = new CacheStoreImpl(cacheSize);
		cache.setSupervisor(supervisor);

		logger.debug("started");
	    for (int i = 0; i < cacheSize / 1024 / 1.25; i++) {
	    	cache.put("test" + i, new DummyObject("test"+i, 1024));
//	    	DummyObject retrObj = (DummyObject)cache.retrieveObject("test"+i);
//		    logger.debug(retrObj.getName());
	    }
	    
	    Long finishedAt = Calendar.getInstance().getTimeInMillis();
		
	    logger.debug("finished in " + (finishedAt - startedAt) + " msecs");
	    logger.debug("" + cache.toString());

	    cache.dispose();

	    logger.debug("" + cache.toString());
	}
	
	private void aboveLimit(int cacheSize, ICacheSupervisor supervisor) throws IOException, ClassNotFoundException {

		Long startedAt = Calendar.getInstance().getTimeInMillis();
		
		ICacheStore cache = new CacheStoreImpl(cacheSize);
		cache.setSupervisor(supervisor);

		logger.debug("started");
	    for (int i = 0; i < cacheSize / 1024; i++) {
	    	cache.put("test" + i, new DummyObject("test"+i, 1024));
	    }
	    
	    Long finishedAt = Calendar.getInstance().getTimeInMillis();
		
	    logger.debug("finished in " + (finishedAt - startedAt) + " msecs");
	    logger.debug("" + cache.toString());

	    cache.dispose();

	    logger.debug("" + cache.toString());
	}
	
	private String randomKey() {
		return "key"+generator.nextInt(cacheSize/objectsSize);
	}

	public DummyObject randomObject() {
    	String key = randomKey();
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize*generator.nextInt(5)];
		return dummy;
	}
	
	long lastOne = 0;
	
	public DummyObject nextObject() {
    	String key = "key"+lastOne++;
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize];
		return dummy;
	}
	
	@Test
	public void fillUpTest() throws IOException, ClassNotFoundException {
		int cacheSize = 50*1024*1024;

		belowLimit(cacheSize, new SimpleCacheSupervisor());
		aboveLimit(cacheSize, new SimpleCacheSupervisor());
	}
	
	
    @Test
    //@Required(max = 50)
    @Required(max = 300) // well under 50ms in sun jvm
    public void firstAndLargestItem() throws IOException { 	
    	DummyObject firstObject = new DummyObject("key0", objectsSize*5);
    	cache.put(firstObject.getName(), firstObject);
    	assertEquals(cache.entries().size(), 1);
    	DummyObject retrievedObject = (DummyObject)cache.get("key0");
    	assertEquals(firstObject.getName(), retrievedObject.getName());
		logger.debug(cache.toString());
    }
    
	private void doSomeReads(int howMany) {
		for (int i = 0; i < howMany; i++) {
    		@SuppressWarnings("unused")
			DummyObject randomPick = (DummyObject)cache.get(randomKey());
			
		}
	}
	@Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 1)
	public void onlyWrites() throws IOException {
    	DummyObject object2add = nextObject();
    	ICacheEntry entry = cache.put(object2add.getName(), object2add);
    	assertNotNull(entry);
	}

	@Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 3000, average = 3)
	public void onlyWrites10Threads() throws Exception {
    	onlyWrites();		
	}

	@Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 6000, average = 6)
	public void onlyWrites20Threads() throws IOException {
    	onlyWrites();		
	}

	@Test
    public void testAll() {
		Map<String, ICacheEntry> entries = cache.entries();
		
		long mySize = 0;
		
    	Iterator<ICacheEntry> iter = entries.values().iterator();
		while (iter.hasNext()) {
			ICacheEntry entry = iter.next();
			if (!entry.expired()) {
				DummyObject dummy = (DummyObject) cache.get(entry.getKey());
				assertNotNull(dummy);
				assertEquals(entry.getKey(), dummy.getName());
			}
			mySize+=entry.size();
		}

		logger.debug("**** computed size is: " + mySize);	
		logger.debug(cache.toString());	
    }
	
    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 750, average = 0.5)
    public void onlyReads() { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.get(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 750, average = 0.5)
    public void onlyReads10Threads() { 	
    	onlyReads();
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1100, average = 0.5)
    public void onlyReads20Threads() { 	
    	onlyReads();
    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 6)
    public void twentyReadsOneWriteOneDelete() throws IOException { 	
    	doSomeReads(20);
    	DummyObject object2add = randomObject();
    	cache.put(object2add.getName(), object2add);
    	cache.delete(randomKey());
    }

	@Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1600, average = 12)
    public void twentyReadsOneWriteOneDelete10Threads() throws IOException { 	
		twentyReadsOneWriteOneDelete();
	}
	
	@Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 24)
    public void twentyReadsOneWriteOneDelete20Threads() throws IOException { 	
		twentyReadsOneWriteOneDelete();
	}

    @Test
    public void testAllOnceAgain() {
    	testAll();
    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 4)
    public void twoReadsOneWriteOneDelete() throws IOException { 	
    	doSomeReads(2);
    	DummyObject object2add = randomObject();
    	cache.put(object2add.getName(), object2add);
    	cache.delete(randomKey());
    }

    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1500, average = 8)
    public void twoReadsOneWriteOneDelete10Threads() throws IOException { 	
    	twoReadsOneWriteOneDelete();
    }

    @Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 5)
    public void twoReadsOneWriteOneDelete20Threads() throws IOException { 	
    	twoReadsOneWriteOneDelete();
    }

    @AfterClass
    public static void checkBuffer() {
		logger.debug(cache.toString());
   }

}