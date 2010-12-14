package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directcache.DirectCacheWithSmallBuffers;
import org.directcache.ICacheEntry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PerformanceWithSmallBuffersTest {
	
	private static Logger logger=LoggerFactory.getLogger(PerformanceWithSmallBuffersTest.class);

	@Rule
    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
	static DirectCacheWithSmallBuffers cache = null;
	static int objectsSize = 2048;
	static Random generator = new Random();
	static int cacheSize = 490*1024*1024;
	
	public static void allocateMemory() {
		String mb2useFromCommandLine = System.getProperty("mb2use");
		if (mb2useFromCommandLine != null) {
			logger.debug("using mb2useFromCommandLine=" + mb2useFromCommandLine);
			cacheSize = new Integer(mb2useFromCommandLine)*1024*1024;
			logger.debug("cacheSize=" + cacheSize);
		}
		cache = new DirectCacheWithSmallBuffers(cacheSize);
		cache.setDefaultDuration(1000);
		logger.debug(cache.toString());
	}
		
	@BeforeClass
	public static void setup() throws Exception {
		allocateMemory();
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
	
    @Test
    @Required(max = 50)
    public void firstAndLargestItem() throws Exception { 	
    	DummyObject firstObject = new DummyObject("key0", objectsSize*5);
    	cache.storeObject(firstObject.getName(), firstObject);
    	assertEquals(cache.entries().size(), 1);
    	DummyObject retrievedObject = (DummyObject)cache.retrieveObject("key0");
    	assertEquals(firstObject.getName(), retrievedObject.getName());
		logger.debug(cache.toString());
    }
    
	private void doSomeReads(int howMany) throws IOException, ClassNotFoundException {
		for (int i = 0; i < howMany; i++) {
    		@SuppressWarnings("unused")
			DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
			
		}
	}
	@Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 0.5)
	public void onlyWrites() throws Exception {
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);		
	}

	@Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1500, average = 2)
	public void onlyWrites10Threads() throws Exception {
    	onlyWrites();		
	}

	@Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 4.5)
	public void onlyWrites20Threads() throws Exception {
    	onlyWrites();		
	}

	@Test
    public void testAll() throws IOException, ClassNotFoundException {
		Map<String, ICacheEntry> entries = cache.entries();
		
    	Iterator<ICacheEntry> iter = entries.values().iterator();
		while (iter.hasNext()) {
			ICacheEntry entry = iter.next();
			if (!entry.expired()) {
				DummyObject dummy = (DummyObject) cache.retrieveObject(entry.getKey());
				assertNotNull(dummy);
				assertEquals(entry.getKey(), dummy.getName());
			}
		}

		logger.debug("all objects checked");
		logger.debug(cache.toString());	
    }
	
    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 750, average = 0.1)
    public void onlyReads() throws Exception { 	
    	@SuppressWarnings("unused")
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 750, average = 0.2)
    public void onlyReads10Threads() throws Exception { 	
    	onlyReads();
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 750, average = 0.5)
    public void onlyReads20Threads() throws Exception { 	
    	onlyReads();
    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 1.5)
    public void twentyReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(20);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }

	@Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1500, average = 3)
    public void twentyReadsOneWriteOneDelete10Threads() throws Exception { 	
		twentyReadsOneWriteOneDelete();
	}
	
	@Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 8)
    public void twentyReadsOneWriteOneDelete20Threads() throws Exception { 	
		twentyReadsOneWriteOneDelete();
	}

    @Test
    public void testAllOnceAgain() throws IOException, ClassNotFoundException {
    	testAll();
    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 2)
    public void twoReadsOneWriteOneDelete() throws Exception { 	
    	doSomeReads(2);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }

    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1500, average = 2)
    public void twoReadsOneWriteOneDelete10Threads() throws Exception { 	
    	twoReadsOneWriteOneDelete();
    }

    @Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 2)
    public void twoReadsOneWriteOneDelete20Threads() throws Exception { 	
    	twoReadsOneWriteOneDelete();
    }

    @AfterClass
    public static void checkBuffer() {
		logger.debug(cache.toString());
   }

}