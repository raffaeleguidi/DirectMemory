package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directcache.ICacheEntry;
import org.directcache.impl.DirectCache2;
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
    
	static DirectCache2 cache = null;
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
		cache = new DirectCache2(cacheSize);
//		cache.setDefaultDuration(1000);
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
	
	long lastOne = 0;
	
	public DummyObject nextObject() {
    	String key = "key"+lastOne++;
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize];
		return dummy;
	}
	
    @Test
    //@Required(max = 50)
    @Required(max = 300) // well under 50ms in sun jvm
    public void firstAndLargestItem() { 	
    	DummyObject firstObject = new DummyObject("key0", objectsSize*5);
    	cache.storeObject(firstObject.getName(), firstObject);
    	assertEquals(cache.entries().size(), 1);
    	DummyObject retrievedObject = (DummyObject)cache.retrieveObject("key0");
    	assertEquals(firstObject.getName(), retrievedObject.getName());
		logger.debug(cache.toString());
    }
    
	private void doSomeReads(int howMany) {
		for (int i = 0; i < howMany; i++) {
    		@SuppressWarnings("unused")
			DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
			
		}
	}
	@Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 1)
	public void onlyWrites() {
    	DummyObject object2add = nextObject();
    	ICacheEntry entry = cache.storeObject(object2add.getName(), object2add);
    	assertNotNull(entry);
	}

	@Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1500, average = 3)
	public void onlyWrites10Threads() throws Exception {
    	onlyWrites();		
	}

	@Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 6)
	public void onlyWrites20Threads() {
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
				DummyObject dummy = (DummyObject) cache.retrieveObject(entry.getKey());
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
		DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 750, average = 0.5)
    public void onlyReads10Threads() { 	
    	onlyReads();
    }
    
    @Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 750, average = 0.5)
    public void onlyReads20Threads() { 	
    	onlyReads();
    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 6)
    public void twentyReadsOneWriteOneDelete() { 	
    	doSomeReads(20);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }

	@Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1500, average = 12)
    public void twentyReadsOneWriteOneDelete10Threads() { 	
		twentyReadsOneWriteOneDelete();
	}
	
	@Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 24)
    public void twentyReadsOneWriteOneDelete20Threads() { 	
		twentyReadsOneWriteOneDelete();
	}

    @Test
    public void testAllOnceAgain() {
    	testAll();
    }

    @Test
    @PerfTest(duration = 10000, threads = 5)
    @Required(max = 1500, average = 4)
    public void twoReadsOneWriteOneDelete() { 	
    	doSomeReads(2);
    	DummyObject object2add = randomObject();
    	cache.storeObject(object2add.getName(), object2add);
    	cache.removeObject(randomKey());
    }

    @Test
    @PerfTest(duration = 10000, threads = 10)
    @Required(max = 1500, average = 8)
    public void twoReadsOneWriteOneDelete10Threads() { 	
    	twoReadsOneWriteOneDelete();
    }

    @Test
    @PerfTest(duration = 10000, threads = 20)
    @Required(max = 1500, average = 5)
    public void twoReadsOneWriteOneDelete20Threads() { 	
    	twoReadsOneWriteOneDelete();
    }

    @AfterClass
    public static void checkBuffer() {
		logger.debug(cache.toString());
   }

}