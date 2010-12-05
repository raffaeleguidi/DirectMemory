package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directcache.CacheEntry;
import org.directcache.DirectCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;


public class PerformanceTest {
	
    @Rule
    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
	static DirectCache cache = null;
	static int size = 0;
	static int objects = 0;
	static int objectsToStore = 45000;
	static int objectsSize = 2500;
	static int mb2use = 400;
	static Random generator = new Random();
	
	public static void allocateMemory() {
		cache = new DirectCache(mb2use);
		System.out.println("allocated " + mb2use + " mb");
	}
	
	public static void storeObjects() throws Exception {
		System.out.print("storing some objects... ");
		for (int i = 1; i < objectsToStore; i++) {
			DummyObject p = new DummyObject("key" + i);
			p.obj = new Object[objectsSize*generator.nextInt(5)];
			CacheEntry desc = cache.storeObject(p.getName(), p);
			size += desc.getSize();
			objects++;
		}
		System.out.println("done");
		System.out.println();
	}
	
	public static void sizeCountAndCapacityAreOk() {

		System.out.println(cache.toString());

		assertEquals (cache.getAllocationTable().size(), objects);
		assertEquals (cache.capacity(), mb2use*1024*1024);
		assertEquals (cache.size(), size);
		assertEquals (cache.remaining(), cache.capacity() - size);		
	}
	
	@BeforeClass
	public static void setup() throws Exception {
		allocateMemory();
	}

	private String randomKey() {
		return "key"+generator.nextInt(objectsToStore);
	}

	public DummyObject randomObject() {
    	String key = randomKey();
		DummyObject dummy = new DummyObject(key, objectsSize*generator.nextInt(5));
		return dummy;
	}

    @Test
    @Required(max = 30)
    public void firstAndLargestItem() throws Exception { 	
    	DummyObject firstObject = new DummyObject("key0", objectsSize);
    	cache.storeObject(firstObject.getName(), firstObject);
		System.out.println("cache size is " + cache.size() + " bytes");
		System.out.println();
		System.out.println(cache.toString());
    }
    
    @Test
    @Required(max = 10000)
    public void fillUpCache() throws Exception {
    	storeObjects();
    }

    @Test
    @PerfTest(invocations = 10000, threads = 1)
    @Required(max = 40, average = 0.2F)
    public void oneReadOneWrite() throws Exception { 	
    	try {
        	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	DummyObject object2add = randomObject();
        	cache.removeObject(object2add.getName());
        	cache.storeObject(object2add.getName(), object2add);
        	assertNotNull(randomPick.getName());
		} catch (Exception e) {
			// TODO: handle exception
		}
    }
    
    @Test
    @PerfTest(invocations = 10000, threads = 1)
    @Required(max = 35, average = 0.4F)
    public void twoReadsOneWrite() throws Exception { 	
    	try {
        	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	DummyObject object2add = randomObject();
        	cache.removeObject(object2add.getName());
        	cache.storeObject(object2add.getName(), object2add);
        	assertNotNull(randomPick.getName());
		} catch (Exception e) {
			// TODO: handle exception
		}
    }
    
    @Test
    @PerfTest(invocations = 10000, threads = 1)
    @Required(max = 90, average = 1.5F)
    public void fourReadsOneWrite() throws Exception { 	
    	try {
        	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	DummyObject object2add = randomObject();
        	cache.removeObject(object2add.getName());
        	cache.storeObject(object2add.getName(), object2add);
        	assertNotNull(randomPick.getName());
		} catch (Exception e) {
			// TODO: handle exception
		}
    }
    
    @Test
    @PerfTest(invocations = 10000, threads = 1)
    @Required(max = 30, average = 0.006F)
    public void onlyRead() throws Exception { 	
    	try {
        	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	assertNotNull(randomPick.getName());
		} catch (Exception e) {
			// TODO: handle exception
		}
    }

    @Test
    @PerfTest(duration = 180000, threads = 20)
    @Required(max = 500, average = 2.5F)
    public void fourReadsOneWriteFor3Minutes() throws Exception { 	
    	try {
        	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	randomPick = (DummyObject)cache.retrieveObject(randomKey());
        	DummyObject object2add = randomObject();
        	cache.removeObject(object2add.getName());
        	cache.storeObject(object2add.getName(), object2add);
        	assertNotNull(randomPick.getName());
		} catch (Exception e) {
			// TODO: handle exception
		}
    }
    
    @AfterClass
    public static void checkBuffer() {
		System.out.println();
		System.out.println(cache.toString());
		cache = null;
   }

}
