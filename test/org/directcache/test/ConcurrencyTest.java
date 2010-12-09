package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Random;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directcache.DirectCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;


public class ConcurrencyTest {
	
    @Rule
    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
	static DirectCache cache = null;
	static int size = 0;
	static int objects = 0;
	static int objectsToStore = 4500;
	static int objectsSize = 2500;
	static int mb2use = 100;
	static Random generator = new Random();
	
	public static void allocateMemory() {
		cache = new DirectCache(mb2use);
		System.out.println("allocated " + mb2use + " mb");
	}
	
	public static void sizeCountAndCapacityAreOk() {

		System.out.println(cache.toString());

		assertEquals (cache.entries().size(), objects);
		assertEquals (cache.capacity(), mb2use*1024*1024);
		assertEquals (cache.usedMemory(), size);
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
    @PerfTest(duration = 5000, threads = 50)
    //@Required(average = 10F)
    public void insertItem() throws Exception { 	
    	DummyObject obj = randomObject();
    	String key = obj.getName();
    	cache.storeObject(key, obj);
    	DummyObject retrievedObj = (DummyObject)cache.retrieveObject(key);
    	assertNotNull(retrievedObj);
    	assertEquals(obj.getName(), retrievedObj.getName());
    	assertEquals(obj.obj.length, retrievedObj.obj.length);
    }

    @Test
    @PerfTest(invocations = 10000, threads = 1)
    @Required(average = 0.3F)
    public void overWriteItem() throws Exception { 	
    	DummyObject obj = new DummyObject("test", generator.nextInt(objectsSize));
    	String key = obj.getName();
    	cache.storeObject(key, obj);
    	DummyObject retrievedObj = (DummyObject)cache.retrieveObject(key);
    	assertEquals(obj.getName(), retrievedObj.getName());
    	assert(obj.obj.length != retrievedObj.obj.length);
    }

    @Test
    @PerfTest(invocations = 20000, threads = 100)
    //@Required(average = 0.9F)
    public void removeItem() throws Exception { 	
    	DummyObject obj = randomObject();
    	String key = obj.getName();
    	cache.storeObject(key, obj);
    	cache.removeObject(key);
    	DummyObject retrievedObj = (DummyObject)cache.retrieveObject(key);
    	assertNull(retrievedObj);
    }

    @AfterClass
    public static void checkBuffer() {
		System.out.println();
		System.out.println(cache.toString());
		cache = null;
    }

}
