package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.log.EmptyExecutionLogger;
import org.directcache.DirectCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;


public class LoadTest {
	
    @Rule
    public ContiPerfRule i = new ContiPerfRule(new EmptyExecutionLogger());
    
	static DirectCache cache = null;
	static int size = 0;
	static int objects = 0;
	static int objectsToStore = 45000;
	static int objectsSize = 2500;
	static int mb2use = 100;
	static Random generator = new Random();
	
	private static Logger logger=LoggerFactory.getLogger("org.directcache.test");	
	
	public static void allocateMemory() {
		cache = new DirectCache(mb2use);
		cache.setDefaultDuration(1);
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
		DummyObject dummy = new DummyObject(key);
		dummy.PayLoad = new byte[objectsSize];
		return dummy;
	}
	
	@Test
    //@Required(average = 10F)
    public void FillUpCache() throws Exception { 	
		logger.warn("entering fillup cache");
		int n=0;
		while (cache.remaining() > objectsSize) {
	    	DummyObject obj = randomObject();
	    	obj.setName("key"+n);
	    	String key = obj.getName();
	    	cache.storeObject(key, obj);
	    	n++;
		}
		objects = cache.entries().size();
		logger.warn("exiting fillup cache with remaining=" + cache.remaining());
	}

	@Test
    @PerfTest(duration = 3000, threads = 10)
    @Required(max = 900, average = 5F)
    public void fourReadsOneWriteForSomeMinutes() throws Exception { 	
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
