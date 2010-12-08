package org.directcache.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;

import org.directcache.CacheEntry;
import org.directcache.DirectCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class BaseTest {
	
	static DirectCache cache = null;
	static int size = 0;
	static int objects = 0;
	static int objectsToStore = 10000;
	static int objectsSize = 2500;
	static int mb2use = 100;
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
    public void firstAndLargestItem() throws Exception { 	
    	DummyObject firstObject = new DummyObject("key0", objectsSize);
    	cache.storeObject(firstObject.getName(), firstObject);
		System.out.println("cache size is " + cache.size() + " bytes");
		System.out.println();
		System.out.println(cache.toString());
    }
    
    @Test
    public void fillUpCache() throws Exception {
    	storeObjects();
    }

    @Test
    public void oneReadOneWrite() throws Exception { 	
    	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	DummyObject object2add = randomObject();
    	cache.removeObject(object2add.getName());
    	cache.storeObject(object2add.getName(), object2add);
    	assertNotNull(randomPick.getName());
    }
    
    @Test
    public void twoReadsOneWrite() throws Exception { 	
    	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	DummyObject object2add = randomObject();
    	cache.removeObject(object2add.getName());
    	cache.storeObject(object2add.getName(), object2add);
    	assertNotNull(randomPick.getName());
    }
    
    @Test
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
    public void onlyRead() throws Exception { 	
    	DummyObject randomPick = (DummyObject)cache.retrieveObject(randomKey());
    	assertNotNull(randomPick.getName());
    }
    
    @AfterClass
    public static void checkBuffer() {
		System.out.println();
		System.out.println(cache.toString());
		cache = null;
   }

}
