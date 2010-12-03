package org.directcache.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.directcache.CacheDescriptor;
import org.directcache.DirectCache;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class BaseTest {
	
	static DirectCache cache = null;
	static int size = 0;
	static int objects = 0;
	static int objectsToStore = 500;
	static int objectsSize = 100000;

	@BeforeClass
	public static void setup() {
		cache = new DirectCache(1024*1024*50);
	}
	
	@Test
	public void storeObjects() throws Exception {
		for (int i = 0; i < objectsToStore; i++) {
			TestClass p = new TestClass("test #" + i);
			p.obj = new Object[objectsSize];
			CacheDescriptor desc = cache.storeObject(p.getName(), p);
			size += desc.getSize();
			objects++;
			//System.out.println("adding object of size " + desc.getSize());
		}
		System.out.println("stored some objects");
	}

	@Test
	public void sizeCountAndCapacityAreOk() {
		System.out.println("counting...");
		System.out.println("-----------------------------");
		System.out.println("items in cache: " + cache.getIndex().size());
		System.out.println("capacity (mb): " + cache.capacity()/1024/1024);
		System.out.println("size (mb): " + cache.size()/1024/1024);
		System.out.println("remaining (mb): " + cache.remaining()/1024/1024);
		System.out.println("-----------------------------");

		assertEquals (cache.getIndex().size(), objects);
		assertEquals (cache.capacity(), 52428800);
		assertEquals (cache.size(), size);
		assertEquals (cache.remaining(), cache.capacity() - size);		
	}
	
	@Test
	public void objectsAreThere() throws IOException, ClassNotFoundException {
		Iterator<CacheDescriptor> index = cache.getIndex().values().iterator();
		while (index.hasNext()) {
			CacheDescriptor desc = index.next();
			TestClass p = (TestClass)cache.retrieveObject(desc.getKey());
			assertNotNull(p);
			assertEquals(desc.getKey(), p.getName());
		}		
		System.out.println("checked " + objects + " objects");
	}

	@Test
	public void removeAllObjects() {
		Iterator<CacheDescriptor> index = cache.getIndex().values().iterator();
		Vector<CacheDescriptor> temp = new Vector<CacheDescriptor>();
		while (index.hasNext()) temp.add(index.next());
		for (CacheDescriptor desc : temp) {
			CacheDescriptor trashed = cache.removeObject(desc.getKey());
			assertNotNull(trashed);
			assertEquals(trashed.getKey(), desc.getKey());
			size -= desc.getSize();
			objects--;
		}		
		System.out.println("removed all objects");
	}

	@Test
	public void sizeCountAndCapacityAreStillOk() {
		System.out.println("counting again");
		sizeCountAndCapacityAreOk();
	}
	
	@Test
	public void storeSomeObjectsAgain() throws Exception {
		System.out.println("going to add again");
		for (int i = 0; i < 500; i++) {
			TestClass p = new TestClass("obj #" + i);
			p.obj = new Object[objectsSize];
			CacheDescriptor desc = cache.storeObject(p.getName(), p);
			assertNotNull(desc);
			size += desc.getSize();
			//System.out.println("adding object of size " + desc.getSize());
			objects++;
		}
		System.out.println("stored some objects again");
	}

	@Test
	public void objectsAreAgainThere() throws IOException, ClassNotFoundException {
		objectsAreThere();
	}

	@Test
	public void sizeCountAndCapacityAreOkAgain() {
		sizeCountAndCapacityAreOk();
	}
	
}
