package org.directcache.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
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
	static int objectsToStore = 10000;
	static int objectsSize = 20000;
	static int mb2use = 400;
	static Random generator = new Random();
	
	@Test
	public void allocateMemory() {
		cache = new DirectCache(1024*1024*mb2use);
		System.out.println("allocated " + mb2use + " mb");
	}
	
	@Test
	public void storeObjects() throws Exception {
		System.out.print("storing some objects... ");
		for (int i = 0; i < objectsToStore; i++) {
			DummyObject p = new DummyObject("test #" + i);
			p.obj = new Object[objectsSize*generator.nextInt(5)];
			CacheDescriptor desc = cache.storeObject(p.getName(), p);
			size += desc.getSize();
			objects++;
			//System.out.println("adding object of size " + desc.getSize());
		}
		System.out.println("done");
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
		assertEquals (cache.capacity(), mb2use*1024*1024);
		assertEquals (cache.size(), size);
		assertEquals (cache.remaining(), cache.capacity() - size);		
	}
	
	@Test
	public void objectsAreThere() throws IOException, ClassNotFoundException {
		Iterator<CacheDescriptor> index = cache.getIndex().values().iterator();
		while (index.hasNext()) {
			CacheDescriptor desc = index.next();
			DummyObject p = (DummyObject)cache.retrieveObject(desc.getKey());
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
		for (int i = 0; i < objectsToStore/2; i++) {
			DummyObject p = new DummyObject("obj #" + i);
			p.obj = new Object[objectsSize*generator.nextInt(5)];
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
