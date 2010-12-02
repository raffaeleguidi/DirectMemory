package org.directcache.test;

import org.directcache.CacheDescriptor;
import org.directcache.DirectCache;
import org.junit.Test;


public class BaseTest {

	@Test
	public void PutTest() {
		DirectCache cache = new DirectCache(1024*1024*50);

		int size = 0;
		int objects = 0;

		for (int i = 0; i < 500; i++) {
			TestClass p = new TestClass("test #" + i);
			p.obj = new Object[i*i];
			try {
				CacheDescriptor desc = cache.storeObject(p.getName(), p);
				size += desc.getSize();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			objects++;
		}
		assert (cache.getIndex().size() == objects);
		assert (cache.capacity() == 52428800);
		assert (cache.size() == size);
		assert (cache.remaining() == cache.capacity() - size);

		System.out.println("items in cache: " + cache.getIndex().size());
		System.out.println("capacity: " + cache.capacity());
		System.out.println("size: " + cache.size());
		System.out.println("remaining: " + cache.remaining());
			
	}
}
