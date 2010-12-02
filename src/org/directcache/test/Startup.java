package org.directcache.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.instrument.Instrumentation;
import java.nio.*;

import org.directcache.CacheDescriptor;
import org.directcache.DirectCache;


public class Startup {


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		DirectCache cache = new DirectCache();

		for (int i = 0; i < 500; i++) {
			TestClass p = new TestClass("test #" + i);
			p.obj = new Object[i*i];
			CacheDescriptor desc = cache.storeObject(p.getName(), p);
		}
		
		System.out.println("item in cache: " + cache.getIndex().size());
		System.out.println("capacity: " + cache.capacity());
		System.out.println("size: " + cache.size());
		System.out.println("remaining: " + cache.remaining());
		

		for (int i = 0; i < 500; i++) {
			TestClass p = new TestClass("test #" + i);
			p.obj = new Object[i*i];
			cache.removeObject(p.getName());
		}
		
		System.out.println("item in cache: " + cache.getIndex().size());
		System.out.println("capacity: " + cache.capacity());
		System.out.println("size: " + cache.size());
		System.out.println("remaining: " + cache.remaining());
		
		for (int i = 301; i < 400; i++) {
			TestClass p = new TestClass("test #" + i);
			p.obj = new Object[i*i];
			CacheDescriptor desc = cache.storeObject(p.getName(), p);
		}

		System.out.println("item in cache: " + cache.getIndex().size());
		System.out.println("capacity: " + cache.capacity());
		System.out.println("size: " + cache.size());
		System.out.println("remaining: " + cache.remaining());
		
		for (int i = 0; i < 500; i++) {
			TestClass p = (TestClass) cache.retrieveObject("test #" + i);
			if (!p.getName().equals("test #" + i))
				System.out.println("attenzione: mi aspettavo test #" + i + " e trovo " + p.getName());
		}

		System.out.println("controllo corrispondenza ok");		

	}

}
