package org.directmemory.memory;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;

public class Cache {

	private static ConcurrentMap<String, Pointer> map;
	
	public static int DEFAULT_CONCURRENCY_LEVEL = 4;
	public static int DEFAULT_INITIAL_CAPACITY = 100000;
	
	private Cache() {
		// not instantiable
	}

	public static void init(int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel) {
		map = new MapMaker()
			.concurrencyLevel(concurrencyLevel)
			.initialCapacity(initialCapacity)
			.makeMap();
		
		MemoryManager.init(numberOfBuffers, size);
	}

	public static void init(int numberOfBuffers, int size) {
		map = new MapMaker()
			.concurrencyLevel(DEFAULT_CONCURRENCY_LEVEL)
			.initialCapacity(DEFAULT_INITIAL_CAPACITY)
			.makeMap();
		
		MemoryManager.init(numberOfBuffers, size);
	}

	public static Pointer put(String key, byte[] payload, int expiresIn) {
  		return map.put(key, MemoryManager.store(payload, expiresIn));
	}
	
	public static Pointer put(String key, byte[] payload) {
  		return put(key, payload, 0);
	}
	
	public static Pointer update(String key, byte[] payload) {
		Pointer p = map.get(key);
		p = MemoryManager.update(p, payload);
  		return p;
	}
	
	public static byte[] retrieve(String key) {
		Pointer ptr = get(key);
		if (ptr.expired() || ptr.free) {
			map.remove(key);
			if (!ptr.free) { 
				MemoryManager.free(ptr);
			}
			return null;
		} else {
	  		return MemoryManager.retrieve(ptr);
		}
	}
	
	public static Pointer get(String key) {
  		return map.get(key);
	}
	
	public static void free(String key) {
		Pointer p = map.remove(key);
		if (p != null) {
			MemoryManager.free(p);
		}
	}
	
	public static void disposeExpired() {
		MemoryManager.disposeExpired();
	}
	
	public static void clear() {
		map.clear();
		MemoryManager.clear();
	}

	public static long entries() {
		return map.size();
	}

}
