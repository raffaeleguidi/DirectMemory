package org.directmemory.memory;

import java.util.concurrent.ConcurrentMap;

import org.directmemory.measures.Ram;
import org.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

public class Cache {

	private static Logger logger = LoggerFactory.getLogger(MemoryManager.class);
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

		logger.info("*** initializing *******************************\r\n" + Format.logo());
		logger.info("************************************************");
		MemoryManager.init(numberOfBuffers, size);
		logger.info("initialized");
		logger.info(Format.it("number of buffer(s): \t%1d  with %2s each", numberOfBuffers, Ram.inMb(size)));
		logger.info(Format.it("initial capacity: \t%1d", initialCapacity));
		logger.info(Format.it("concurrency level: \t%1d", concurrencyLevel));
	}

	public static void init(int numberOfBuffers, int size) {
		init(numberOfBuffers, size, DEFAULT_INITIAL_CAPACITY, DEFAULT_CONCURRENCY_LEVEL);
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

	private static void dump(OffHeapMemoryBuffer mem) {
		logger.info(Format.it("off-heap - buffer: \t%1d", mem.bufferNumber));
		logger.info(Format.it("off-heap - allocated: \t%1s", Ram.inMb(mem.capacity())));
		logger.info(Format.it("off-heap - used:      \t%1s", Ram.inMb(mem.used())));
		logger.info(Format.it("heap 	- max: \t%1s", Ram.inMb(Runtime.getRuntime().maxMemory())));
		logger.info(Format.it("heap     - allocated: \t%1s", Ram.inMb(Runtime.getRuntime().totalMemory())));
		logger.info(Format.it("heap     - free : \t%1s", Ram.inMb(Runtime.getRuntime().freeMemory())));
		logger.info("************************************************");
	}
	
	public static void dump() {
		if (!logger.isInfoEnabled())
			return;
		
		logger.info("*** DirectMemory statistics ********************");
		
		for (OffHeapMemoryBuffer mem : MemoryManager.buffers) {
			dump(mem);
		}
	}

}
