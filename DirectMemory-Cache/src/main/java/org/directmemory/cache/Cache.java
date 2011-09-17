package org.directmemory.cache;

import java.io.EOFException;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;

import org.directmemory.measures.Every;
import org.directmemory.measures.Ram;
import org.directmemory.memory.MemoryManager;
import org.directmemory.memory.OffHeapMemoryBuffer;
import org.directmemory.memory.Pointer;
import org.directmemory.misc.Format;
import org.directmemory.serialization.ProtoStuffSerializerV1;
import org.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

public class Cache {

	private static Logger logger = LoggerFactory.getLogger(MemoryManager.class);
	private static ConcurrentMap<String, Pointer> map;
	
	public static int DEFAULT_CONCURRENCY_LEVEL = 4;
	public static int DEFAULT_INITIAL_CAPACITY = 100000;
	
	public static Serializer serializer = new ProtoStuffSerializerV1();

	private Cache() {
		// not instantiable
	}
	
	
	private final static Timer timer = new Timer();

    public static void scheduleDisposalEvery(long l) {
        timer.schedule(new TimerTask() {
            public void run() {
				 logger.info("begin scheduled disposal");
				 collectExpired();
				 collectLFU();
				 logger.info("scheduled disposal complete");
            }
        }, l);
        logger.info("disposal scheduled every " + l + " milliseconds");
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
		scheduleDisposalEvery(Every.seconds(10));
	}

	public static void init(int numberOfBuffers, int size) {
		init(numberOfBuffers, size, DEFAULT_INITIAL_CAPACITY, DEFAULT_CONCURRENCY_LEVEL);
	}

	public static Pointer putByteArray(String key, byte[] payload, int expiresIn) {
		Pointer ptr = MemoryManager.store(payload, expiresIn);
		map.put(key, ptr);
  		return ptr;
	}
	
	public static Pointer putByteArray(String key, byte[] payload) {
  		return putByteArray(key, payload, 0);
	}
	
	public static Pointer put(String key, Object object) {
		return put(key, object, 0);
	}
	
	public static Pointer put(String key, Object object, int expiresIn) {
		try {
			byte[] payload = serializer.serialize(object, object.getClass());
			Pointer ptr = putByteArray(key, payload);
			ptr.clazz = object.getClass();
			return ptr; 
		} catch (IOException e) {
			logger.error(e.getMessage());
			return null;
		}
	}
	
	public static Pointer updateByteArray(String key, byte[] payload) {
		Pointer p = map.get(key);
		p = MemoryManager.update(p, payload);
  		return p;
	}
	
	public static Pointer update(String key, Object object) {
		Pointer p = map.get(key);
		try {
			p = MemoryManager.update(p, serializer.serialize(object, object.getClass()));
			p.clazz = object.getClass();
	  		return p;
		} catch (IOException e) {
			logger.error(e.getMessage());
			return null;
		}
	}
	
	public static byte[] retrieveByteArray(String key) {
		Pointer ptr = getPointer(key);
		if (ptr == null) return null;
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
	
	public static Object retrieve(String key) {
		Pointer ptr = getPointer(key);
		if (ptr == null) return null;
		if (ptr.expired() || ptr.free) {
			map.remove(key);
			if (!ptr.free) { 
				MemoryManager.free(ptr);
			}
			return null;
		} else {
	  		try {
				return serializer.deserialize(MemoryManager.retrieve(ptr),ptr.clazz);
			} catch (EOFException e) {
				logger.error(e.getMessage());
			} catch (IOException e) {
				logger.error(e.getMessage());
			} catch (ClassNotFoundException e) {
				logger.error(e.getMessage());
			} catch (InstantiationException e) {
				logger.error(e.getMessage());
			} catch (IllegalAccessException e) {
				logger.error(e.getMessage());
			}
		}
		return null;
	}
	
	public static Pointer getPointer(String key) {
  		return map.get(key);
	}
	
	public static void free(String key) {
		Pointer p = map.remove(key);
		if (p != null) {
			MemoryManager.free(p);
		}
	}
	
	public static void free(Pointer pointer) {
		MemoryManager.free(pointer);
	}
	
	public static void collectExpired() {
		MemoryManager.collectExpired();
		// still have to look for orphan (storing references to freed pointers) map entries
	}
	
	public static void collectLFU() {
		MemoryManager.collectLFU();
		// can possibly clear one whole buffer if it's too fragmented - investigate
	}
	
	public static void collectAll() {
		 Thread thread = new Thread(){
			 public void run(){
				 logger.info("begin disposal");
				 collectExpired();
				 collectLFU();
				 logger.info("disposal complete");
			 }
		 };
		 thread.start();
	}
	
	
	public static void clear() {
		map.clear();
		MemoryManager.clear();
		logger.info("Cache cleared");
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
