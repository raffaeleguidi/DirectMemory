package org.directmemory.monitoring;

import org.directmemory.cache.Cache;
import org.directmemory.measures.Monitor;
import org.directmemory.memory.Pointer;
import org.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect Performance {
	public static final String cache_put = "cache.put"; 
	public static final String cache_retrieve = "cache.retrieve"; 
	public static final String cache_get = "cache.get"; 
	
	private static Logger logger = LoggerFactory.getLogger(Cache.class);
	
	pointcut putPointcut(String key, byte[] payload) : 
		execution(Pointer org.directmemory.cache.Cache.put(String, byte[])) && 
		args(key, payload);
	
	pointcut retrievePointcut(String key) : 
		execution(byte[] org.directmemory.cache.Cache.retrieve(String)) && 
		args(key);
	
	pointcut getPointcut(String key) : 
		execution(Pointer org.directmemory.cache.Cache.get(String)) && 
		args(key);
		
	Pointer around(String key, byte[] payload): putPointcut(key, payload) {
		Monitor mon = Monitor.get(cache_put);
		final long startedAt = mon.start();
		Pointer entry = proceed(key, payload);
		if (logger.isDebugEnabled()) logger.debug(Format.it("put: [%s] %d bytes", key, payload.length ));
		mon.stop(startedAt);
		return entry;
    }
	
	byte[] around(String key): retrievePointcut(key) {
		Monitor mon = Monitor.get(cache_retrieve);
		final long startedAt = mon.start();
		byte[] payload = proceed(key);
		if (logger.isDebugEnabled()) logger.debug(Format.it("retrieve: [%s] %d bytes", key, payload.length ));
		mon.stop(startedAt);
		return payload;
	}
	
	Pointer around(String key): getPointcut(key) {
		Monitor mon = Monitor.get(cache_get);
		final long startedAt = mon.start();
		Pointer pointer = proceed(key);
		if (logger.isDebugEnabled()) logger.debug(Format.it("get: [%s]", key));
		mon.stop(startedAt);
		return pointer;
	}
	
	pointcut getMeasures() : 
		execution(void org.directmemory.cache.Cache.dump());

	after() : getMeasures() {
		for (Monitor monitor : Monitor.monitors.values()) {
			logger.info(monitor.toString());
		}
	}

}
