package org.directmemory.monitoring;

import org.directmemory.cache.Cache;
import org.directmemory.measures.Monitor;
import org.directmemory.memory.Pointer;
import org.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect Performance {
	public static final String cache_prefix = "cache";
	public static final String cache_put = cache_prefix + ".put"; 
	public static final String cache_retrieve = cache_prefix + ".retrieve"; 
	public static final String cache_get = cache_prefix + ".get"; 
	public static final String cache_collectLFU = cache_prefix + ".collectLFU"; 
	public static final String cache_collectExpired = cache_prefix + ".collectExpired"; 
	
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
		
	pointcut collectLFUPointcut() : 
		execution(void org.directmemory.cache.Cache.collectLFU()); 
		
	pointcut collectExpiredPointcut() : 
		execution(void org.directmemory.cache.Cache.collectExpired()); 
		
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
		if (logger.isDebugEnabled()) logger.debug(Format.it("retrieve: [%s]", key ));
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
	
	void around(): collectLFUPointcut() {
		Monitor mon = Monitor.get(cache_collectLFU);
		final long startedAt = mon.start();
		proceed();
		if (logger.isDebugEnabled()) logger.debug(Format.it("collect LFU"));
		mon.stop(startedAt);
	}
	
	void around(): collectExpiredPointcut() {
		Monitor mon = Monitor.get(cache_collectExpired);
		final long startedAt = mon.start();
		proceed();
		if (logger.isDebugEnabled()) logger.debug(Format.it("collect expired"));
		mon.stop(startedAt);
	}
	
}
