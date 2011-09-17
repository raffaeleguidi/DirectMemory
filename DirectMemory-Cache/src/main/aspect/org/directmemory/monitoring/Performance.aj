package org.directmemory.monitoring;

import org.directmemory.cache.Cache;
import org.directmemory.measures.Monitor;
import org.directmemory.memory.Pointer;
import org.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect Performance {
	public static final String cache_prefix = "cache";

	public static final String cache_putByteArray = cache_prefix + ".putByteArray"; 
	public static final String cache_retrieveByteArray = cache_prefix + ".retrieveByteArray"; 
	public static final String cache_getPointer = cache_prefix + ".getPointer"; 

	public static final String cache_putObject = cache_prefix + ".put"; 
	public static final String cache_retrieveObject = cache_prefix + ".get"; 

	public static final String cache_collectLFU = cache_prefix + ".collectLFU"; 
	public static final String cache_collectExpired = cache_prefix + ".collectExpired"; 
	
	public static final String cache_serialize = cache_prefix + ".serializer.serialize"; 
	public static final String cache_deserialize = cache_prefix + ".serializer.deserialize"; 

	private static Logger logger = LoggerFactory.getLogger(Cache.class);
	
	pointcut putByteArrayPointcut(String key, byte[] payload) : 
		execution(Pointer org.directmemory.cache.Cache.putByteArray(String, byte[])) && 
		args(key, payload);
	
	pointcut putObjectPointcut(String key, Object object, int expiresIn) : 
		execution(Pointer org.directmemory.cache.Cache.put(String, Object, int)) && 
		args(key, object, expiresIn);
	
	pointcut retrieveByteArrayPointcut(String key) : 
		execution(byte[] org.directmemory.cache.Cache.retrieveByteArray(String)) && 
		args(key);
	
	pointcut retrieveObjectPointcut(String key) : 
		execution(Object org.directmemory.cache.Cache.retrieve(String)) && 
		args(key);
	
	pointcut getPointcut(String key) : 
		execution(Pointer org.directmemory.cache.Cache.getPointer(String)) && 
		args(key);
		
	pointcut collectLFUPointcut() : 
		execution(void org.directmemory.cache.Cache.collectLFU()); 
		
	pointcut collectExpiredPointcut() : 
		execution(void org.directmemory.cache.Cache.collectExpired()); 
		
	pointcut serializePointcut(Object obj, @SuppressWarnings("rawtypes") Class clazz) : 
		execution(byte[] org.directmemory.serialization.ProtoStuffSerializerV1.serialize(Object, Class)) && 
		args(obj, clazz); 
		
	pointcut deserializePointcut(byte[] source, @SuppressWarnings("rawtypes") Class clazz) : 
		execution(Object org.directmemory.serialization.ProtoStuffSerializerV1.deserialize(byte[], Class)) && 
		args(source, clazz); 
		
	Pointer around(String key, byte[] payload): putByteArrayPointcut(key, payload) {
		Monitor mon = Monitor.get(cache_putByteArray);
		final long startedAt = mon.start();
		Pointer entry = proceed(key, payload);
		if (logger.isDebugEnabled()) logger.debug(Format.it("put: [%s] %d bytes", key, payload.length ));
		mon.stop(startedAt);
		return entry;
    }
	
	Pointer around(String key, Object object, int expiresIn): putObjectPointcut(key, object, expiresIn) {
		Monitor mon = Monitor.get(cache_putObject);
		final long startedAt = mon.start();
		Pointer entry = proceed(key, object, expiresIn);
		if (logger.isDebugEnabled()) logger.debug(Format.it("put object: [%s]", key));
		mon.stop(startedAt);
		return entry;
    }
	
	byte[] around(String key): retrieveByteArrayPointcut(key) {
		Monitor mon = Monitor.get(cache_retrieveByteArray);
		final long startedAt = mon.start();
		byte[] payload = proceed(key);
		if (logger.isDebugEnabled()) logger.debug(Format.it("retrieve: [%s]", key ));
		mon.stop(startedAt);
		return payload;
	}
	
	Object around(String key): retrieveObjectPointcut(key) {
		Monitor mon = Monitor.get(cache_retrieveObject);
		final long startedAt = mon.start();
		Object payload = proceed(key);
		if (logger.isDebugEnabled()) logger.debug(Format.it("retrieve object: [%s]", key ));
		mon.stop(startedAt);
		return payload;
	}
	
	Pointer around(String key): getPointcut(key) {
		Monitor mon = Monitor.get(cache_getPointer);
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
	
	byte[] around(Object obj, @SuppressWarnings("rawtypes") Class clazz): serializePointcut(obj, clazz) {
		Monitor mon = Monitor.get(cache_serialize);
		final long startedAt = mon.start();
		byte[] payload = proceed(obj, clazz);
		if (logger.isDebugEnabled()) logger.debug(Format.it("serialize: [%s]", clazz.getSimpleName() ));
		mon.stop(startedAt);
		return payload;
	}
	
	Object around(byte[] source, @SuppressWarnings("rawtypes") Class clazz): deserializePointcut(source, clazz) {
		Monitor mon = Monitor.get(cache_deserialize);
		final long startedAt = mon.start();
		Object obj = proceed(source, clazz);
		if (logger.isDebugEnabled()) logger.debug(Format.it("deserialize: [%s]", clazz.getSimpleName() ));
		mon.stop(startedAt);
		return obj;
	}
	
}
