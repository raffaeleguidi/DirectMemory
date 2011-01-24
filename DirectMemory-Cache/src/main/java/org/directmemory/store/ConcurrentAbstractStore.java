package org.directmemory.store;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheEntry2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class ConcurrentAbstractStore extends ConcurrentSkipListMap<String, CacheEntry2> {

	protected static Logger logger=LoggerFactory.getLogger(ConcurrentAbstractStore.class);
	/**
	 * 
	 */
	public AtomicLong limit = new AtomicLong(-1);
	public AtomicLong size = new AtomicLong(0);
	private static final long serialVersionUID = 1L;
	
	abstract String storeName();
	
	public ConcurrentAbstractStore nextStore;
	public ConcurrentAbstractStore topStore = this;
	
	protected void touch(CacheEntry2 entry) {
		entry.lastAccessed = System.currentTimeMillis();
		entry.reads.incrementAndGet();
	}	
	
	protected void reset(CacheEntry2 entry) {
		entry.created = System.currentTimeMillis();
		entry.lastAccessed = System.currentTimeMillis();
		entry.reads.set(0l);
	}	
	
	@Override
	public CacheEntry2 put(String key, CacheEntry2 entry) {
		size.incrementAndGet();
		checkMapSize();
		popIn(entry);
		reset(entry);
		return super.put(entry.key, entry);
	}
	
	private void checkMapSize() {
		if (limit.get() == -1) return;
		while (size.get() > limit.get() ) {
			Entry<String, CacheEntry2> eldest = this.pollFirstEntry();
			size.decrementAndGet();
			if (nextStore != null) {
				nextStore.put(eldest.getKey(), eldest.getValue());
			}
		}
	}

	@Override
	public CacheEntry2 get(Object key) {
		CacheEntry2 entry =  super.get(key);
		if (entry != null) {
			if (entry.expired()) {
				super.remove(entry.key);
				size.decrementAndGet();
				return null;
			}
			touch(entry);
			if (topStore != this) {
				super.remove(entry.key);
				size.decrementAndGet();
				popOut(entry);
				topStore.put(entry.key, entry);
//				;
//				size.decrementAndGet();
//				popOut(entry);
//				topStore.put(entry.key, super.remove(entry.key));
			}
		} else {
			if (nextStore != null) {
				entry = nextStore.get(key);
			}
		}
		return entry;
	}
	
	@Override
	public CacheEntry2 remove(Object key) {
		CacheEntry2 entry =  super.remove(key);
		if (entry != null) {
			if (nextStore != null) {
				entry = nextStore.remove(key);
			}
			size.decrementAndGet();
		}
		return entry;
	}
	
	@Override
	public void putAll(Map<? extends String, ? extends CacheEntry2> m) {
		throw new NotImplementedException();
	}
	
	abstract void popIn(CacheEntry2 entry);
	abstract void popOut(CacheEntry2 entry);
	
	AtomicLong removed = new AtomicLong(0);
	
	abstract byte[] toStream(CacheEntry entry);
	abstract Object toObject(CacheEntry entry);

	public void dispose() {
		clear();
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + ": entries " + size.get() + "/" + limit;
	}
}
