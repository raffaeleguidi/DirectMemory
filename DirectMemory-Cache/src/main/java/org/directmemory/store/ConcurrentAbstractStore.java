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
		popIn(entry);
		checkMapSize();
//		System.out.println("put " + key + " in " + getClass().toString());
		reset(entry);
		return super.put(entry.key, entry);
	}
	
	private synchronized void checkMapSize() {
		Entry<String, CacheEntry2> first = this.firstEntry();
		if (first == null) return;
		if (removeEldestEntry(first)) {
			remove(first.getKey());
		}
	}

	@Override
	public CacheEntry2 get(Object key) {
		CacheEntry2 entry =  super.get(key);
		if (entry != null) {
			if (entry.expired()) {
				remove(key);
				return null;
			}
			touch(entry);
			if (topStore != this) {
				popOut(entry);
//				System.out.println("get " + key + " from " + getClass().toString());
				topStore.put(entry.key, entry);
				remove(entry.key);
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
		size.decrementAndGet();
		CacheEntry2 entry =  super.remove(key);
		if (entry == null) {
			if (nextStore != null) {
				entry = nextStore.remove(key);
			}
		} else {
//			System.out.println("remove " + entry.key + " from " + getClass().toString());
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
	
	protected boolean removeEldestEntry(java.util.Map.Entry<String, CacheEntry2> eldest) {
		if (limit.get() == -1) return false;
		if (size.get() > limit.get() ) {
			if (nextStore != null) {
				nextStore.put(eldest.getKey(), eldest.getValue());
			}
//			System.out.println("removing " + removed.incrementAndGet());
			return true;
		}
		return false;
	}

	abstract byte[] toStream(CacheEntry entry);
	abstract Object toObject(CacheEntry entry);

	public void dispose() {
		clear();
	}
	
	@Override
	public String toString() {
		return storeName() + ": entries " + size.get() + "/" + limit;
	}
}
