package org.directmemory.store;

import java.util.LinkedHashMap;
import java.util.Map;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheEntry2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class AbstractStore extends LinkedHashMap<String, CacheEntry2> implements Store {

	protected static Logger logger=LoggerFactory.getLogger(AbstractStore.class);
	/**
	 * 
	 */
	public long limit = -1;
	private static final long serialVersionUID = 1L;
	
	abstract String storeName();
	
	public Store nextStore;
	public Store topStore = this;
	
	protected void touch(CacheEntry2 entry) {
		entry.lastAccessed = System.currentTimeMillis();
		entry.reads.incrementAndGet();
	}	
	
	protected void reset(CacheEntry2 entry) {
		entry.created = System.currentTimeMillis();
		entry.lastAccessed = System.currentTimeMillis();
		entry.reads.set(0l);
	}	
	
	/* (non-Javadoc)
	 * @see org.directmemory.store.Store#put(java.lang.String, org.directmemory.cache.CacheEntry2)
	 */
	@Override
	public CacheEntry2 put(String key, CacheEntry2 entry) {
		popIn(entry);
//		System.out.println("put " + key + " in " + getClass().toString());
		reset(entry);
		return super.put(entry.key, entry);
	}
	
	/* (non-Javadoc)
	 * @see org.directmemory.store.Store#get(java.lang.Object)
	 */
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
	
	/* (non-Javadoc)
	 * @see org.directmemory.store.Store#remove(java.lang.Object)
	 */
	@Override
	public CacheEntry2 remove(Object key) {
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
	
	/* (non-Javadoc)
	 * @see org.directmemory.store.Store#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends String, ? extends CacheEntry2> m) {
		throw new NotImplementedException();
	}
	
	abstract void popIn(CacheEntry2 entry);
	abstract void popOut(CacheEntry2 entry);
	
	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry<String, CacheEntry2> eldest) {
		if (size() == limit+1 ) {
			super.removeEldestEntry(eldest);
			if (nextStore != null) {
//				System.out.println("removeeldest " + eldest.getKey() + " from " + getClass().toString());
				nextStore.put(eldest.getKey(), eldest.getValue());
			}
			return true;
		}
		return false;
	}

	abstract byte[] toStream(CacheEntry entry);
	abstract Object toObject(CacheEntry entry);

	/* (non-Javadoc)
	 * @see org.directmemory.store.Store#dispose()
	 */
	@Override
	public void dispose() {
		clear();
	}
	
	/* (non-Javadoc)
	 * @see org.directmemory.store.Store#toString()
	 */
	@Override
	public String toString() {
		return storeName() + ": entries " + size() + "/" + limit;
	}
}
