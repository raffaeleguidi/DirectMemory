package org.directmemory.store;

import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import org.directmemory.cache.CacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class AbstractStore extends LinkedHashMap<String, CacheEntry> {

	protected static Logger logger=LoggerFactory.getLogger(AbstractStore.class);
	/**
	 * 
	 */
	public int limit = -1;
	private static final long serialVersionUID = 1L;
	
	public AbstractStore nextStore;
	public AbstractStore topStore = this;
	
	protected void touch(CacheEntry entry) {
		entry.lastAccessed = Calendar.getInstance().getTime();
		entry.reads.incrementAndGet();
	}	
	
	protected void reset(CacheEntry entry) {
		entry.inserted = Calendar.getInstance().getTime();
		entry.lastAccessed = Calendar.getInstance().getTime();
		entry.reads.set(0l);
	}	
	
	@Override
	public CacheEntry put(String key, CacheEntry entry) {
		popIn(entry);
		reset(entry);
		return super.put(key, entry);
	}
	
	@Override
	public CacheEntry get(Object key) {
		CacheEntry entry =  super.get(key);
		if (entry != null) {
			touch(entry);
			// following not needed? is this an access ordered map?
			// think so
			//super.remove(key);
			//super.put(key.toString(), entry);
			if (topStore != this) {
				remove(entry);
				popOut(entry);
				topStore.put(entry.key, entry);
			}
		} else {
			if (nextStore != null) {
				entry = nextStore.get(key);
			}
		}
		return entry;
	}
	
	@Override
	public CacheEntry remove(Object key) {
		CacheEntry entry =  super.remove(key);
		if (entry == null) {
			if (nextStore != null) {
				entry = nextStore.remove(key);
			}
		}
		return entry;
	}
	
	@Override
	public void putAll(Map<? extends String, ? extends CacheEntry> m) {
		throw new NotImplementedException();
	}
	
	abstract void popIn(CacheEntry entry);
	abstract void popOut(CacheEntry entry);
	
	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry<String, CacheEntry> eldest) {
		if (size() == limit ) {
			super.removeEldestEntry(eldest);
			if (nextStore != null) {
				nextStore.put(eldest.getKey(), eldest.getValue());
			}
			return true;
		}
		return false;
	}

	abstract byte[] toStream(CacheEntry entry);
	abstract Object toObject(CacheEntry entry);

	public void dispose() {
		clear();
	}

}
