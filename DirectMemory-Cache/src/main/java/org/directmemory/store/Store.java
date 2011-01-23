package org.directmemory.store;

import java.util.Map;

import org.directmemory.cache.CacheEntry2;

public interface Store {

	public abstract CacheEntry2 put(String key, CacheEntry2 entry);

	public abstract CacheEntry2 get(Object key);

	public abstract CacheEntry2 remove(Object key);

	public abstract void putAll(Map<? extends String, ? extends CacheEntry2> m);

	public abstract void dispose();

	public abstract String toString();

}