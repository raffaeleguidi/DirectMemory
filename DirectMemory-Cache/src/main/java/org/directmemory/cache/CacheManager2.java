package org.directmemory.cache;



import org.directmemory.store.AbstractStore;
import org.directmemory.store.HeapStore;
import org.directmemory.store.SimpleOffHeapStore;

public class CacheManager2  {
	public HeapStore heap;
	
	public CacheManager2(int limit) {
		heap = new HeapStore();
		heap.limit = limit;
	}

	public CacheManager2(int limit, AbstractStore secondLevel) {
		heap = new HeapStore();
		heap.limit = limit;
		heap.nextStore = secondLevel;
		secondLevel.topStore = heap;
	}

	public CacheManager2(int limit, SimpleOffHeapStore secondLevel, int limit2) {
		heap = new HeapStore();
		heap.limit = 1000;
		heap.nextStore = secondLevel;
		secondLevel.limit = limit2;
		secondLevel.topStore = heap;
	}

	public CacheManager2(int limit, AbstractStore secondLevel, int limit2, AbstractStore thirdLevel) {
		heap = new HeapStore();
		heap.limit = 1000;
		heap.nextStore = secondLevel;
		secondLevel.limit = limit2;
		secondLevel.topStore = heap;
		secondLevel.nextStore = thirdLevel;
		thirdLevel.topStore = heap;
	}

	public CacheManager2(int limit, AbstractStore secondLevel, int limit2, AbstractStore thirdLevel, int limit3) {
		heap = new HeapStore();
		heap.limit = 1000;
		heap.nextStore = secondLevel;
		secondLevel.limit = limit2;
		secondLevel.topStore = heap;
		secondLevel.nextStore = thirdLevel;
		thirdLevel.topStore = heap;
		thirdLevel.limit = limit3;
	}

	public void limit(int limit) {
		heap.limit = limit;
	}
	
	public synchronized CacheEntry getEntry(String key) {
		return heap.get(key);
	}
	public synchronized Object get(String key) {
		CacheEntry entry = getEntry(key);
		if (entry != null) {
			return entry.object;
		}
		return null;
	}
	public synchronized CacheEntry remove(String key) {
		return heap.remove(key);
	}
	public synchronized CacheEntry remove(CacheEntry entry) {
		return heap.remove(entry.key);
	}
	public synchronized CacheEntry put(String key, Object object) {
		CacheEntry entry = new CacheEntry();
		entry.key = key;
		entry.object = object;
		return put(entry);
	}
	public synchronized CacheEntry put(CacheEntry entry) {
		return heap.put(entry.key, entry);
	}
}
