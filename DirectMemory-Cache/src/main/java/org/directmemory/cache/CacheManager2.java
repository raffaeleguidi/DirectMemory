package org.directmemory.cache;



import org.directmemory.store.AbstractStore;
import org.directmemory.store.HeapStore;
import org.directmemory.store.SimpleOffHeapStore;

public class CacheManager2  {
	public HeapStore heap;
	private long createdAt = System.currentTimeMillis();
	
	public long uptime() {
		return System.currentTimeMillis() - createdAt;
	}
	
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
		heap.limit = limit;
		heap.nextStore = secondLevel;
		secondLevel.limit = limit2;
		secondLevel.topStore = heap;
	}

	public CacheManager2(int limit, AbstractStore secondLevel, int limit2, AbstractStore thirdLevel) {
		heap = new HeapStore();
		heap.limit = limit;
		heap.nextStore = secondLevel;
		secondLevel.limit = limit2;
		secondLevel.topStore = heap;
		secondLevel.nextStore = thirdLevel;
		thirdLevel.topStore = heap;
	}

	public CacheManager2(int limit, AbstractStore secondLevel, int limit2, AbstractStore thirdLevel, int limit3) {
		heap = new HeapStore();
		heap.limit = limit;
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
	public String measures() {
		// to be used with the speed aspect
		return null;
	}
	
	public void dispose() {
		AbstractStore current = heap;
		while (current != null) {
			current.dispose();
			current = current.nextStore;
		}
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("DirectMemory Cache status: {\r\n");
		sb.append("   uptime: ");
		sb.append(uptime());
		sb.append("ms");
		sb.append("\r\n   stores: {");
		AbstractStore current = heap;
		while (current != null) {
			sb.append("\r\n      ");
			sb.append(current.toString());
			current = current.nextStore;
		}
		sb.append("\r\n   }");
		sb.append("\r\n}");
		return sb.toString();
	}
}
