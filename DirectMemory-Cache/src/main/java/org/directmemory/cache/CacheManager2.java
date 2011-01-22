package org.directmemory.cache;



import org.directmemory.store.ConcurrentAbstractStore;
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
		heap.limit.set(limit);
	}

	public CacheManager2(int limit, ConcurrentAbstractStore secondLevel) {
		heap = new HeapStore();
		heap.limit.set(limit);
		heap.nextStore = secondLevel;
		secondLevel.topStore = heap;
	}

	public CacheManager2(int limit, SimpleOffHeapStore secondLevel, int limit2) {
		heap = new HeapStore();
		heap.limit.set(limit);
		heap.nextStore = secondLevel;
		secondLevel.limit.set(limit2);
		secondLevel.topStore = heap;
	}

	public CacheManager2(int limit, ConcurrentAbstractStore secondLevel, int limit2, ConcurrentAbstractStore thirdLevel) {
		heap = new HeapStore();
		heap.limit.set(limit);
		heap.nextStore = secondLevel;
		secondLevel.limit.set(limit2);
		secondLevel.topStore = heap;
		secondLevel.nextStore = thirdLevel;
		thirdLevel.topStore = heap;
	}

	public CacheManager2(int limit, ConcurrentAbstractStore secondLevel, int limit2, ConcurrentAbstractStore thirdLevel, int limit3) {
		heap = new HeapStore();
		heap.limit.set(limit);
		heap.nextStore = secondLevel;
		secondLevel.limit.set(limit2);
		secondLevel.topStore = heap;
		secondLevel.nextStore = thirdLevel;
		thirdLevel.topStore = heap;
		thirdLevel.limit.set(limit3);
	}

//	public void limit(int limit) {
//		heap.limit = limit;
//	}
//	
	public CacheEntry2 getEntry(String key) {
		return heap.get(key);
	}
	public Object get(String key) {
		CacheEntry2 entry = getEntry(key);
		if (entry != null) {
			return entry.object;
		}
		return null;
	}
	public CacheEntry2 remove(String key) {
		return heap.remove(key);
	}
	public CacheEntry2 remove(CacheEntry entry) {
		return heap.remove(entry.key);
	}
	public CacheEntry2 put(String key, Object object) {
		CacheEntry2 entry = new CacheEntry2();
		entry.key = key;
		entry.object = object;
		return put(entry);
	}
	public CacheEntry2 put(CacheEntry2 entry) {
		return heap.put(entry.key, entry);
	}
	public String measures() {
		// to be used with the speed aspect
		return null;
	}
	
	public void dispose() {
		ConcurrentAbstractStore current = heap;
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
		ConcurrentAbstractStore current = heap;
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
