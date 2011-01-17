package org.directmemory.store;

import org.directmemory.cache.CacheEntry;

public class OffHeapStore extends AbstractStore {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	void popIn(CacheEntry entry) {
//		System.out.println("offheap popIn entry " + entry.key);
	}

	@Override
	void popOut(CacheEntry entry) {
//		System.out.println("offheap popOut entry " + entry.key);
	}
	
	@Override
	public CacheEntry remove(Object key) {
		return super.remove(key);
	}

}
