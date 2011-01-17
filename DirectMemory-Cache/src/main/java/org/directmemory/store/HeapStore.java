package org.directmemory.store;

import org.directmemory.cache.CacheEntry;

public class HeapStore extends AbstractStore {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	void popIn(CacheEntry entry) {
//		System.out.println("heap popIn entry " + entry.key);
	}

	@Override
	void popOut(CacheEntry entry) {
//		System.out.println("heap popOut entry " + entry.key);
	}

}
