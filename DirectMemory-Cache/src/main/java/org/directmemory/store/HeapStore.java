package org.directmemory.store;

import org.directmemory.cache.CacheEntry;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class HeapStore extends AbstractStore {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	void popIn(CacheEntry entry) {
		entry.setStore(this);
	}

	@Override
	void popOut(CacheEntry entry) {
//		System.out.println("heap popOut entry " + entry.key);
	}

	@Override
	byte[] toStream(CacheEntry entry) {
		throw new NotImplementedException();
	}

	@Override
	Object toObject(CacheEntry entry) {
		return entry.object;
	}

}
