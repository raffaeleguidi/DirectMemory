package org.directmemory.store;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheEntry2;
import org.directmemory.measures.Ram;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class HeapStore extends ConcurrentAbstractStore implements Store {

	@Override
	String storeName() {
		return "HeapStore";
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	void popIn(CacheEntry2 entry) {
		entry.setStore(this);
	}

	@Override
	void popOut(CacheEntry2 entry) {
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
	
	@Override
	public String toString() {
		return super.toString() + " with a " + Ram.inMb(Runtime.getRuntime().totalMemory()) + "/" + Ram.inMb(Runtime.getRuntime().maxMemory()) + " heap usage and " + Ram.inMb(Runtime.getRuntime().freeMemory()) + " of free heap";
	}

}
