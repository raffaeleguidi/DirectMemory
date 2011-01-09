package org.directmemory.storage;

import org.directmemory.cache.CacheEntry;

public class HeapStorage extends Storage {

	public HeapStorage() {
		
	}
	
	public HeapStorage(int entriesLimit){
		super.entriesLimit = entriesLimit;
	}

	@Override
	protected boolean moveIn(CacheEntry entry) {
		return true;
	}

	@Override
	public boolean moveToHeap(CacheEntry entry) {
		return true;
	}
}
