package org.directmemory.storage;

import org.directmemory.CacheEntry;

public class HeapStorage extends Storage {

	public HeapStorage() {
		
	}
	
	public HeapStorage(int entriesLimit){
		super.entriesLimit = entriesLimit;
	}

	@Override
	protected boolean moveIn(CacheEntry entry) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean moveToHeap(CacheEntry entry) {
		// TODO Auto-generated method stub
		return true;
	}
}
