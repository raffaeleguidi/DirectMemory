package org.directmemory.cache;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.directmemory.memory.MemorySlot;
import org.directmemory.store.Store;

public class CacheEntry2 {
	public String key = null;
	public int size = -1;
	public int position = -1;

	public Object object = null;
	public ByteBuffer buffer = null;
	public byte[] array = null;

	public long expiresAt = -1;
	public Store store;
	
	public MemorySlot slot = null;

	@SuppressWarnings({"rawtypes","unchecked"})
	public Class clazz = null;
	public String path = null;
	public Object identity = null; // this is for nosql data stores
	
	public boolean onNosql() {
		return identity != null;
	}
	public boolean onDisk() {
		return path != null;
	}
	public boolean inHeap() {
		return object != null;
	}
	
	public boolean offHeap() {
		return object == null && !onDisk() && buffer != null;
	}
	
	public boolean expired() {
		if (expiresAt == -1) return false;
		return System.currentTimeMillis() >= expiresAt;
	}
	
	public long expiresIn (long milliseconds) {
		expiresAt = System.currentTimeMillis() + milliseconds;
		return expiresAt;
	} 
	
	@SuppressWarnings("unchecked")
	public Class<? extends Object> clazz() {
		if (object != null) {
			clazz = object.getClass();
		}
		return clazz;
	}
	
	public long created;
	public long lastAccessed;
	
	public AtomicLong reads = new AtomicLong();
	
	public void setStore(Store store) {
		this.store = store;
	}
	public Store getStore() {
		return store;
	}
}

