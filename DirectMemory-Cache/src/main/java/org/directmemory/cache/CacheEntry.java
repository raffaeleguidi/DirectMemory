package org.directmemory.cache;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.directmemory.storage.Storage;
import org.directmemory.store.AbstractStore;

public class CacheEntry implements Comparable<CacheEntry> {
	public String key = null;
	public int size = -1;
	public int position = -1;
	public Object object = null;
	public ByteBuffer buffer = null;
	public byte[] array = null;
	public Date expiresOn = null;
	public long expiresAt = -1;
	public AbstractStore store;
	
	

	@SuppressWarnings({"rawtypes","unchecked"})
	public Class clazz = null;
	public String path = null;
	public Object identity = null; // this is for nosql data stores
	
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
		return ((expiresOn != null) && new Date().after(expiresOn));
	}
	
	public boolean isExpired() {
		if (expiresAt == -1) return false;
		return System.currentTimeMillis() >= expiresAt;
	}
	
	public long expires (long milliseconds) {
		expiresAt = System.currentTimeMillis() + milliseconds;
		return expiresAt;
	} 
	
	public void expiresIn(long milliseconds) {
		if (milliseconds != -1)
			expiresOn = new Date(new Date().getTime() + milliseconds);
	}	

	public int compareTo(CacheEntry other) {
		if (this.size < other.size)
			return -1;
		if (this.size == other.size)
			return 0;
		return 1;
	}
	
	public byte[] bufferData() {
		if (buffer != null) {
			final byte[] temp = new byte[size];
			buffer.position(position);
			buffer.get(temp);
			return temp;
		}		
		return null;
	}

	@SuppressWarnings("unchecked")
	public Class<? extends Object> clazz() {
		if (object != null) {
			clazz = object.getClass();
		}
		return clazz;
	}
	
	private Storage storage = null;
	public Date inserted;
	public Date lastAccessed;
	public AtomicLong reads = new AtomicLong();
	
	public Storage getStorage() {
		return storage;
	}
	public void setStorage(Storage storage) {
		this.storage = storage;
	}
	public void setStore(AbstractStore store) {
		this.store = store;
	}
	public AbstractStore getStore() {
		return store;
	}
}

