package org.directmemory.thread;

import org.directmemory.CacheStore;

public abstract class CacheEnabledThread extends Thread {
	
	public CacheEnabledThread(ThreadGroup group, String name, CacheStore cache) {
		super(group, name);
		this.cache = cache;
	}

	public CacheEnabledThread(CacheStore cache) {
		super();
		this.cache = cache;
	}

	public CacheStore cache;
}
