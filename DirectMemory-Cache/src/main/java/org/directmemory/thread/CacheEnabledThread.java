package org.directmemory.thread;

import org.directmemory.CacheManager;

public abstract class CacheEnabledThread extends Thread {
	
	public CacheEnabledThread(ThreadGroup group, String name, CacheManager cache) {
		super(group, name);
		this.cache = cache;
	}

	public CacheEnabledThread(CacheManager cache) {
		super();
		this.cache = cache;
	}

	public CacheManager cache;
}
