package org.directmemory.supervisor;

import org.directmemory.CacheStore;

public interface Supervisor {

	public abstract void checkLimits(CacheStore cache);

}