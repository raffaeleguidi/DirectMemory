package org.directmemory.supervisor;

import org.directmemory.cache.CacheManager;

public interface Supervisor {

	public abstract void disposeOverflow(CacheManager cache);
	// this is probably not useful and was not implemented in any case
	//void signalOverflow(Storage storage);
}