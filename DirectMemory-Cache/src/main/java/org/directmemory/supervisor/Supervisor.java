package org.directmemory.supervisor;

import org.directmemory.CacheManager;
import org.directmemory.storage.Storage;

public interface Supervisor {

	public abstract void disposeOverflow(CacheManager cache);
	void signalOverflow(Storage storage);
}