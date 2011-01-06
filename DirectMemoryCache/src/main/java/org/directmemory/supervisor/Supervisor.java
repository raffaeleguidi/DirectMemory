package org.directmemory.supervisor;

import org.directmemory.CacheStore;
import org.directmemory.storage.Storage;

public interface Supervisor {

	public abstract void disposeOverflow(CacheStore cache);
	void signalOverflow(Storage storage);
}