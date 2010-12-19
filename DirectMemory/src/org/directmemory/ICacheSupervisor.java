package org.directmemory;


public interface ICacheSupervisor {

	public abstract long signalLRUCollectionNeeded(ICacheStore store,
			long bytesToFree);

	public abstract void signalWeDeleted(String key);

	public abstract void signalWeInserted(ICacheEntry newEntry);

	public abstract void signalWeRetrieved(ICacheEntry entry);

	public abstract void signalReset();

	public abstract long signalCollectExpiredNeeded(
			ICacheStore store, long bytesToFree);

}