package org.directmemory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public interface ICacheStore {

	public abstract void reset();

	public abstract int getDefaultDuration();

	public abstract void setDefaultDuration(int defaultDuration);

	public abstract Map<String, ICacheEntry> entries();

	public abstract ICacheEntry put(String key, Serializable obj) throws IOException;

	public abstract ICacheEntry put(String key, Serializable obj,
			int duration) throws Exception;

	public abstract Serializable get(String key) throws IOException,
			ClassNotFoundException;

	public abstract ICacheEntry delete(String key);

	public abstract long remaining();

	public abstract long usedMemory();

	public abstract long offHeapLimit();

	public abstract void dispose();
	
	public abstract ICacheSupervisor getSupervisor(); 

	public abstract void setSupervisor(ICacheSupervisor supervisor); 
}