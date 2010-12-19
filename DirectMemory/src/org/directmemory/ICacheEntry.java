package org.directmemory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

public interface ICacheEntry {

	public abstract void touch();

	public abstract Date getLastUsed();

	public abstract Date lastUsed();

	public abstract int getDuration();

	public abstract void setDuration(int duration);

	public abstract String getKey();

	public abstract void setKey(String key);

	public abstract int getSize();

	public abstract void setSize(int size);

	public abstract Date getTimeStamp();

	public abstract boolean expired();

	public abstract int size();

	public abstract void dispose();
	
	public abstract Serializable getPayload();
	
	public abstract boolean offHeap();
	
	public abstract void moveOffHeap() throws IOException;

	public abstract void moveInHeap() throws IOException, ClassNotFoundException;
}