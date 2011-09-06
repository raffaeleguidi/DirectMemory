package org.directmemory.memory;

public class Pointer {
	public int start;
	public int end;
	public long created;
	public long expires;
	public long expiresIn;
	public long hits;
	public boolean free;
	public long lastHit;
	public int bufferNumber;
	public Class<? extends Object> clazz;
	
	public byte[] content() {
		return null;
	}
	public boolean expired() {
		if (expires > 0 || expiresIn > 0) { 
			return (expiresIn + created < System.currentTimeMillis());
		} else {
			return false;
		}
	}
	
	public float getFrequency() {
		return (float)(System.currentTimeMillis()-created)/hits;
	}
}
