package org.directcache.buffer;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

import org.directcache.ICacheEntry;

public class CacheEntryWithBuffer implements ICacheEntry {
	
	String key;
	int size;
	int position;
	int duration = -1;
	private Date created = Calendar.getInstance().getTime();
	private Date lastUsed = null;

	ByteBuffer buffer = null;

	public int getDuration() {
		return duration;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public CacheEntryWithBuffer(String key, int size, int position) {
		this.key = key;
		this.size = size;
		this.position = position;
	}
	public CacheEntryWithBuffer(String key, int size, int position, int duration) {
		this.key = key;
		this.size = size;
		this.position = position;
		this.duration = duration;
	}
	
	public CacheEntryWithBuffer (String key, byte[] source, int duration) {
		this.key = key;
		this.size = source.length;
		this.buffer = ByteBuffer.allocateDirect(this.size);
		this.duration = duration;
		this.buffer.put(source);
	}
	
	public byte[] getBuffer() {
		byte[] dest = new byte[size]; 
		synchronized (buffer) {
			buffer.position(0);
			buffer.get(dest);
		}
		return dest;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public int getPosition() {
		return position;
	}
	public void setPosition(int position) {
		this.position = position;
	}
	public Date getTimeStamp() {
		return created;
	}
	public void setTimeStamp(Date timeStamp) {
		this.created = timeStamp;
	}
	
	public boolean expired() {
		if (duration==-1) 
			return false;
		Date expiryTime = new Date(duration + created.getTime());
		boolean result = new Date().after(expiryTime);
		return result;
	}
	@Override
	public void touch() {
		created = Calendar.getInstance().getTime();		
	}

	@Override
	public Date getLastUsed() {
		return lastUsed;
	}
	@Override
	public Date lastUsed() {
		return lastUsed;
	}
	@Override
	public int size() {
		return size;
	}
	@Override
	public void dispose() {
		buffer.clear();
	}
}