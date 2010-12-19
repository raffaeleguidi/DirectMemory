package org.directmemory.impl;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

import org.directmemory.ICacheEntry;

public class CacheEntryImpl implements ICacheEntry {
	
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
	public CacheEntryImpl(String key, int size, int position) {
		this.key = key;
		this.size = size;
		this.position = position;
	}
	public CacheEntryImpl(String key, int size, int position, int duration) {
		this.key = key;
		this.size = size;
		this.position = position;
		this.duration = duration;
	}
	
	public CacheEntryImpl (String key, byte[] source, int duration) throws OutOfMemoryError {
		this.key = key;
		this.size = source.length;
		this.buffer = ByteBuffer.allocateDirect(this.size);
		this.duration = duration;
		this.buffer.put(source);
	}
	
	public CacheEntryImpl() {
		
	}
	
	private CacheEntryImpl (String key, ByteBuffer buffer, int duration) {
		this.key = key;
		this.buffer = buffer;
		this.duration = duration;
		this.size = buffer.capacity();
	}
	
	public static CacheEntryImpl allocate(String key, byte[] source, int duration) {
		try {
			ByteBuffer buffer = ByteBuffer.allocateDirect(source.length);
			buffer.put(source);
			CacheEntryImpl entry = new CacheEntryImpl(key, buffer, duration);
			return entry;
		} catch (OutOfMemoryError e) {
			return null;
		}
	}
	
	public static CacheEntryImpl allocate(String key, byte[] source) {
		return allocate(key, source, -1);
	}
	
	public byte[] getBuffer() {
		
		byte[] dest = null; 
		try {
			synchronized (buffer) {
				buffer.position(0);
				dest = new byte[size];
				buffer.get(dest);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		buffer = null;
	}
}