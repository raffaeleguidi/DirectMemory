package org.directmemory.impl;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

import org.directmemory.ICacheEntry;
import org.directmemory.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheEntryImpl implements ICacheEntry {
	
	private static Logger logger=LoggerFactory.getLogger(CacheEntryImpl.class);

	String key;
	int size;
	int duration = -1;
	private Serializable payLoad = null;
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
		this.duration = -1;
	}

	public CacheEntryImpl (String key, byte[] source, int duration) throws OutOfMemoryError {
		this.key = key;
		this.size = source.length;
		this.duration = duration;
		this.buffer = ByteBuffer.allocateDirect(this.size);
		this.buffer.put(source);
	}
	
	public CacheEntryImpl (String key, Serializable payLoad, int duration) throws OutOfMemoryError {
		this.key = key;
		this.size = -1;
		this.payLoad = payLoad;
		this.duration = duration;
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
			logger.error("error retrieving buffer: " + e.getMessage());
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
	public Date getTimeStamp() {
		return created;
	}
	public void setTimeStamp(Date timeStamp) {
		this.created = timeStamp;
	}
	
	public boolean expired() {
		if (duration==-1) {
			return false;
		}
		Date expiryTime = new Date(duration + created.getTime());
		boolean result = new Date().after(expiryTime);
		return result;
	}
	
	public boolean offHeap() {
		return buffer != null;
	}
	
	public void touch() {
		created = Calendar.getInstance().getTime();		
	}

	public Date getLastUsed() {
		return lastUsed;
	}
	public Date lastUsed() {
		return lastUsed;
	}
	public int size() {
		return size;
	}
	public void dispose() {
		buffer.clear();
		buffer = null;
	}
	public Serializable getPayload() {
		if (offHeap()) {
			try {
				return SerializationUtils.deserialize(getBuffer());
			} catch (IOException e) {
				logger.error("error retrieving payload: " + e.getMessage());
			} catch (ClassNotFoundException e) {
				logger.error("error retrieving payload: " + e.getMessage());
			}
		} 
		return payLoad;
	}
	public void moveOffHeap() throws IOException {
		byte[] source = SerializationUtils.serializeObject(payLoad);
		ByteBuffer buffer = ByteBuffer.allocateDirect(source.length);
		buffer.put(source);
		size = source.length;
		payLoad = null;
	}
	public void moveInHeap() throws IOException, ClassNotFoundException {
		payLoad = SerializationUtils.deserialize(getBuffer());
		buffer.clear();
		buffer = null;
		size = -1;
	}	
}