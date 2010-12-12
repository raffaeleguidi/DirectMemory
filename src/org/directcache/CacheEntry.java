package org.directcache;

import java.util.Calendar;
import java.util.Date;

public class CacheEntry {
	
	String key;
	int size;
	int position;
	private Date created = Calendar.getInstance().getTime();
	private Date lastUsed = null;

	public void touch() {
		this.lastUsed = Calendar.getInstance().getTime();
	}

	int duration = -1;

	public Date getLastUsed() {
		return lastUsed;
	}
	
	public Date lastUsed() {
		return lastUsed;
	}
	
	public int getDuration() {
		return duration;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public CacheEntry(String key, int size, int position) {
		this.key = key;
		this.size = size;
		this.position = position;
	}
	public CacheEntry(String key, int size, int position, int duration) {
		this.key = key;
		this.size = size;
		this.position = position;
		this.duration = duration;
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
	
	public boolean expired() {
		if (duration==-1) 
			return false;
		Date expiryTime = new Date(duration + created.getTime());
		boolean result = new Date().after(expiryTime);
		return result;
	}

	public int size() {
		return size;
	}
}
