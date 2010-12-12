package org.directcache;

import java.util.Calendar;
import java.util.Date;

public class CacheEntry implements ICacheEntry {
	
	String key;
	int size;
	int position;
	private Date created = Calendar.getInstance().getTime();
	private Date lastUsed = null;

	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#touch()
	 */
	@Override
	public void touch() {
		this.lastUsed = Calendar.getInstance().getTime();
	}

	int duration = -1;

	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#getLastUsed()
	 */
	@Override
	public Date getLastUsed() {
		return lastUsed;
	}
	
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#lastUsed()
	 */
	@Override
	public Date lastUsed() {
		return lastUsed;
	}
	
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#getDuration()
	 */
	@Override
	public int getDuration() {
		return duration;
	}
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#setDuration(int)
	 */
	@Override
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
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#getKey()
	 */
	@Override
	public String getKey() {
		return key;
	}
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#setKey(java.lang.String)
	 */
	@Override
	public void setKey(String key) {
		this.key = key;
	}
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#getSize()
	 */
	@Override
	public int getSize() {
		return size;
	}
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#setSize(int)
	 */
	@Override
	public void setSize(int size) {
		this.size = size;
	}
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#getPosition()
	 */
	@Override
	public int getPosition() {
		return position;
	}
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#setPosition(int)
	 */
	@Override
	public void setPosition(int position) {
		this.position = position;
	}
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#getTimeStamp()
	 */
	@Override
	public Date getTimeStamp() {
		return created;
	}
	
	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#expired()
	 */
	@Override
	public boolean expired() {
		if (duration==-1) 
			return false;
		Date expiryTime = new Date(duration + created.getTime());
		boolean result = new Date().after(expiryTime);
		return result;
	}

	/* (non-Javadoc)
	 * @see org.directcache.ICacheEntry#size()
	 */
	@Override
	public int size() {
		return size;
	}

	@Override
	public void dispose() {
		// noop
	}
}
