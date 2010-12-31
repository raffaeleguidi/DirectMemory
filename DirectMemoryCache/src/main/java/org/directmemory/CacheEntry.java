package org.directmemory;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

public class CacheEntry implements Comparable<CacheEntry> {
	public String key = null;
	public int size = -1;
	public int position = -1;
	public Object object = null;
	public ByteBuffer buffer = null;
	public byte[] array = null;
	public Date expiresOn = null;
	
	
	@SuppressWarnings("rawtypes")
	public Class clazz = null;
	
	public boolean inHeap() {
		return !offHeap();
	}
	
	public boolean offHeap() {
		return object == null;
	}
	
	public boolean expired() {
		return Calendar.getInstance().after(expiresOn);//return expiresOn < new Date();
	}
	
	public void expiresIn(int milliseconds) {
		expiresOn = new Date(new Date().getTime() + milliseconds);
	}	

	public int compareTo(CacheEntry other) {
		if (this.size < other.size)
			return -1;
		if (this.size == other.size)
			return 0;
		return 1;
	}

}

