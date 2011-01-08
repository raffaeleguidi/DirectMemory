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
	
	

	@SuppressWarnings({"rawtypes","unchecked"})
	public Class clazz = null;
	public String path = null;
	
	public boolean onDisk() {
		return path != null;
	}
	public boolean inHeap() {
		return object != null;
	}
	
	public boolean offHeap() {
		return object == null && !onDisk();
	}
	
	public boolean expired() {
		return ((expiresOn != null) && Calendar.getInstance().after(expiresOn));
	}
	
	public void expiresIn(int milliseconds) {
		if (milliseconds != -1)
			expiresOn = new Date(new Date().getTime() + milliseconds);
	}	

	public int compareTo(CacheEntry other) {
		if (this.size < other.size)
			return -1;
		if (this.size == other.size)
			return 0;
		return 1;
	}
	
	public byte[] rawData() {
		if (buffer != null) {
			final byte[] temp = new byte[size];
			buffer.position(position);
			buffer.get(temp);
			return temp;
		}		
		if (array != null) {
			return array;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public Class<? extends Object> clazz() {
		if (object != null) {
			clazz = object.getClass();
		}
		return clazz;
	}
}
