package org.directmemory;
import java.nio.ByteBuffer;

public class CacheEntry implements Comparable<CacheEntry> {
	public String key = null;
	public int size = -1;
	public int position = -1;
	public Object object = null;
	public ByteBuffer buffer = null;
	@SuppressWarnings("rawtypes")
	public Class clazz = null;
	
	public boolean inHeap() {
		return !offHeap();
	}
	
	public boolean offHeap() {
		return object == null;
	}

	@Override
	public int compareTo(CacheEntry other) {
		if (this.size < other.size)
			return -1;
		if (this.size == other.size)
			return 0;
		return 1;
	}

}

