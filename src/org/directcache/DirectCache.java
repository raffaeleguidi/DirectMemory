package org.directcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class DirectCache {
	private Map<String, CacheEntry> allocationTable = new HashMap<String, CacheEntry>();
	private List<CacheEntry> garbage = new Vector<CacheEntry>();
	private int sizeInMb;
	public DirectCache() {
		// default 50mb
		this.sizeInMb = 1024*1024*50;
		buf = ByteBuffer.allocateDirect(sizeInMb);
	}
	public DirectCache(int sizeInMb) {
		super();
		this.sizeInMb = sizeInMb;
		buf = ByteBuffer.allocateDirect(sizeInMb);
	}

	private ByteBuffer buf;
	private int defaultDuration=0;

	public int getDefaultDuration() {
		return defaultDuration;
	}
	public void setDefaultDuration(int defaultDuration) {
		this.defaultDuration = defaultDuration;
	}
	public Map<String, CacheEntry> getAllocationTable() {
		return allocationTable;
	}

	private byte[] serializeObject(Serializable obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();
		byte[] b = baos.toByteArray();
		return b;		
	}

	public CacheEntry storeObject(String key, Serializable obj) throws Exception {
		return storeObject(key, obj, defaultDuration);
	}
	
	public CacheEntry storeObject(String key, Serializable obj, int duration) throws Exception {

		byte b[] = serializeObject(obj);
		
		synchronized (buf) {
			if (b.length > remaining()) {
				throw new Exception("DirectCache full");
			}
			if (b.length > buf.remaining() && b.length <= garbageSize) {
				return storeReusingGarbage(key, b, duration);
			} else {
				return storeAtTheEnd(key, b, duration);
			}
		}
	}
	
	private CacheEntry storeReusingGarbage(String key, byte[] b, int duration) throws Exception {
		for (CacheEntry trashed : garbage) {
			if (trashed.size >= b.length) {
				CacheEntry entry = new CacheEntry(key, b.length, trashed.position, duration);
				allocationTable.put(key, entry);
				trashed.size -= b.length;
				garbageSize -= b.length;
				trashed.position += b.length;
				if (trashed.size == 0) 
					garbage.remove(trashed);
				int oldPos = buf.position();
				buf.position(entry.position);
				buf.put(b, 0, entry.size);
				buf.position(oldPos);
				return entry;
			}
		}
		throw new BufferTooFragmentedException("Buffer too fragmented");
	}
	private CacheEntry storeAtTheEnd(String key, byte[] b, int duration) {
		CacheEntry entry = new CacheEntry(key, b.length, buf.position(), duration);
		buf.put(b);
		allocationTable.put(key,entry);
		return entry;
	} 	
	
	public Serializable retrieveObject(String key) throws IOException, ClassNotFoundException {
		CacheEntry entry = allocationTable.get(key);
		if (entry == null)
			return null;
		byte[] b = new byte[entry.size];
		synchronized (buf) {
			int pos = buf.position();
			buf.position(entry.position);
			buf.get(b);
			buf.position(pos);
			ByteArrayInputStream bis = new ByteArrayInputStream(b);
			ObjectInputStream ois = new ObjectInputStream(bis);
			Serializable obj = (Serializable) ois.readObject();
			ois.close();
			return obj;
		}
	}

	public CacheEntry removeObject(String key) {
		synchronized (buf) {
			CacheEntry entry = allocationTable.get(key);
			if (entry == null) 
				return null;
			allocationTable.remove(key);
			garbage.add(entry);
			garbageSize += entry.size;
			return entry;
		}
	}
	
	private int garbageSize = 0;
	
	public int remaining() {
		return buf.remaining()+garbageSize;
	}
	public int size() {
		return (buf.capacity()-remaining());
	}
	public int capacity() {
		return buf.capacity();
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("DirectCache {" );
		sb.append("items: ");
		sb.append(getAllocationTable().size());
		sb.append(", ");
		sb.append("capacity (mb): ");
		sb.append(capacity()/1024/1024);
		sb.append(", ");
		sb.append("size (mb): ");
		sb.append(size()/1024/1024);
		sb.append(", ");
		sb.append("remaining (mb): ");
		sb.append(remaining()/1024/1024);
		sb.append("}");
		
		return sb.toString();
	}
	
}
