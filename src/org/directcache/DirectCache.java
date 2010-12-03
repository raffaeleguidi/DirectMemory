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
	private Map<String, CacheDescriptor> index = new HashMap();
	private List<CacheDescriptor> garbage = new Vector<CacheDescriptor>();
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

	public Map getIndex() {
		return index;
	}

	public void setIndex(Map index) {
		this.index = index;
	}
	private byte[] serializeObject(Serializable obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();
		byte[] b = baos.toByteArray();
		return b;		
	}

	public CacheDescriptor storeObject(String key, Serializable obj) throws Exception {

		byte b[] = serializeObject(obj);
		
		if (b.length > remaining()) {
			throw new Exception("DirectCache full");
		}

		if (b.length > buf.remaining() && b.length <= garbageSize) {
			return storeReusingGarbage(key, b);
		} else {
			// best case - it won't happen too often
			return storeAtTheEnd(key, b);			
		}
	}
	
	private CacheDescriptor storeReusingGarbage(String key, byte[] b) throws Exception {
		//throw new Exception("Garbage reuse not yet implemented");
		for (CacheDescriptor trashed : garbage) {
			if (trashed.size >= b.length) {
				CacheDescriptor desc = new CacheDescriptor(key, b.length, trashed.position);
				index.put(key, desc);
				trashed.size -= b.length;
				garbageSize -= b.length;
				trashed.position += b.length;
				if (trashed.size == 0) 
					garbage.remove(trashed);
				int oldPos = buf.position();
				buf.position(desc.position);
				buf.put(b, 0, desc.size);
				buf.position(oldPos);
				return desc;
			}
		}
		throw new Exception("Buffer too fragmented");
	}
	private CacheDescriptor storeAtTheEnd(String key, byte[] b) {
		CacheDescriptor descr = new CacheDescriptor(key, b.length, buf.position());
		buf.put(b);
		index.put(key,descr);
		return descr;
	} 	
	
	public Serializable retrieveObject(String key) throws IOException, ClassNotFoundException {
		CacheDescriptor desc = index.get(key);
		byte[] b = new byte[desc.size];
		int pos = buf.position();
		buf.position(desc.position);
		buf.get(b);
		buf.position(pos);
		ByteArrayInputStream bis = new ByteArrayInputStream(b);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Serializable obj = (Serializable) ois.readObject();
		ois.close();
		return obj;
	}

	public CacheDescriptor removeObject(String key) {
		CacheDescriptor desc = index.get(key);
		byte[] b = new byte[desc.size];
		int pos = buf.position();
		buf.position(desc.position);
		buf.put(b);
		buf.position(pos);
		index.remove(key);
		garbage.add(desc);
		garbageSize += desc.size;
		return desc;
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
	
}
