package org.directcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

public class DirectCache {

	private static Logger logger=Logger.getLogger("org.directcache");
	
	private Map<String, CacheEntry> allocationTable = new Hashtable<String, CacheEntry>();
	private List<CacheEntry> garbage = new Vector<CacheEntry>();
	private int sizeInMb;
//	private int largestTrashedItem = 0;
	private int totalGarbageSize = 0;
	
	
	public DirectCache() {
		// default 50mb
		this.sizeInMb = 50;
		buf = ByteBuffer.allocateDirect(1024*1024*sizeInMb);
		logger.info("DirectCache allocated with the default " + sizeInMb + "mb buffer");
	}
	public DirectCache(int sizeInMb) {
		super();
		this.sizeInMb = sizeInMb;
		//off-heap allocation
		buf = ByteBuffer.allocateDirect(1024*1024*sizeInMb);
		//heap allocation
		//buf = ByteBuffer.allocate(1024*1024*sizeInMb);
		logger.warning("DirectCache allocated with " + sizeInMb + "mb buffer");
		logger.info("DirectCache allocated with " + sizeInMb + "mb buffer");
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
		logger.fine("object serialized");
		return b;		
	}

	public CacheEntry storeObject(String key, Serializable obj) throws Exception {
		return storeObject(key, obj, defaultDuration);
	}
	
	public CacheEntry storeObject(String key, Serializable obj, int duration) throws Exception {

		logger.info("serializing object with key '" + key + "'");

		byte b[] = serializeObject(obj);
		
		logger.info("object with key '" + key + "' serialized (" + b.length + ") bytes");

		synchronized (buf) {
			logger.info("buffer size is " + buf.remaining() + " garbage size is " + totalGarbageSize);
			if (b.length > remaining()) {
				logger.warning("throwing DirectCache full exception");
				throw new DirectCacheFullException("DirectCache full");
			}
			if (b.length > buf.remaining() && b.length <= totalGarbageSize) {
				logger.info("object with key '" + key + "' doesn't fit in buffer but fits in garbage");
				return storeReusingGarbage(key, b, duration);
			} else {
				logger.info("object with key '" + key + "' fits in buffer");
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
				totalGarbageSize -= b.length;
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

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntry entry = allocationTable.get(key);

		if (entry == null) {
			logger.info("could not find object with key '" + key + "'");
			return null;
		}

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
			logger.info("retrieved object with key '" + key + "' (" + b.length + " bytes)");
			return obj;
		}
	}

	public CacheEntry removeObject(String key) {

		logger.info("looking for object with key '" + key + "'");
		
		synchronized (buf) {
			CacheEntry entry = allocationTable.get(key);
			if (entry == null) {
				logger.info("could not find object with key '" + key + "'");
				return null;
			}
			allocationTable.remove(key);
			garbage.add(entry);
			totalGarbageSize += entry.size;
			
			logger.info("object with key '" + key + "' trashed");
			return entry;
		}
	}
	
	public int remaining() {
		return buf.remaining()+totalGarbageSize;
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
