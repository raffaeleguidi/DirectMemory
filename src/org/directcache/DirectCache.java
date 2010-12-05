package org.directcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

import org.directcache.buffer.ThreadSafeDirectBuffer;
import org.directcache.exceptions.BufferTooFragmentedException;
import org.directcache.exceptions.DirectCacheFullException;

public class DirectCache {

	private static Logger logger=Logger.getLogger("org.directcache");
	
	private ThreadSafeDirectBuffer buffer;
	private Map<String, CacheEntry> allocationTable = new Hashtable<String, CacheEntry>();
	private List<CacheEntry> garbage = new Vector<CacheEntry>();

	private int sizeInMb;
	private int totalGarbageSize = 0;
	private int defaultDuration=0;

	
	
	public DirectCache() {
		// defaults to 50mb
		this.sizeInMb = 50;
		buffer = new ThreadSafeDirectBuffer(1024*1024*sizeInMb);
		logger.info("DirectCache allocated with the default " + sizeInMb + "mb buffer");
	}
	public DirectCache(int sizeInMb) {
		super();
		this.sizeInMb = sizeInMb;
		buffer = new ThreadSafeDirectBuffer(1024*1024*sizeInMb);
		logger.info("DirectCache allocated with " + sizeInMb + "mb buffer");
	}

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

		synchronized (buffer) {
			logger.info("buffer size is " + buffer.remaining() + " garbage size is " + totalGarbageSize);
			if (b.length > remaining()) {
				logger.warning("throwing DirectCache full exception");
				throw new DirectCacheFullException("DirectCache full");
			}
			if (b.length > buffer.remaining() && b.length <= totalGarbageSize) {
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
				buffer.put(b, entry.position);
				
				// not really an optimized garbage collection algorythm but...
				if (trashed.size == 0) 
					garbage.remove(trashed);

				return entry;
			}
		}
		throw new BufferTooFragmentedException("Buffer too fragmented");
	}
	private CacheEntry storeAtTheEnd(String key, byte[] b, int duration) {
		CacheEntry entry = new CacheEntry(key, b.length, buffer.position(), duration);
		buffer.append(b);
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

		byte[] b = buffer.get(entry.position, entry.size);
		Serializable obj = deserialize(b);
		
		logger.info("retrieved object with key '" + key + "' (" + b.length + " bytes)");
		
		return obj;
	}
	
	private Serializable deserialize(byte[] b) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(b);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Serializable obj = (Serializable) ois.readObject();
		ois.close();
		return obj;
	}

	public CacheEntry removeObject(String key) {

		logger.info("looking for object with key '" + key + "'");
		
		synchronized (buffer) {
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
		return buffer.remaining()+totalGarbageSize;
	}
	public int size() {
		return (buffer.capacity()-remaining());
	}
	public int capacity() {
		return buffer.capacity();
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
