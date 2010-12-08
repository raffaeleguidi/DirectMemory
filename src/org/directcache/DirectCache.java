package org.directcache;

import static ch.lambdaj.Lambda.filter;
import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.directcache.buffer.ThreadSafeDirectBuffer;
import org.directcache.exceptions.BufferTooFragmentedException;
import org.directcache.exceptions.DirectCacheFullException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectCache {

	private static Logger logger=LoggerFactory.getLogger(DirectCache.class);
	
	private ThreadSafeDirectBuffer buffer;
	private Map<String, CacheEntry> allocationTable = new Hashtable<String, CacheEntry>();
	private List<CacheEntry> garbage = new Vector<CacheEntry>();

	private int sizeInMb;
	private int totalGarbageSize = 0;
	private int defaultDuration=-1;
		
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
		logger.debug("object serialized");
		return b;		
	}

	public CacheEntry storeObject(String key, Serializable obj) throws Exception {
		return storeObject(key, obj, defaultDuration);
	}
	
	public CacheEntry storeObject(String key, Serializable obj, int duration) throws Exception {

		logger.info("serializing object with key '" + key + "'");

		byte b[] = serializeObject(obj);
		
		logger.info("object with key '" + key + "' serialized (" + b.length + ") bytes");
		logger.info("buffer size is " + buffer.remaining() + " garbage size is " + totalGarbageSize);

		synchronized (buffer) {
			if (b.length > remaining()) {
				logger.warn("throwing DirectCache full exception");
				throw new DirectCacheFullException("DirectCache full", b.length);
			}

			// remaining non va bene
			// definire la finestra corrente, ovvero quanti byte liberi ho davanti
			
			if (b.length <= buffer.remaining() &! endReached) {   
				logger.info("object with key '" + key + "' fits in buffer");
				return storeAtTheEnd(key, b, duration);
			} else if (b.length <= totalGarbageSize) {
				logger.info("object with key '" + key + "' doesn't fit in buffer but fits in garbage");
				return storeReusingGarbage(key, b, duration);	
			}
			return null;
		}
	}
	
	private boolean endReached = false;
	
	private CacheEntry storeReusingGarbage(String key, byte[] b, int duration) throws Exception {
		endReached = true;
		
		// a volte va in overflow - capire perchè
		// rifare la query con lambdaj
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
		throw new BufferTooFragmentedException("Buffer too fragmented", b.length);
	}
	private CacheEntry storeAtTheEnd(String key, byte[] b, int duration) {
		CacheEntry entry = new CacheEntry(key, b.length, buffer.position(), duration);
		allocationTable.put(key, entry);
		buffer.append(b);
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
		try {
			Serializable obj = deserialize(b);
			logger.info("retrieved object with key '" + key + "' (" + b.length + " bytes)");		
			return obj;
		} catch (EOFException ex) {
			logger.error("EOFException deserializing object with key '" + key + "' at position " + entry.position + " with size " + entry.size);
			return null;
		}
	}
	
	private Serializable deserialize(byte[] b) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(b);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Serializable obj = (Serializable) ois.readObject();
		ois.close();
		return obj;
	}
	
	public void collectExpired() {	
		
		List<CacheEntry> expiredList = filter(
										having(on(CacheEntry.class).expired())
										, allocationTable.values()
									);
		for (CacheEntry expired : expiredList) {
			removeObject(expired.getKey());
		}
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
