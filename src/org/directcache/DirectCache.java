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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectCache {

	// suggested java memory settings:
	//-Xms512m -Xmx512m -XX:MaxPermSize=512m -Xss512k
	//   - win32 with 4gb - allocated 1250mb
	//   - win32 with 2gb - allocated 490mb

	// have to try this
	//-XX:MaxDirectMemorySize
	//java -d64 -XX:MaxDirectMemorySize=12g -XX:+UseLargePages com.example.MyApp	
	// see http://www.kdgregory.com/index.php?page=java.byteBuffer
	// and http://www.kdgregory.com/programming/java/ByteBuffer_JUG_Presentation.pdf
	
	private static Logger logger=LoggerFactory.getLogger(DirectCache.class);
	
	private ThreadSafeDirectBuffer buffer;
	private Map<String, CacheEntry> entries = new Hashtable<String, CacheEntry>();
	private List<CacheEntry> garbagedEntries = new Vector<CacheEntry>();

	private int capacity;
	private int usedMemory;
	private int garbageSize = 0;
	private int defaultDuration=-1;
		
	public DirectCache() {
		// defaults to 50mb
		this.capacity = 50*1024*1024;
		buffer = new ThreadSafeDirectBuffer(1024*1024*capacity);
		logger.info("DirectCache allocated with the default " + capacity + "mb buffer");
	}
	public DirectCache(int capacity) {
		this.capacity = capacity;
		buffer = new ThreadSafeDirectBuffer(capacity);
		logger.info("DirectCache allocated with " + capacity + " bytes buffer");
	}

	public int getDefaultDuration() {
		return defaultDuration;
	}
	public void setDefaultDuration(int defaultDuration) {
		this.defaultDuration = defaultDuration;
	}
	public Map<String, CacheEntry> entries() {
		return entries;
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

		logger.info("attempting to remove object with key '" + key + "' - just in case");

		removeObject(key);
		
		logger.info("serializing object with key '" + key + "'");

		byte source[] = serializeObject(obj);
		
		logger.info("object with key '" + key + "' serialized (" + source.length + ") bytes");
		logger.info("buffer size is " + buffer.remaining() + " garbage size is " + garbageSize);

		synchronized (buffer) {
			if (source.length > remaining()) {
				collectExpired();
			}
			if (source.length > remaining()) {
				logger.warn("DirectCache full");
				return null;
			}
			
			CacheEntry storedEntry = null;
			
			if (source.length <= buffer.remaining() &! endReached) {   
				logger.info("object with key '" + key + "' fits in buffer");
				storedEntry = storeAtTheEnd(key, source, duration);
			} else if (source.length <= garbageSize) {
				logger.info("object with key '" + key + "' doesn't fit in buffer but fits in garbage");
				storedEntry = storeReusingGarbage(key, source, duration);	
			}
			if (storedEntry != null) {
				usedMemory+=storedEntry.size;
			}
			return storedEntry;
		}
	}
	
	private boolean endReached = false;

	private CacheEntry storeReusingGarbage(String key, byte[] source, int duration) throws Exception {
		endReached = true;

		logger.info("storing object with key '" + key + "'");

//		logger.warn("Executing lambda query for objects larger than " + source.length);
//
//		List<CacheEntry> entriesThatFit =
//			sort(
//				filter(
//					having( 
//							on(
//									CacheEntry.class).size, 
//									greaterThan(source.length) 
//							), 
//					garbagedEntries
//				)
//				,
//				on(CacheEntry.class).size
//			);
//		
//		logger.warn("Finished lambda query");
//
//		if (entriesThatFit.size() >= 1) {
//			logger.warn("Obtained " + entriesThatFit.size() + " entries - using the first");
//			CacheEntry trashed = entriesThatFit.get(1);
//			CacheEntry entry = new CacheEntry(key, source.length, trashed.position, duration);
//			entries.put(key, entry);
//			trashed.size -= source.length;
//			garbageSize -= source.length;
//			trashed.position += source.length;
//			buffer.put(source, entry.position);
//			logger.warn("Reusing entry " + key);
//			return entry;
//		} else {
//			logger.warn("Buffer too fragmented for entry " + key);
//			return null;
//		}
		
		// a volte va in overflow - capire perchè
		// rifare la query con lambdaj

		for (CacheEntry trashed : garbagedEntries) {
			if (trashed.size >= source.length) {
				CacheEntry entry = new CacheEntry(key, source.length, trashed.position, duration);
				entries.put(key, entry);
				trashed.size -= source.length;
				garbageSize -= source.length;
				trashed.position += source.length;
				buffer.put(source, entry.position);
				
				// not really an optimized garbage collection algorythm but...
				if (trashed.size == 0) 
					garbagedEntries.remove(trashed);

				logger.info("Reusing entry " + key);

				return entry;
			}
		}
		logger.warn("Buffer too fragmented for entry " + key);
		return null;
	}
	private CacheEntry storeAtTheEnd(String key, byte[] bsource, int duration) {
		logger.info("storing object with key '" + key + "'");
		CacheEntry entry = new CacheEntry(key, bsource.length, buffer.position(), duration);
		entries.put(key, entry);
		buffer.append(bsource);
		return entry;
	} 	
	
	public Serializable retrieveObject(String key) throws IOException, ClassNotFoundException {

		logger.info("looking for object with key '" + key + "'");
		
		CacheEntry entry = entries.get(key);

		if (entry == null) {
			logger.info("could not find object with key '" + key + "'");
			return null;
		}

		// mah...
		synchronized (buffer) {
			byte[] dest = buffer.get(entry.position, entry.size);
			try {
				Serializable obj = deserialize(dest);
				logger.info("retrieved object with key '" + key + "' ("
						+ dest.length + " bytes)");
				return obj;
			} catch (EOFException ex) {
				logger.error("EOFException deserializing object with key '"
						+ key + "' at position " + entry.position
						+ " with size " + entry.size);
				return null;
			}
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
										, entries.values()
									);
		logger.warn("Collecting " + expiredList.size() +  " expired entries");

		for (CacheEntry expired : expiredList) {
			removeObject(expired.getKey());
		}
	}
	
	public CacheEntry removeObject(String key) {

		logger.info("looking for object with key '" + key + "'");
		
		synchronized (buffer) {
			CacheEntry entry = entries.get(key);
			if (entry == null) {
				logger.info("could not find object with key '" + key + "'");
				return null;
			}
			entries.remove(key);
			garbagedEntries.add(entry);
			garbageSize += entry.size;
			usedMemory -= entry.size;
			
			logger.info("object with key '" + key + "' trashed");
			return entry;
		}
	}
	
	public int remaining() {
		return capacity-usedMemory;
	}
	public int usedMemory() {
		return usedMemory;
	}
	public int capacity() {
		return buffer.capacity();
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("DirectCache {" );
		sb.append("entries: ");
		sb.append(entries().size());
		sb.append(", ");
		sb.append("capacity (mb): ");
		sb.append(capacity()/1024/1024);
		sb.append(", ");
		sb.append("size (mb): ");
		sb.append(usedMemory()/1024/1024);
		sb.append(", ");
		sb.append("remaining (mb): ");
		sb.append(remaining()/1024/1024);
		sb.append("}");
		
		return sb.toString();
	}
	
}
