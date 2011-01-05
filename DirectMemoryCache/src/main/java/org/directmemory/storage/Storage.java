package org.directmemory.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.directmemory.CacheEntry;
import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Storage {
	protected static Logger logger=LoggerFactory.getLogger(Storage.class);
	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	public Serializer serializer = new StandardSerializer();
	
	protected abstract boolean store(CacheEntry entry);
	protected abstract boolean restore(CacheEntry entry);

	public boolean put(CacheEntry entry) {
		if (entry.key == null) {
			logger.warn("why an entry with a null key?!?");
		}
		if (store(entry)) {		
			logger.debug("stored entry " + entry.key);
			entries.put(entry.key, entry);
			// remove it to make sure it will not get duplicated
			lruQueue.remove(entry);
			// and then add it to the top of the tail
			lruQueue.add(entry);
			// everything's fine
			return true;
		} else {
			logger.debug("failed to store entry " + entry.key);
			return false;
		}
	}
	
	public boolean remove(CacheEntry entry) {
		logger.debug("remove entry " + entry.key);
		if (restore(entry)) {
			return lruQueue.remove(entry);
		} else {
			return false;
		}
	}
	
	public boolean remove(String key) {
		logger.debug("remove entry with key " + key);
		CacheEntry entry = entries.get(key);
		return lruQueue.remove(entry);
	}
	
	public CacheEntry get(String key) {
		CacheEntry entry = entries.get(key);
		if (restore(entry)) {
			lruQueue.remove(entry);
			lruQueue.add(entry);
			logger.debug("retrieve entry with key " + key);
			return entry;
		} else {
			logger.debug("failed to retrieve entry with key " + key);
			return null;
		}
	}
	
	public void moveEntryTo(CacheEntry entry, Storage storage) {
		logger.debug("move entry " + entry.key + " to storage " + storage);
		remove(entry);
		storage.store(entry);
	}
	
	public void moveEntryTo(String key, Storage storage) {
		CacheEntry entry = get(key); 
		moveEntryTo(entry, storage);
	}
	
	public void moveButKeepTrackOfEntryTo(CacheEntry entry, Storage storage) {
		logger.debug("move but keep track of entry " + entry.key + " to storage " + storage);
		lruQueue.remove(entry);
		storage.store(entry);
	}
	
	public void moveButKeepTrackOfEntryTo(String key, Storage storage) {
		CacheEntry entry = get(key); 
		moveButKeepTrackOfEntryTo(entry, storage);
	}
	public long count() {
		return lruQueue.size();
	}

}
