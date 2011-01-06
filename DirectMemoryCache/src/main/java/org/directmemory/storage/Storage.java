package org.directmemory.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.directmemory.CacheEntry;
import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
import org.directmemory.supervisor.Supervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Storage {
	protected static Logger logger=LoggerFactory.getLogger(Storage.class);
	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	public Serializer serializer = new StandardSerializer();
	
	public Supervisor supervisor;
	public Storage next;
	
	protected abstract boolean moveIn(CacheEntry entry);
	public abstract boolean moveToHeap(CacheEntry entry);

	public boolean put(CacheEntry entry) {
		if (entry.key == null) {
			logger.warn("why an entry with a null key?!?");
		}
		if (moveIn(entry)) {		
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
		return delete(entry.key);
	}
	
	public boolean delete(String key) {
		logger.debug("remove entry with key " + key);
		CacheEntry entry = entries.remove(key);
		return lruQueue.remove(entry);
	}
	
	public CacheEntry get(String key) {
		CacheEntry entry = entries.get(key);
		if (moveToHeap(entry)) {
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
		storage.moveIn(entry);
	}
	
	public void moveEntryTo(String key, Storage storage) {
		CacheEntry entry = get(key); 
		moveEntryTo(entry, storage);
	}
	
	public void moveButKeepTrackOfEntryTo(CacheEntry entry, Storage storage) {
		logger.debug("move but keep track of entry " + entry.key + " to storage " + storage);
		lruQueue.remove(entry);
		storage.moveIn(entry);
	}
	
	public void moveButKeepTrackOfEntryTo(String key, Storage storage) {
		CacheEntry entry = get(key); 
		moveButKeepTrackOfEntryTo(entry, storage);
	}
	public long count() {
		return lruQueue.size();
	}
	public void reset() {
		lruQueue.clear();
		entries.clear();
	}
	public CacheEntry removeLast() {
		CacheEntry last = lruQueue.poll();
		entries.remove(last.key);
		return last;
	}
	public void overflowToNext() {
		do {
			CacheEntry last = removeLast();
			if (next != null) {
				moveEntryTo(last, next);
				logger.debug("moved " + last.key + " to " + next.toString());
			} else {
				logger.debug("discarded " + last.key);
			}
		} while (overflow() > 0);
	}
	abstract int overflow();

}
