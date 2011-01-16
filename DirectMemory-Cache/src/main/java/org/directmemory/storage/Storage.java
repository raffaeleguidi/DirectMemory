package org.directmemory.storage;

import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.directmemory.cache.CacheEntry;
import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Storage {
	protected static Logger logger=LoggerFactory.getLogger(Storage.class);
	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	
	public Serializer serializer = new StandardSerializer();
	
	// not needed and probably not desireable
	// it would be better to work through the CacheManager
	//public Supervisor supervisor;
	public Storage next;
	public Storage first;
	
	protected int entriesLimit = -1;
	
	public int entriesLimit() {
		return entriesLimit;
	}
	
	public Map<String, CacheEntry> entries() {
		return entries;
	}


	protected abstract boolean moveIn(CacheEntry entry);
	protected abstract boolean moveToHeap(CacheEntry entry);

	public boolean put(CacheEntry entry) {
		if (entry == null || entry.key == null) {
			logger.warn("not valid or null entry");
		}
		if (moveIn(entry)) {		
			logger.debug("stored entry " + entry.key);
			entries.put(entry.key, entry);
			touch(entry);
			return true;
		} else {
			logger.debug("failed to store entry " + entry.key);
			return false;
		}
	}

	public CacheEntry remove(CacheEntry entry) {
		return delete(entry.key);
	}
	
	public CacheEntry delete(String key) {
		logger.debug("remove entry with key " + key);
		CacheEntry entry = entries.remove(key);
		lruQueue.remove(entry);
		return entry;
	}
	
	public CacheEntry get(String key) {
		CacheEntry entry = entries.get(key);
		
		if (entry != null) {
			if (moveToHeap(entry)) {
				if (first == null || first == this) {
					touch(entry);
				} else {
					moveEntryTo(entry, first);
				}
				logger.debug("retrieve entry with key " + key);
				return entry;
			} else {
				logger.debug("failed to retrieve entry with key " + key);
				return null;
			}
		} else {
			if (next != null) {
				entry = next.get(key);
			}
		}
		return entry;
	}
	
	public void moveEntryTo(CacheEntry entry, Storage storage) {
		logger.debug("move entry " + entry.key + " to storage " + storage);
		remove(entry);
		storage.put(entry);
		entry.setStorage(storage);
	}
	
	public void moveEntryTo(String key, Storage storage) {
		CacheEntry entry = get(key); 
		moveEntryTo(entry, storage);
	}
	
//	public void moveButKeepTrackOfEntryTo(CacheEntry entry, Storage storage) {
//		logger.debug("move but keep track of entry " + entry.key + " to storage " + storage);
//		lruQueue.remove(entry);
//		storage.put(entry);
//		entry.setStorage(storage);
//	}
//	
//	public void moveButKeepTrackOfEntryTo(String key, Storage storage) {
//		CacheEntry entry = get(key); 
//		moveButKeepTrackOfEntryTo(entry, storage);
//	}
	public long count() {
		return entries.size();
	}
	public void dispose() {
		lruQueue.clear();
		entries.clear();
	}
	public CacheEntry removeLast() {
		CacheEntry last = lruQueue.peek();
		logger.debug("remove last entry entry with key " + last.key);
		if (first != null && first != this) {
			logger.debug("but keeping track of it");
			entries.remove(last.key);
			last.setStorage(null);
		}
		return last;
	}
	public void overflowToNext() {
		while (overflow() > 0) {
			CacheEntry last = lruQueue.peek();
			if (last == null) {
				logger.debug("no entries to discard");
				return;
			}
			if (next != null) {
//				if (first == null || first == this) {
//					moveButKeepTrackOfEntryTo(last, next);
//					logger.debug("moved but keeping track of " + last.key + " to " + next.toString());
//				} else {
					moveEntryTo(last, next);
					logger.debug("moved " + last.key + " to " + next.toString());
//				}
			} else {
				remove(last);
				logger.debug("next storage is null: discarded " + last.key + "");
			}
		}
	}
	public int overflow() {
		if (entriesLimit == -1)
			return 0;
		
		return entries.size() - entriesLimit;
	}
	
	public CacheEntry peek() {
		return lruQueue.peek();
	}
	
	public void moveOut(CacheEntry entry) {
		moveToHeap(entry);
		if (this.first == null) {
			entry.setStorage(this);
		} else {
			entry.setStorage(this.first);
		}
		remove(entry);
	}
	
	@Override
	public String toString() {
		return new Formatter()
					.format(
							"Storage: entries %1d", 
							entries.size()
							)
					.toString();
	}
	
	public String performance() {
		return null;
	}
	
	public void touch(CacheEntry entry) {
		lruQueue.remove(entry);
		lruQueue.add(entry);
	}
}
