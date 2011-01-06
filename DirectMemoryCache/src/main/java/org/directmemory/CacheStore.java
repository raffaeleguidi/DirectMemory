package org.directmemory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.Storage;
import org.directmemory.supervisor.SimpleSupervisor;
import org.directmemory.supervisor.Supervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jndi.ldap.EntryChangeResponseControl;

public class CacheStore {	
	private static Logger logger=LoggerFactory.getLogger(CacheStore.class);

	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruOffheapQueue = new ConcurrentLinkedQueue<CacheEntry>();

	Storage entriesOffHeap;
	Storage entriesOnDisk;

	int entriesLimit = -1;
	
	private Serializer serializer;
	private Supervisor supervisor;

	private int defaultExpirationTime = -1;
	
	public CacheStore (int entriesLimit, int pageSize, int maxPages) {
		logger.info("Cache initialization started");
		this.entriesLimit = entriesLimit;
		entriesOffHeap = new OffHeapStorage(pageSize, maxPages);
		entriesOnDisk = new FileStorage();
		setSerializer(new StandardSerializer());
		setSupervisor(new SimpleSupervisor());
		entriesOffHeap.next = entriesOnDisk;
		logger.info("Cache initialization ok");
	}
	
	public void disposeExpired() {
		for (Iterator<CacheEntry> iterator = entries.values().iterator(); iterator.hasNext();) {
			CacheEntry entry = iterator.next();
			if (entry.expired()) {
				remove(entry.key);
			}
		}
	}
	
	private void moveEntriesOffHeap(int entries2move) {
		if (entries2move < 1) return;
		for (int i = 0; i < entries2move; i++) {
			CacheEntry entry = lruQueue.peek();
			if (entriesOffHeap.put(entry)) {
				lruQueue.remove(entry);
				logger.debug("moved off heap " + entry.key + ": pos=" + entry.position + " size=" + entry.size);
			} else {
				logger.debug("no room for " + entry.key + " - skipping");
			}
		}
	}
	
	public void disposeHeapOverflow() {
		if (entriesLimit == -1) {
			return;
		}
		moveEntriesOffHeap(lruQueue.size() - entriesLimit);
	}
	
	public void disposeOffHeapOverflow() {
		//int bytes2free = usedMemory.get()-(pageSize*memoryPages.size());
		// totally nonsense: choose a strategy
		//moveEntriesToDisk(bytes2free);
	}
		
	private void moveEntriesToDisk(int bytes2free) {
		int freedBytes = 0;
		while (freedBytes < bytes2free) {
			CacheEntry last = lruOffheapQueue.poll();
			if (last == null) {
				logger.warn("no lru entries in off heap slots");
				return;
			}			
			entriesOffHeap.moveEntryTo(last, entriesOnDisk);
			freedBytes += last.size;
		}
	}
	
	public void askSupervisorForDisposal() {
		getSupervisor().disposeOverflow(this);
	}
	
//	protected void moveInHeap(CacheEntry entry) {
//		if (entriesOffHeap.moveToHeap(entry)) {
//			lruQueue.remove(entry);
//			lruQueue.add(entry);
//		}
//	}

	public CacheEntry put(String key, Object object) {
		return put(key, object, defaultExpirationTime);
	}
	
	public CacheEntry put(String key, Object object, int expiresIn) {
		CacheEntry entry = new CacheEntry();
		entry.key = key;
		entry.object = object;
		entry.expiresIn(expiresIn);
		entries.put(key, entry);
		lruQueue.add(entry);
		askSupervisorForDisposal();
		return entry;
	} 
	
	public CacheEntry getEntry(String key) {
		CacheEntry entry = entries.get(key);
		if (entry == null) {
			return null;
		} else if (entry.expired()) {
			remove(key);
			return null;
		} else if (entry.inHeap()) {
			lruQueue.remove(entry);
			lruQueue.add(entry);
		} else if (entry.offHeap()) {
			if (entriesOffHeap.moveToHeap(entry)) {
				lruQueue.remove(entry);
				lruQueue.add(entry);
			}
		} else if (entry.onDisk()) {
			if (entriesOnDisk.moveToHeap(entry)) {
				lruQueue.remove(entry);
				lruQueue.add(entry);
			}
		}
		return entry;
	}
	
	public Object get(String key) {
		CacheEntry entry = getEntry(key);
		askSupervisorForDisposal();
		if (entry == null) {
			return null;
		} else {
			return entry.object;
		}
	}
	
	public CacheEntry remove(String key) {
		CacheEntry entry = entries.remove(key);
		if (entry == null) {
			return null;
		} else if (entry.inHeap()) {
			lruQueue.remove(entry);
		} else if (entry.offHeap()){
			entriesOffHeap.delete(key);
		} else if (entry.onDisk()) {
			entriesOnDisk.delete(key);
		}
		askSupervisorForDisposal();
		return entry;
	}
	
	public CacheEntry removeLast() {
		CacheEntry next = lruQueue.peek();
		// todo: do we need it? put it somewhere else
		if (next.size > ((OffHeapStorage)entriesOffHeap).slots().last().size) {
			return null;
		}
		CacheEntry last = lruQueue.poll();
		entries.remove(last.key);
		return last;
	}
	
	public CacheEntry removeLastOffHeap() {
		return entriesOffHeap.removeLast();
	}
	
	public int heapEntriesCount() {
		return lruQueue.size();
	}
	
	public long offHeapEntriesCount() {
		return entriesOffHeap.count();
	}
	
	public int usedMemory() {
		return ((OffHeapStorage)entriesOffHeap).usedMemory();
	}
	
	@Override
	public String toString() {
		final String crLf = "\r\n";
		return "CacheStore stats: " + 
				"{ " + crLf + 
				"   entries: " + entries.size() + crLf + 
				"   heap: " + heapEntriesCount() + "/" + entriesLimit + crLf +  
				"   memory: " + usedMemory() + "/" + ((OffHeapStorage)entriesOffHeap).capacity() + crLf + 
				"   in " + offHeapEntriesCount() + " off-heap" + " and " + onDiskEntriesCount() + " on disk entries" + crLf + 
				"   free slots: " + ((OffHeapStorage)entriesOffHeap).slots().size() + " first size is: " + ((OffHeapStorage)entriesOffHeap).slots().first().size + " last size=" + ((OffHeapStorage)entriesOffHeap).slots().last().size + crLf + 
				"}" 
			;
	}
	
	public long onDiskEntriesCount() {
		return entriesOnDisk.count();
	}

	public static void displayTimings() {
		// replaced by a dedicated aspect	
	}
	
	public void reset() {
		lruQueue.clear();
		entries.clear();
		entriesOffHeap.reset();
		entriesOnDisk.reset();
		logger.info("Cache reset - " + toString());
	}
	
	public static int MB(double mega) {
		return (int)mega * 1024 * 1024;
	}
	
	public static int KB(double kilo) {
		return (int)kilo * 1024;
	}

	public void setSupervisor(Supervisor supervisor) {
		entriesOnDisk.supervisor = supervisor;
		entriesOffHeap.supervisor = supervisor;
		this.supervisor = supervisor;
	}

	public Supervisor getSupervisor() {
		return supervisor;
	}

	public void setSerializer(Serializer serializer) {
		entriesOffHeap.serializer = serializer;
		entriesOnDisk.serializer = serializer;
		this.serializer = serializer;
	}

	public Serializer getSerializer() {
		return serializer;
	}
}
