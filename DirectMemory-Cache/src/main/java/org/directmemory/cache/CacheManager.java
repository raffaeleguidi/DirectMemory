package org.directmemory.cache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.HeapStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.Storage;
import org.directmemory.supervisor.SimpleSupervisor;
import org.directmemory.supervisor.Supervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheManager {	
	private static Logger logger=LoggerFactory.getLogger(CacheManager.class);

	ConcurrentLinkedQueue<Storage> storages = new ConcurrentLinkedQueue<Storage>();

	HeapStorage heapStore;
	Storage offHeapStore;
	Storage diskStore;
	
	private Serializer serializer;
	private Supervisor supervisor;

	private long defaultExpirationTime = -1;
	
	public CacheManager (int entriesLimit, int pageSize, int maxPages) {
		logger.info("Cache initialization started");
		heapStore = new HeapStorage(entriesLimit);
		offHeapStore = new OffHeapStorage(pageSize, maxPages);
		diskStore = new FileStorage();
		heapStore().next = offHeapStore;
		offHeapStore.next = diskStore;
		offHeapStore.first = heapStore;
		diskStore.first = heapStore;
		setSerializer(new StandardSerializer());
		setSupervisor(new SimpleSupervisor());
		offHeapStore.next = diskStore;
		logger.info("Cache initialization ok");
	}
	
	public CacheManager() {
		// do nothing
	}

	private Storage lastStorage = null;
	private Storage firstStorage = null;
	
	public void addStorage(Storage storage) {
		if (lastStorage == null) {
			lastStorage = storage;
			firstStorage = storage;
		} else {
			lastStorage.next = storage;
			lastStorage.first = firstStorage;
			lastStorage = storage;
		}
		if (serializer != null)
			storage.serializer = serializer;
		
		storages.add(storage);
	}
	
	
	public void disposeExpired() {
		if (heapStore != null) {
			for (Iterator<CacheEntry> iterator = heapStore.entries().values().iterator(); iterator.hasNext();) {
				CacheEntry entry = iterator.next();
				if (entry.expired()) {
					remove(entry.key);
				}
			}
		}
		if (storages.size() > 0) {
			Storage heapStore = storages.peek();
			for (Iterator<CacheEntry> iterator = heapStore.entries().values().iterator(); iterator.hasNext();) {
				CacheEntry entry = iterator.next();
				if (entry.expired()) {
					remove(entry.key);
				}
			}
		}
		
	}
	
	public void disposeOverflow() {
		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			storage.overflowToNext();
		}
		if (heapStore != null)
			heapStore().overflowToNext();
		if (offHeapStore != null)
			offHeapStore().overflowToNext();
		if (diskStore != null)
			diskStore().overflowToNext();
	}
	
	public void askSupervisorForDisposal() {
		supervisor.disposeOverflow(this);
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
	
	
	
	
	public long getDefaultExpirationTime() {
		return defaultExpirationTime;
	}

	public void setDefaultExpirationTime(long defaultExpirationTime) {
		this.defaultExpirationTime = defaultExpirationTime;
	}

	public CacheEntry put(String key, Object object, long expiresIn) {
		CacheEntry entry = new CacheEntry();
		entry.key = key;
		entry.object = object;
		entry.expiresIn(expiresIn);
		heapStore.put(entry);
		askSupervisorForDisposal();
		return entry;
	} 
	
	public CacheEntry getEntry(String key) {
		CacheEntry entry = heapStore.get(key);
		if (entry == null) {
			return null;
		} else if (entry.expired()) {
			remove(key);
			return null;
		} else if (entry.inHeap()) {
			// do nothing
			// or: heapStore.touch(entry);
		} else if (entry.offHeap()) {
			offHeapStore.moveOut(entry);
		} else if (entry.onDisk()) {
			diskStore.moveOut(entry);
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
		CacheEntry entry = heapStore.delete(key);
		if (entry == null) {
			return null;
		} else if (entry.inHeap()) {
			//lruQueue.remove(entry);
			// do nothing
		} else if (entry.offHeap()){
			offHeapStore.delete(key);
		} else if (entry.onDisk()) {
			diskStore.delete(key);
		}
		askSupervisorForDisposal();
		return entry;
	}
	
	public long heapEntriesCount() {
		return heapStore.count();
	}
	
	public long offHeapEntriesCount() {
		return offHeapStore.count();
	}
	
	public long usedMemory() {
		return ((OffHeapStorage)offHeapStore).usedMemory();
	}
	
	@Override
	public String toString() {
		final String crLf = "\r\n";
		StringBuffer sb = new StringBuffer();
		
		sb.append("CacheStore stats: \r\n{\r\n");
		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			sb.append("   ");
			sb.append(storage.toString());
			sb.append(crLf);			
		}
		sb.append("{");
		sb.append(crLf);
		return sb.toString();
//		
//		return "CacheStore stats: " + 
//				"{ " + crLf + 
//				"   entries: " + heapStore.entries().size() + crLf + 
//				"   heap: " + heapStore().count() + "/" + heapStore.entriesLimit() + crLf +  
//				"   memory: " + usedMemory() + "/" + ((OffHeapStorage)offHeapStore).capacity() + crLf + 
//				"   in " + offHeapEntriesCount() + " off-heap" + " and " + onDiskEntriesCount() + " on disk entries" + crLf + 
//				"   free slots: " + ((OffHeapStorage)offHeapStore).slots().size() + " first size is: " + ((OffHeapStorage)offHeapStore).slots().first().size + " last size=" + ((OffHeapStorage)offHeapStore).slots().last().size + crLf + 
//				"}" 
//			;
	}
	
	public long onDiskEntriesCount() {
		return diskStore.count();
	}

	public static void displayTimings() {
		// replaced by a dedicated aspect	
	}
	
	public void reset() {
		if (heapStore != null)
			heapStore.reset();
		if (offHeapStore != null)
			offHeapStore.reset();
		if (diskStore != null)
			diskStore.reset();
		logger.info("Cache reset - " + toString());
	}
	
	public void setSupervisor(Supervisor supervisor) {
		this.supervisor = supervisor;
	}

	public Supervisor getSupervisor() {
		return supervisor;
	}

	public void setSerializer(Serializer serializer) {
		this.serializer = serializer;
		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			storage.serializer = serializer;
		}
		if (offHeapStore != null)
			offHeapStore.serializer = serializer;
		if (diskStore != null)
			diskStore.serializer = serializer;
	}

	public Serializer getSerializer() {
		return serializer;
	}

	public Storage heapStore() {
		return heapStore;
	}

	public Storage offHeapStore() {
		return offHeapStore;
	}
	public Storage diskStore() {
		return diskStore;
	}
}
