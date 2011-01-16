package org.directmemory.cache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.directmemory.measures.Ram;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.directmemory.serialization.Serializer;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.HeapStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.OrientDBStorage;
import org.directmemory.storage.Storage;
import org.directmemory.supervisor.SimpleSupervisor;
import org.directmemory.supervisor.Supervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheManager {	
	private static Logger logger=LoggerFactory.getLogger(CacheManager.class);

	ConcurrentLinkedQueue<Storage> storages = new ConcurrentLinkedQueue<Storage>();

//	HeapStorage heapStore;
//	Storage offHeapStore;
//	Storage diskStore;
	
	private Serializer serializer;
	private Supervisor supervisor;

	private long defaultExpirationTime = -1;
	
	public CacheManager (int entriesLimit, int pageSize, int maxPages) {
		logger.info("Cache initialization started");
		addStorage(new HeapStorage(entriesLimit));
		addStorage(new OffHeapStorage(pageSize, maxPages));
		addStorage(new OrientDBStorage());
		setSerializer(new ProtoStuffSerializer(Ram.Kb(10)));
		setSupervisor(new SimpleSupervisor());
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
	}
	
	public void askSupervisorForDisposal() {
		supervisor.disposeOverflow(this);
	}

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
		// TODO: got to be changed in order to accomplish with the storage chain change
		firstStorage.put(entry);
		askSupervisorForDisposal();
		return entry;
	} 
	
	public CacheEntry getEntry(String key) {
		// TODO: got to be changed in order to accomplish with the storage chain change
		CacheEntry entry = firstStorage.get(key);
		if (entry == null) {
			return null;
		} else if (entry.expired()) {
			// does remove work this way?
			remove(key);
			return null;
		} else if (!entry.inHeap()) {
			entry.getStorage().moveOut(entry);
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
		// TODO: got to be changed in order to accomplish with the storage chain change
		CacheEntry entry = firstStorage.delete(key);
		if (!entry.inHeap()) {
			entry.getStorage().delete(key);
		}
		askSupervisorForDisposal();
		return entry;
	}
	
	@Override
	public String toString() {
		final String crLf = "\r\n";
		StringBuffer sb = new StringBuffer();

		sb.append("CacheStore stats: \r\n{\r\n");
		sb.append("   Total entries: ");
		sb.append(count());
		sb.append("\r\n");
		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			sb.append("   ");
			sb.append(storage.toString());
			sb.append(crLf);			
		}
		sb.append("}");
		sb.append(crLf);
		return sb.toString();
	}
	
	public long heapEntriesCount() {
		return firstStorage.count();
	}
	
	private long countEntriesInStorageClass(@SuppressWarnings({"rawtypes", "unchecked"}) Class clazz) {
		long result = 0;
		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			if (storage.getClass().equals(clazz)) { 
				result += storage.count();
			}			
		}
		return result;
	}
	
	public long count() {
		long result = 0;
		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			result += storage.count();
		}
		return result;
	}

	public long offHeapEntriesCount() {
		return countEntriesInStorageClass(OffHeapStorage.class);
	}
	
	public long usedMemory() {
		long result = 0;
		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			if (storage.getClass().equals(OffHeapStorage.class)) { 
				result += ((OffHeapStorage)storage).usedMemory();
			}			
		}
		return result;
	}
	
	public long onDiskEntriesCount() {
		return countEntriesInStorageClass(FileStorage.class);
	}

	public long onDBEntriesCount() {
		return countEntriesInStorageClass(OrientDBStorage.class);
	}

	public static void displayTimings() {
		// replaced by a dedicated aspect
		// do we need this as a hook or can it be deleted?
	}
	
	public static String getTimings() {
		// replaced by a dedicated aspect
		// do we need this as a hook or can it be deleted?
		return null;
	}
	
	public void dispose() {

		Iterator<Storage> iter = storages.iterator();
		while (iter.hasNext()) {
			Storage storage = iter.next();
			storage.dispose();
		}
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
	}

	public Serializer getSerializer() {
		return serializer;
	}

}
