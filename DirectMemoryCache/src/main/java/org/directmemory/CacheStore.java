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
import org.directmemory.storage.Storage;
import org.directmemory.supervisor.SimpleSupervisor;
import org.directmemory.supervisor.Supervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheStore {	
	private static Logger logger=LoggerFactory.getLogger(CacheStore.class);

	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruOffheapQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentSkipListSet<CacheEntry> slots = new ConcurrentSkipListSet<CacheEntry>();
	ConcurrentLinkedQueue<ByteBuffer> memoryPages = new ConcurrentLinkedQueue<ByteBuffer>(); 
	public int maxPages = 0;
		
	private AtomicInteger usedMemory = new AtomicInteger(0);
	
	Storage entriesOnDisk = new FileStorage();

	int entriesLimit = -1;

	public int pageSize = 0;
	
	public Serializer serializer = new StandardSerializer();
	public Supervisor supervisor = new SimpleSupervisor();

	private int defaultExpirationTime = -1;
	
	private CacheEntry slotForBuffer(ByteBuffer buffer) {
		CacheEntry newSlot = new CacheEntry();
		newSlot.position = buffer.position();
		newSlot.size = buffer.limit();
		newSlot.buffer = buffer.duplicate();
		return newSlot;
	}
	
	
	private CacheEntry addMemoryPageAndGetFirstSlot() {		
		if (memoryPages.size() < maxPages) {
			logger.info("allocating a new memory page");
			
			ByteBuffer page = ByteBuffer.allocateDirect(pageSize);
			// setup beginning and limit of buffer
			page.position(0);
			page.mark();
			page.limit(pageSize);
			page.reset();

			CacheEntry firstSlot = slotForBuffer(page);
			memoryPages.add(page);
			slots.add(firstSlot);
			return firstSlot;
		} else {
			logger.debug("no memory pages left");
			return null;
		}
	}
	
	public CacheStore (int entriesLimit, int pageSize, int maxPages) {
		logger.info("Cache initialization started");
		this.entriesLimit = entriesLimit;
		this.pageSize = pageSize;
		this.maxPages = maxPages;
		addMemoryPageAndGetFirstSlot();
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
			synchronized (entry) {
				CacheEntry newEntry = new CacheEntry();
				try {
					newEntry.array = serializer.serialize(entry.object, entry.object.getClass());
					newEntry.size = newEntry.array.length;
					
					// change buffer size and position management
					newEntry.clazz = entry.clazz();
					newEntry.key = entry.key;
					newEntry.buffer = bufferFor(newEntry);
					newEntry.position = newEntry.buffer.position();
					newEntry.buffer.put(newEntry.array);
					newEntry.array = null;

					usedMemory.addAndGet(newEntry.size);
					lruQueue.remove(entry);
					entries.put(newEntry.key, newEntry);
					lruOffheapQueue.add(newEntry);

					logger.debug("moved off heap " + newEntry.key + ": pos=" + newEntry.position + " size=" + newEntry.size);
					
				} catch (Exception e) {
					logger.debug("no room for " + entry.key + " - skipping");
					lruQueue.remove(entry);
					lruQueue.add(entry);
				}
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
		int bytes2free = usedMemory.get()-(pageSize*memoryPages.size());
		moveEntriesToDisk(bytes2free);
	}
		
	private void moveEntriesToDisk(int bytes2free) {
		int freedBytes = 0;
		while (freedBytes < bytes2free) {
			CacheEntry last = lruOffheapQueue.poll();
			if (last == null) {
				logger.warn("no lru entries in off heap slots");
				return;
			}			
			entriesOnDisk.serializer = this.serializer;
			entriesOnDisk.put(last);
			usedMemory.addAndGet(-last.size);
			CacheEntry slot = new CacheEntry();
			slot.buffer = last.buffer.duplicate();
			slot.size = last.size;
			slot.position = last.position;
			slot.buffer.position(slot.position);
			last.buffer = null;
			slots.add(slot);			
			logger.debug("created free slot of " + slot.size + " bytes");
			freedBytes += last.size;
		}
	}

//	public void oldDisposeOffHeapOverflow() {
//        Stopwatch stopWatch = SimonManager.getStopwatch("detail.disposeOffHeapOverflow");
//		Split split = stopWatch.start();
//		int freedBytes = 0;
//		int bytes2free = usedMemory.get()-(pageSize*pages);
//		
//		while (freedBytes < bytes2free) {
//			CacheEntry removedEntry = removeLastOffHeap();
//			freedBytes += removedEntry.size;
//		}
//		split.stop();
//	}
	
	public void askSupervisorForDisposal() {
		supervisor.disposeOverflow(this);
	}
	

	
//	protected void moveOffheap(CacheEntry entry) {
//        Stopwatch stopWatch = SimonManager.getStopwatch("detail.moveoffheap");
//		Split split = stopWatch.start();
//		byte[] array = null;
//		try {
//			array = serializer.serialize((Serializable)entry.object, entry.object.getClass());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		entry.clazz = entry.object.getClass();
//		entry.size = array.length;
//		entry.object = null;
//		ByteBuffer buf = bufferFor(entry);
//		entry.position = buf.position();
//		buf.put(array);
//		entry.buffer = buf;
//		lruQueue.remove(entry);
//		lruOffheapQueue.add(entry);
//		usedMemory.addAndGet(entry.size);
//		entries.put(entry.key, entry); // needed? it seems so...
////		logger.info(entry.key + " position " + entry.position + " size " + entry.size);
//		split.stop();
//	}
	
	protected void moveInHeap(CacheEntry entry) {
		byte[] source = null; 
		source = new byte[entry.size]; 
		try {
			synchronized (entry) {
				ByteBuffer buf = entry.buffer;
				buf.position(entry.position);
				buf.get(source);
				Object obj = serializer.deserialize(source, entry.clazz);
				entry.object = obj;
				entry.buffer = null;
				// change buffer size and position management
				CacheEntry freeSlot = new CacheEntry();
				freeSlot.buffer = buf;
				freeSlot.position = entry.position;
				freeSlot.buffer.position(freeSlot.position);
				freeSlot.size = entry.size;
				slots.add(freeSlot);
				logger.debug("added slot of " + freeSlot.size + " bytes");
			}

			lruOffheapQueue.remove(entry);
			
			usedMemory.addAndGet(-source.length);
			lruQueue.remove(entry);
			lruQueue.add(entry);
		} catch (UTFDataFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StreamCorruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (EOFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		split.stop();
	}
	
//	private CacheEntry forceRoomFor(CacheEntry entry) {
//        Stopwatch stopWatch = SimonManager.getStopwatch("detail.forceMakeRoomFor");
//		Split split = stopWatch.start();
//		//detail.forceMakeRoomFor
//		
//		// should remove only the larger ones
//		// and some defrag would be needed sooner or later
//		CacheEntry last = removeLastOffHeap();
//		if (last == null) {
//			logger.warn("no slots off heap available");
//			split.stop();
//			return null;
//		}
//		while (last != null && last.size <= entry.size ) { 
//			last = removeLastOffHeap();
//		}
//		split.stop();
//		return last;
//	}

	private ByteBuffer bufferFor(CacheEntry entry) {
		// look for the smaller free buffer that can contain entry
		// it fails for almost equal buffers!!!
		CacheEntry slot = slots.ceiling(entry);
		
		if (slot == null) {
			// no large enough slots left at all
			slot = addMemoryPageAndGetFirstSlot();
		}
		if (slot == null) {
			// no free slots left free the last recently used
			CacheEntry first = slots.first();
			CacheEntry last = slots.last();
			logger.debug("cannot find a free slot for entry " + entry.key + " of size " + entry.size);
			logger.debug("slots=" + slots.size() + " first size is: " + first.size + " last size=" + last.size);
//			slot = forceRoomFor(entry);
			moveEntriesToDisk(entry.size);
			slot = slots.ceiling(entry);
		}
		if (slot == null) {
			// no free memory left - I quit trying
			return null;
		}
		
		return slice(slot, entry);
	}	
	
	private ByteBuffer slice(CacheEntry slot2Slice, CacheEntry entry) {
		synchronized (slot2Slice) {
			// change buffer size and position management
			logger.debug("we removed it? " + slots.remove(slot2Slice));
			slot2Slice.size = slot2Slice.size - entry.size;
			slot2Slice.position = slot2Slice.position + entry.size;
			ByteBuffer buf = slot2Slice.buffer.duplicate();
			slot2Slice.buffer.position(slot2Slice.position);
			if (slot2Slice.size > 0) {
				slots.add(slot2Slice);
				logger.debug("added sliced slot of " + slot2Slice.size + " bytes");
			} else {
				logger.debug("size of slot is zero bytes");
				logger.debug("and is in slots? " + slots.contains(slot2Slice));
				if (logger.isDebugEnabled()) {
					logger.debug(toString());
				}
			}
			return buf;
		}
		
	}

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
			moveInHeap(entry);
		} else if (entry.onDisk()) {
			entriesOnDisk.remove(entry);
			lruQueue.add(entry);
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
		if (entry.inHeap()) {
			lruQueue.remove(entry);
		} else {
			usedMemory.addAndGet(-entry.size);
			lruOffheapQueue.remove(entry);
			slots.add(entry);
			logger.debug("added slot of " + entry.size + " bytes");
		}
		askSupervisorForDisposal();
		return entry;
	}
	
	public CacheEntry removeLast() {
		CacheEntry next = lruQueue.peek();
		if (next.size > slots.last().size) {
			return null;
		}
		CacheEntry last = lruQueue.poll();
		entries.remove(last.key);
		return last;
	}
	
	public CacheEntry removeLastOffHeap() {
		CacheEntry last = lruOffheapQueue.poll();
		if (last == null) {
			logger.warn("no lru from off heap");
			return null;
		}		
		usedMemory.addAndGet(-last.size);
		entries.remove(last.key);
		slots.add(last);
		logger.debug("added slot of " + last.size + " bytes");
		return last;
	}
	
	public int heapEntriesCount() {
		return lruQueue.size();
	}
	
	public int offHeapEntriesCount() {
		return lruOffheapQueue.size();
	}
	
	public int usedMemory() {
		return usedMemory.get();
	}
	
	@Override
	public String toString() {
		final String crLf = "\r\n";
		return "CacheStore stats: " + 
				"{ " + crLf + 
				"   entries: " + entries.size() + crLf + 
				"   heap: " + heapEntriesCount() + "/" + entriesLimit + crLf +  
				"   memory: " + usedMemory.get() + "/" + (pageSize * memoryPages.size()) + crLf + 
				"   in " + offHeapEntriesCount() + " off-heap" + " and " + onDiskEntriesCount() + " on disk entries" + crLf + 
				"   free slots: " + slots.size() + " first size is: " + slots.first().size + " last size=" + slots.last().size + crLf + 
				"}" 
			;
	}
	
	private long onDiskEntriesCount() {
		return entriesOnDisk.count();
	}

	public static void displayTimings() {
		// replaced by a dedicated aspect
//		showTiming(SimonManager.getStopwatch("cache.put"));
//		showTiming(SimonManager.getStopwatch("cache.get"));
//		showTiming(SimonManager.getStopwatch("cache.remove"));
//		showTiming(SimonManager.getStopwatch("serializer.PSSerialize"));
//		showTiming(SimonManager.getStopwatch("serializer.PSDeserialize"));
//		showTiming(SimonManager.getStopwatch("serializer.javaSerialize"));
//		showTiming(SimonManager.getStopwatch("serializer.javaDeserialize"));
//		showTiming(SimonManager.getStopwatch("detail.disposeOverflow"));		
//		showTiming(SimonManager.getStopwatch("detail.disposeHeapOverflow"));		
//		showTiming(SimonManager.getStopwatch("detail.disposeOffHeapOverflow"));		
//		showTiming(SimonManager.getStopwatch("detail.disposeExpired"));		
//		showTiming(SimonManager.getStopwatch("detail.moveinheap"));		
//		showTiming(SimonManager.getStopwatch("detail.moveoffheap"));		
//		showTiming(SimonManager.getStopwatch("detail.move2disk"));		
//		showTiming(SimonManager.getStopwatch("detail.movefromdisk"));		
//		showTiming(SimonManager.getStopwatch("detail.removelast"));		
//		showTiming(SimonManager.getStopwatch("detail.removelastoffheap"));		
//		showTiming(SimonManager.getStopwatch("detail.forceMakeRoomFor"));		
	}
	
	public void reset() {
		lruQueue.clear();
		entries.clear();
		slots.clear();
		for (Iterator<ByteBuffer> iterator = memoryPages.iterator(); iterator.hasNext();) {
			ByteBuffer page = iterator.next();
			slots.add(slotForBuffer(page));
		}
		logger.info("Cache reset");
		logger.info(toString());
	}
	
	public static int MB(double mega) {
		return (int)mega * 1024 * 1024;
	}
	
	public static int KB(double kilo) {
		return (int)kilo * 1024;
	}
}
