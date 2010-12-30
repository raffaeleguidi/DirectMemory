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
import org.directmemory.supervisor.SimpleSupervisor;
import org.directmemory.supervisor.Supervisor;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheStore {	
	private static Logger logger=LoggerFactory.getLogger(CacheStore.class);

	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruOffheapQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentSkipListSet<CacheEntry> slots = new ConcurrentSkipListSet<CacheEntry>();
	private AtomicInteger usedMemory = new AtomicInteger(0);

	int entriesLimit = -1;

	public int pageSize = 0;
	public int pages = 0;
	public int maxPages = 0;
	
	public Serializer serializer = new StandardSerializer();
	public Supervisor supervisor = new SimpleSupervisor();
	
	
	private CacheEntry addMemoryPageAndGetFirstSlot() {		
		if (pages < maxPages) {
			logger.info("allocating a new memory page");
			this.pages++;
			CacheEntry firstSlot = new CacheEntry();
			firstSlot.position = 0;
			firstSlot.size = pageSize;
			firstSlot.buffer = ByteBuffer.allocateDirect(pageSize);
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
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.disposeExpired");
		Split split = stopWatch.start();
		
		for (Iterator<CacheEntry> iterator = entries.values().iterator(); iterator.hasNext();) {
			CacheEntry entry = iterator.next();
			if (entry.expired()) {
				remove(entry.key);
			}
		}
		
		split.stop();
	}	
	
	private void moveItemsOffHeap(int entries2move) {
		if (entries2move < 1) return;
		
		for (int i = 0; i < entries2move; i++) {
			CacheEntry entry = lruQueue.peek();
			synchronized (entry) {
				CacheEntry newEntry = new CacheEntry();
				try {
					newEntry.array = serializer.serialize(entry.object, entry.object.getClass());
					newEntry.size = newEntry.array.length;
					newEntry.clazz = entry.object.getClass();
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
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.disposeHeapOverflow");
		Split split = stopWatch.start();
		
		moveItemsOffHeap(lruQueue.size() - entriesLimit);

		split.stop();
	}
	
	public void disposeOffHeapOverflow() {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.disposeOffHeapOverflow");
		Split split = stopWatch.start();
		int freedBytes = 0;
		int bytes2free = usedMemory.get()-(pageSize*pages);
		
		while (freedBytes < bytes2free) {
			CacheEntry removedEntry = removeLastOffHeap();
			freedBytes += removedEntry.size;
		}
		split.stop();
	}
	
	public void askSupervisorToDisposeOverflow() {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.disposeOverflow");
		Split split = stopWatch.start();
		supervisor.disposeOverflow(this);
		split.stop();
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
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.moveinheap");
		Split split = stopWatch.start();
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
				CacheEntry freeSlot = new CacheEntry();
				freeSlot.buffer = buf;
				freeSlot.position = entry.position;
				freeSlot.buffer.position(freeSlot.position);
				freeSlot.size = entry.size;
				slots.add(freeSlot);
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
		split.stop();
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
		CacheEntry slot = slots.higher(entry);
		
		if (slot == null) {
			// no free slots left free the last recently used
			CacheEntry first = slots.first();
			CacheEntry last = slots.last();
			logger.debug("cannot find a free slot for entry " + entry.key + " of size " + entry.size);
			logger.debug("slots=" + slots.size() + " first size is: " + first.size + " last size=" + last.size);
//			slot = forceRoomFor(entry);
		}
		if (slot == null) {
			// no large enough slots left at all
			slot = addMemoryPageAndGetFirstSlot();
		}
		if (slot == null) {
			// no free memory left - I quit trying
			return null;
		}
		
		return slice(slot, entry);
	}	
	
	private ByteBuffer slice(CacheEntry slot2Slice, CacheEntry entry) {
		synchronized (slot2Slice) {
			slot2Slice.size = slot2Slice.size - entry.size;
			slot2Slice.position = slot2Slice.position + entry.size;
			ByteBuffer buf = slot2Slice.buffer.duplicate();
			slot2Slice.buffer.position(slot2Slice.position);
			return buf;
		}
		
	}

	public CacheEntry put(String key, Object object) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.put");
		Split split = stopWatch.start();
		CacheEntry entry = new CacheEntry();
		entry.key = key;
		entry.object = object;
		entries.put(key, entry);
		lruQueue.add(entry);
		askSupervisorToDisposeOverflow();
		split.stop();
		return entry;
	}
	
	public CacheEntry getEntry(String key) {
		CacheEntry entry = entries.get(key);
		if (entry == null) {
			return null;
		}
		if (entry.expired()) {
			remove(key);
			return null;
		}
		if (!entry.inHeap()) {
			moveInHeap(entry);
		} else {
			lruQueue.remove(entry);
			lruQueue.add(entry);
		}

		return entry;
	}
	
	public Object get(String key) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.get");
		Split split = stopWatch.start();
		CacheEntry entry = getEntry(key);
		askSupervisorToDisposeOverflow();
		split.stop();
		if (entry == null) {
			return null;
		} else {
			return entry.object;
		}
	}
	
	public CacheEntry remove(String key) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.remove");
		Split split = stopWatch.start();
		CacheEntry entry = entries.remove(key);
		if (entry.inHeap()) {
			lruQueue.remove(entry);
		} else {
			usedMemory.addAndGet(-entry.size);
			lruOffheapQueue.remove(entry);
			slots.add(entry);
		}
		askSupervisorToDisposeOverflow();
		split.stop();
		return entry;
	}
	
	public CacheEntry removeLast() {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.removelast");
		Split split = stopWatch.start();
		CacheEntry next = lruQueue.peek();
		if (next.size > slots.last().size) {
			split.stop();
			return null;
		}
		CacheEntry last = lruQueue.poll();
		entries.remove(last.key);
		split.stop();
		return last;
	}
	
	public CacheEntry removeLastOffHeap() {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.removelastoffheap");
		Split split = stopWatch.start();
		CacheEntry last = lruOffheapQueue.poll();
		if (last == null) {
			logger.warn("no lru from off heap");
			split.stop();
			return null;
		}
		
		usedMemory.addAndGet(-last.size);
		entries.remove(last.key);
		slots.add(last);
		split.stop();
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
		return "CacheStore: {heap entries=" + heapEntriesCount() + "/" + entriesLimit + 
				" memory=" + usedMemory.get() + "/" + (pageSize * pages) +  
				" in " + offHeapEntriesCount() + " off heap entries}";
	}
	
	private static void showTiming(Stopwatch sw) {
		double average = ((double)sw.getTotal() / (double)sw.getCounter() /1000000);
		logger.info(sw.getName() + " " + sw.getCounter() + " hits - average " + average + " - max active:" + sw.getMaxActive() + " total time " + (sw.getTotal()/1000000));
	}
	
	public static void displayTimings() {
		showTiming(SimonManager.getStopwatch("cache.put"));
		showTiming(SimonManager.getStopwatch("cache.get"));
		showTiming(SimonManager.getStopwatch("cache.remove"));
		showTiming(SimonManager.getStopwatch("serializer.PSSerialize"));
		showTiming(SimonManager.getStopwatch("serializer.PSDeserialize"));
		showTiming(SimonManager.getStopwatch("serializer.javaSerialize"));
		showTiming(SimonManager.getStopwatch("serializer.javaDeserialize"));
		showTiming(SimonManager.getStopwatch("detail.disposeOverflow"));		
		showTiming(SimonManager.getStopwatch("detail.disposeHeapOverflow"));		
		showTiming(SimonManager.getStopwatch("detail.disposeOffHeapOverflow"));		
		showTiming(SimonManager.getStopwatch("detail.disposeExpired"));		
		showTiming(SimonManager.getStopwatch("detail.moveoffheap"));		
		showTiming(SimonManager.getStopwatch("detail.moveinheap"));		
		showTiming(SimonManager.getStopwatch("detail.removelast"));		
		showTiming(SimonManager.getStopwatch("detail.removelastoffheap"));		
		showTiming(SimonManager.getStopwatch("detail.forceMakeRoomFor"));		
	}
	
	public void dispose() {
		logger.warn("Off heap memory is not freed. Shout at the programmer!");
		lruQueue.clear();
		entries.clear();
		logger.info("Cache disposed");
	}
	
	public static int MB(int size) {
		return size * 1024 * 1024;
	}
	
}
