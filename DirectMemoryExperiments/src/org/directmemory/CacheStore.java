package org.directmemory;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
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
			logger.warn("no memory pages left");
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
	
	public void disposeHeapOverflow() {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.disposeHeapOverflow");
		Split split = stopWatch.start();
		if (entriesLimit == -1) {
			split.stop();
			return;
		}
		while ((entries.size() - offHeapEntriesCount()) >= entriesLimit) {
			CacheEntry entry = removeLast();
			moveOffheap(entry);
		}
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
	
	public void disposeOverflow() {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.disposeOverflow");
		Split split = stopWatch.start();
		supervisor.disposeOverflow(this);
		split.stop();
	}
	

	
	protected void moveOffheap(CacheEntry entry) {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.moveoffheap");
		Split split = stopWatch.start();
		byte[] array = null;
		try {
			array = serializer.serialize((Serializable)entry.object, entry.object.getClass());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		disposeOverflow();

		entry.clazz = entry.object.getClass();
		entry.size = array.length;
		entry.object = null;
		ByteBuffer buf = bufferFor(entry);
		entry.position = buf.position();
		buf.put(array);
		entry.buffer = buf;
		lruQueue.remove(entry);
		lruOffheapQueue.add(entry);
		usedMemory.addAndGet(entry.size);
		entries.put(entry.key, entry);
		split.stop();
	}
	
	protected void moveInHeap(CacheEntry entry) {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.moveinheap");
		Split split = stopWatch.start();
		disposeOverflow();
		byte[] source = null; 
		source = new byte[entry.size]; 
		try {
			ByteBuffer buf = entry.buffer.duplicate();
			entry.buffer.clear();
			entry.buffer = null;
			buf.position(entry.position);
			buf.get(source);
			Object obj = serializer.deserialize(source, entry.clazz);
			entry.object = obj;
			usedMemory.addAndGet(-source.length);
			lruOffheapQueue.remove(entry);
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

	private ByteBuffer bufferFor(CacheEntry entry) {
		// look for the smaller free buffer that can contain entry
		CacheEntry slot = slots.higher(entry);
		if (slot == null) {
			slot = addMemoryPageAndGetFirstSlot();
			if (slot == null) {
				// no free memory left
				return null;
			}
		}
		synchronized (slot) {
			ByteBuffer freeBuffer = slot.buffer.duplicate();
			freeBuffer.position(slot.position);
			slot.buffer.position(slot.position);
			slot.position += entry.size;
			slot.size -= entry.size;
			return freeBuffer;
		}
	}	
	
	public CacheEntry put(String key, Object object) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.put");
		Split split = stopWatch.start();
		disposeOverflow();
		CacheEntry entry = new CacheEntry();
		entry.key = key;
		entry.object = object;
		lruQueue.add(entry);
		entries.put(key, entry);
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
		disposeOverflow();
		CacheEntry entry = getEntry(key);
		split.stop();
		if (entry == null)
			return null;
		
		return entry.object;
	}
	
	public CacheEntry remove(String key) {
        Stopwatch stopWatch = SimonManager.getStopwatch("cache.remove");
		Split split = stopWatch.start();
		disposeOverflow();
		CacheEntry entry = entries.remove(key);
		if (entry.inHeap()) {
			lruQueue.remove(entry);
		} else {
			usedMemory.addAndGet(-entry.size);
			lruOffheapQueue.remove(entry);
			slots.add(entry);
		}
		split.stop();
		return entry;
	}
	
	public CacheEntry removeLast() {
        Stopwatch stopWatch = SimonManager.getStopwatch("detail.removelast");
		Split split = stopWatch.start();
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
		return "CacheStore: {heap entries=" + heapEntriesCount() + ", off heap entries=" + offHeapEntriesCount() + ", usedMemory=" + usedMemory.get() + ", limit=" + entriesLimit + ", cacheSize=" + (pageSize * pages) +"}";
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
		showTiming(SimonManager.getStopwatch("detail.moveoffheap"));		
		showTiming(SimonManager.getStopwatch("detail.moveinheap"));		
		showTiming(SimonManager.getStopwatch("detail.removelast"));		
		showTiming(SimonManager.getStopwatch("detail.removelastoffheap"));		
		showTiming(SimonManager.getStopwatch("detail.makeroominoffheap"));		
	}
	
	public void dispose() {
		logger.warn("Off heap memory is not freed. Shout at the programmer!");
		lruQueue.clear();
		entries.clear();
		logger.info("Cache disposed");
	}
	
}
