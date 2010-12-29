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
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;

public class CacheStore {
	ByteBuffer buffer;
	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruOffheapQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentSkipListSet<CacheEntry> slots = new ConcurrentSkipListSet<CacheEntry>();
	private AtomicInteger usedMemory = new AtomicInteger(0);
//	private AtomicInteger offHeapEntries = new AtomicInteger(0);
	int entriesLimit = -1;
	
	public Serializer serializer = null;
	
	public CacheStore (int entriesLimit, int cacheSize) {
		serializer = new StandardSerializer(); 
		this.entriesLimit = entriesLimit;
		this.buffer = ByteBuffer.allocateDirect(cacheSize);
		CacheEntry firstSlot = new CacheEntry();
		firstSlot.position = 0;
		firstSlot.size = cacheSize;
		firstSlot.buffer = buffer.duplicate();
		slots.add(firstSlot);
	}
	
	public CacheStore (int entriesLimit, int cacheSize, Serializer serializer) {
		this.serializer = serializer; 
		this.entriesLimit = entriesLimit;
		this.buffer = ByteBuffer.allocateDirect(cacheSize);
		CacheEntry firstSlot = new CacheEntry();
		firstSlot.position = 0;
		firstSlot.size = cacheSize;
		firstSlot.buffer = buffer.duplicate();
		slots.add(firstSlot);
	}
	
	private void checkHeapLimits() {
        Stopwatch stopWatch = SimonManager.getStopwatch("checkheaplimits");
		Split split = stopWatch.start();
		if (entriesLimit == -1) {
			split.stop();
			return;
		}
		if ((entries.size() - offHeapEntriesCount()) >= entriesLimit) {
			CacheEntry entry = removeLast();
			moveOffheap(entry);
		}
		split.stop();
	}
	
	protected void moveOffheap(CacheEntry entry) {
		byte[] array = null;
		try {
			array = serializer.serialize((Serializable)entry.object, entry.object.getClass());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		makeRoomInOffHeapMemory(array.length);
		entry.clazz = entry.object.getClass();
		entry.size = array.length;
		entry.object = null;
		ByteBuffer buf = getBufferFor(entry);
		entry.position = buf.position();
		buf.put(array);
		entry.buffer = buf;
		lruQueue.remove(entry);
		lruOffheapQueue.add(entry);
		usedMemory.addAndGet(entry.size);
		entries.put(entry.key, entry);
	}
	
	protected void moveInHeap(CacheEntry entry) {
		checkHeapLimits();
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
	}

	private ByteBuffer getBufferFor(CacheEntry entry) {
		CacheEntry slot = slots.higher(entry);
		if (slot == null) {
			// should try allocating a new buffer?
			// what if no buffer available?
			return null;
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

	private void makeRoomInOffHeapMemory(int bytesNeeded) {
		int freedBytes = 0;
		int bytes2free = (usedMemory.get() + bytesNeeded)-buffer.limit();
		
		while (freedBytes < bytes2free) {
			CacheEntry removedEntry = removeLastOffHeap();
			freedBytes += removedEntry.size;
			// should save to disk or demote to next layer
		}
	}
	
	public CacheEntry put(String key, Object object) {
        Stopwatch stopWatch = SimonManager.getStopwatch("put");
		Split split = stopWatch.start();
		checkHeapLimits();
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
        Stopwatch stopWatch = SimonManager.getStopwatch("get");
		Split split = stopWatch.start();
		CacheEntry entry = getEntry(key);
		split.stop();
		if (entry == null)
			return null;
		
		return entry.object;
	}
	
	public CacheEntry remove(String key) {
        Stopwatch stopWatch = SimonManager.getStopwatch("remove");
		Split split = stopWatch.start();
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
		CacheEntry last = lruQueue.poll();
		entries.remove(last.key);
		return last;
	}
	
	public CacheEntry removeLastOffHeap() {
		CacheEntry last = lruOffheapQueue.poll();
//		offHeapEntries.addAndGet(-1);
		usedMemory.addAndGet(-last.size);
		entries.remove(last.key);
		slots.add(last);
		return last;
	}
	
	public int heapEntriesCount() {
//		return entries.size() - offHeapEntries.get();
		return lruQueue.size();
	}
	
	public int offHeapEntriesCount() {
//		return offHeapEntries.get();
		return lruOffheapQueue.size();
	}
	
	public int usedMemory() {
		return usedMemory.get();
	}
	
	@Override
	public String toString() {
		return "CacheStore: {heap entries=" + heapEntriesCount() + ", off heap entries=" + offHeapEntriesCount() + ", usedMemory=" + usedMemory.get() + ", limit=" + entriesLimit + ", cacheSize=" + buffer.limit() +"}";
	}
}
