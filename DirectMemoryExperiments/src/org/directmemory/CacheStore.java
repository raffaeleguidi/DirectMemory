package org.directmemory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;

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
		if (entriesLimit == -1)
			return;
		if ((entries.size() - offHeapEntriesCount()) >= entriesLimit) {
			CacheEntry entry = removeLast();
			moveOffheap(entry);
		}
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
		entry.buffer = getBufferFor(entry);
		entry.position = entry.buffer.position();
		entry.buffer.put(array);
		lruQueue.remove(entry);
		lruOffheapQueue.add(entry);
		usedMemory.addAndGet(entry.size);
//		offHeapEntries.addAndGet(1);
		entries.put(entry.key, entry);
	}
	
	protected void moveInHeap(CacheEntry entry) {
		entry.buffer.position(entry.position);
		byte[] source = new byte[entry.size]; 
		entry.buffer.get(source);
		try {
			checkHeapLimits();
			entry.object = serializer.deserialize(source, entry.clazz);
//			offHeapEntries.decrementAndGet();
			usedMemory.addAndGet(-source.length);
			lruOffheapQueue.remove(entry);
			lruQueue.remove(entry);
			lruQueue.add(entry);
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
		ByteBuffer freeBuffer = slot.buffer.duplicate();
		freeBuffer.position(slot.position);
		slot.buffer.position(slot.position);
		slot.position += entry.size;
		slot.size -= entry.size;
		return freeBuffer;
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
		checkHeapLimits();
		CacheEntry entry = new CacheEntry();
		entry.key = key;
		entry.object = object;
		lruQueue.add(entry);
		entries.put(key, entry);
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

		return entries.get(key);
	}
	
	public Object get(String key) {
		CacheEntry entry = getEntry(key);
		return entry.object;
	}
	
	public CacheEntry remove(String key) {
		CacheEntry entry = entries.remove(key);
		if (entry.inHeap()) {
			lruQueue.remove(entry);
		} else {
//			offHeapEntries.addAndGet(-1);
			usedMemory.addAndGet(-entry.size);
			lruOffheapQueue.remove(entry);
			slots.add(entry);
		}
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
