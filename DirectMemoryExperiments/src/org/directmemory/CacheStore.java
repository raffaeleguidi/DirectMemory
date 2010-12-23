package org.directmemory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.directmemory.utils.StandardSerializer;
import org.directmemory.utils.Serializer;

public class CacheStore {
	ByteBuffer buffer;
	Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentLinkedQueue<CacheEntry> lruOffheapQueue = new ConcurrentLinkedQueue<CacheEntry>();
	ConcurrentSkipListSet<CacheEntry> slots = new ConcurrentSkipListSet<CacheEntry>();
	private AtomicInteger usedMemory = new AtomicInteger(0);
	private AtomicInteger offHeapEntries = new AtomicInteger(0);
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
		if ((entries.size() - offHeapEntries.get()) >= entriesLimit) {
			CacheEntry entry = removeLast();
			saveOffheap(entry);
		}
	}
	
	protected void saveOffheap(CacheEntry entry) {
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
		lruOffheapQueue.add(entry);
		usedMemory.addAndGet(entry.size);
		offHeapEntries.addAndGet(1);
		entries.put(entry.key, entry);
	}

	private ByteBuffer getBufferFor(CacheEntry entry) {
		CacheEntry slot = slots.higher(entry);
//		if (slot == null)
//			throw new Exception("Couldn't find a free slot");
		ByteBuffer freeBuffer = slot.buffer.duplicate();
		freeBuffer.position(slot.position);
		slot.buffer.position(slot.position);
		slot.position += entry.size;
		slot.size -= entry.size;
		return freeBuffer;
	}

	private void makeRoomInOffHeapMemory(int bytes2add) {
		int freedBytes = 0;
		int bytes2free = (usedMemory.get() + bytes2add)-buffer.limit();
		
		while (freedBytes < bytes2free) {
			CacheEntry removedEntry = removeLastOffHeap();
			freedBytes += removedEntry.size;
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
		if (entry == null)
			return null;
		if (entry.inHeap()) {
			lruQueue.remove(entry);
			lruQueue.add(entry);
		} else {
			lruOffheapQueue.remove(entry);
			lruOffheapQueue.add(entry);
		}
		return entries.get(key);
	}
	
	public Object get(String key) {
		CacheEntry entry = getEntry(key);
		if (entry.inHeap()) {
			return entry.object;
		} else {
			entry.buffer.position(entry.position);
			byte[] source = new byte[entry.size]; 
			entry.buffer.get(source);
			try {
				return serializer.deserialize(source, entry.clazz);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}
	
	public CacheEntry remove(String key) {
		CacheEntry entry = entries.remove(key);
		if (entry.inHeap()) {
			lruQueue.remove(entry);
		} else {
			offHeapEntries.addAndGet(-1);
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
		offHeapEntries.addAndGet(-1);
		usedMemory.addAndGet(-last.size);
		entries.remove(last.key);
		slots.add(last);
		return last;
	}
	
	public int heapEntriesCount() {
		return entries.size() - offHeapEntries.get();
	}
	
	public int offHeapEntriesCount() {
		return offHeapEntries.get();
	}
	
	public int usedMemory() {
		return usedMemory.get();
	}
	
	@Override
	public String toString() {
		return "CacheStore: {heap entries=" + heapEntriesCount() + ", off heap entries=" + offHeapEntries.get() + ", usedMemory=" + usedMemory.get() + ", limit=" + entriesLimit + ", cacheSize=" + buffer.limit() +"}";
	}
}
