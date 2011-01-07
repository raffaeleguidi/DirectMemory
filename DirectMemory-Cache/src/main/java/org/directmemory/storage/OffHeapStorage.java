package org.directmemory.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.directmemory.CacheEntry;

public class OffHeapStorage extends Storage {

	public ConcurrentSkipListSet<CacheEntry> slots = new ConcurrentSkipListSet<CacheEntry>();
	ConcurrentLinkedQueue<ByteBuffer> memoryPages = new ConcurrentLinkedQueue<ByteBuffer>(); 
	public int maxPages = 0;
	public int pageSize = 0;
	
	public ConcurrentSkipListSet<CacheEntry> slots() {
		return slots;
	}

	private AtomicInteger usedMemory = new AtomicInteger(0);
	
	public OffHeapStorage(int pageSize, int maxPages) {
		this.pageSize = pageSize;
		this.maxPages = maxPages;
		addMemoryPageAndGetFirstSlot();
	}
	
	public int capacity() {
		return maxPages * pageSize;
	}
	
	public int remaining() {
		return capacity() - usedMemory.get();
	}
	
	public int usedMemory() {
		return usedMemory.get();
	}

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
	
	private ByteBuffer slice(CacheEntry slot2Slice, CacheEntry entry) {
		synchronized (slot2Slice) {
			// TODO: change buffer size and position management
			logger.debug("we removed it? " + slots.remove(slot2Slice));
			slot2Slice.size = slot2Slice.size - entry.size;
			slot2Slice.position = slot2Slice.position + entry.size;
			ByteBuffer buf = slot2Slice.buffer.duplicate();
			slot2Slice.buffer.position(slot2Slice.position);
			// TODO: check this
			slot2Slice.buffer.mark();
			if (slot2Slice.size > 0) {
				slots.add(slot2Slice);
				if (slot2Slice.buffer == null) {
					logger.error("why null?!?");
				}
				logger.debug("added sliced slot of " + slot2Slice.size + " bytes");
			} else {
				logger.debug("size of slot is zero bytes; we used: " + entry.size);
				logger.debug("and is in slots? " + slots.contains(slot2Slice));
			}
			return buf;
		}
	}

	private ByteBuffer bufferFor(CacheEntry entry) {
		// look for the smaller free buffer that can contain entry
		// it fails for almost equal buffers!!!
		CacheEntry slot = slots.ceiling(entry);

		if (slot == null) {
			// no large enough slots left at all, try to allocate another page
			slot = addMemoryPageAndGetFirstSlot();
		}

		if (slot == null) {
			// no free slots left free the last recently used
			CacheEntry first = slots.first();
			CacheEntry last = slots.last();
			logger.debug("cannot find a free slot for entry " + entry.key + " of size " + entry.size);
			logger.debug("slots=" + slots.size() + " first size is: " + first.size + " last size=" + last.size);
			signalOverFlow(entry.size); 
			slot = slots.ceiling(entry);
			if (slot.buffer == null) {
				logger.error("error: " + slot.key + " has an empty buffer");
				return null;
			}
		}

		logger.debug("got slot for " + entry.key + " at position " + slot.position);
		
		return slice(slot, entry);
	}	
	
	AtomicInteger pendingAllocation = new AtomicInteger(); 
	
	private void signalOverFlow(int size) {
		pendingAllocation.addAndGet(size);
		// should we pass it the supervisor or process it right now?
		// well, let's keep it in the supervisor for now
		logger.debug("overflow (before) is " + overflow());
		overflowToNext();
//		supervisor.signalOverflow(this);
		pendingAllocation.set(0);
		logger.debug("overflow (after) is " + overflow());
	}
	
	@Override
	public void overflowToNext() {
		super.overflowToNext();
	}

	@Override
	protected boolean moveIn(CacheEntry entry) {
		synchronized(entry) {
			@SuppressWarnings({"rawtypes", "unchecked"})
			Class clazz = entry.clazz();
			try {
				entry.array = serializer.serialize(entry.object, entry.object.getClass());
			} catch (IOException e) {
				logger.debug("error serializing " + entry.key + ": pos=" + entry.position + " size=" + entry.size);
				return false;
			}
			entry.size = entry.array.length;
			ByteBuffer buf = bufferFor(entry); 
			if (buf != null){
				entry.buffer = buf;
//				entry.size = entry.array.length;
				
				// TODO: change buffer size and position management
				entry.clazz = clazz;
				//TODO: check this
				entry.buffer.reset();
				entry.position = entry.buffer.position();
				entry.buffer.put(entry.array);
				entry.array = null;
				entry.object = null;
	
				usedMemory.addAndGet(entry.size);
				entries.put(entry.key, entry);
				lruQueue.remove(entry);
				lruQueue.add(entry);
				logger.debug("stored off heap " + entry.key + ": pos=" + entry.position + " size=" + entry.size);
			} else {
				logger.debug("no room to store " + entry.key + " - skipping");
				return false;
			}
		}
		// everything is ok
		return true;
	}

	@Override
	public boolean moveToHeap(CacheEntry entry) {
		byte[] source = null; 
		source = new byte[entry.size]; 
		try {
			synchronized (entry) {
				if (entry.buffer == null) {
					// sometimes this is null: why? already in heap?
					if (entry.object != null) {
						logger.warn("entry is already in heap" ); // yes
					} else {
						logger.error("entry is in non consistent state"); // no!
					}
					// is it really false?
					return false;
				}
				ByteBuffer buf = entry.buffer;
				buf.position(entry.position);
				buf.get(source);
				Object obj = serializer.deserialize(source, entry.clazz);
				entry.object = obj;
				logger.debug("freed slot of " + entry.size + " bytes");
			}
			usedMemory.addAndGet(-source.length);
			remove(entry);
		} catch (UTFDataFormatException e) {
			logger.error(e.getMessage());
		} catch (StreamCorruptedException e) {
			logger.error(e.getMessage());
		} catch (EOFException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
		} catch (InstantiationException e) {
			logger.error(e.getMessage());
		} catch (IllegalAccessException e) {
			logger.error(e.getMessage());
		}
		// everything should be fine even in case of errors
		return true;
	}
	
	@Override
	public void reset() {
		super.reset();
		slots.clear();
		for (ByteBuffer buffer : memoryPages) {
			buffer.clear();
		}
		memoryPages.clear();
		usedMemory.set(0);
		addMemoryPageAndGetFirstSlot();
		logger.debug("off heap storage reset");
	}
	
	@Override
	public void moveEntryTo(CacheEntry entry, Storage storage) {
		CacheEntry slot = new CacheEntry();
		slot.buffer = entry.buffer.duplicate();
		slot.size = entry.size;
		slot.position = entry.position;
		slot.buffer.position(slot.position);
		super.moveEntryTo(entry, storage);
		if (entries.containsKey(entry.key)){
			usedMemory.addAndGet(-entry.size);
			slots.add(slot);	
		}
		logger.debug("created free slot from " + entry.key + " of " + slot.size + " bytes");
	}
	
	@Override
	public CacheEntry delete(String key) {
		CacheEntry entry = entries.get(key);
		if (entry != null) {
			usedMemory.addAndGet(-entry.size);
			slots.add(entry);
			logger.debug("added slot of " + entry.size + " bytes");
			return super.delete(key);
		}
		return null;
	}
	
	@Override
	public CacheEntry removeLast() {
		CacheEntry last = lruQueue.poll();
		if (last == null) {
			logger.warn("no lru from off heap");
			return null;
		}		
		usedMemory.addAndGet(-last.size);
		entries.remove(last.key);
		slots.add(last);
		if (last.buffer == null) {
			logger.error("why null?!?");
		}
		logger.debug("added slot of " + last.size + " bytes");
		return last;
	}

	@Override
	public int overflow() {
		return (usedMemory.get() + pendingAllocation.get()) - capacity();
	}
}
