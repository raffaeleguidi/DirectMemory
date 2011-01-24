package org.directmemory.memory;

import java.nio.ByteBuffer;
import java.util.Formatter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkMemory {

	protected static Logger logger=LoggerFactory.getLogger(BulkMemory.class);

	public ConcurrentSkipListSet<MemorySlot> slots = new ConcurrentSkipListSet<MemorySlot>();
	ConcurrentLinkedQueue<ByteBuffer> memoryPages = new ConcurrentLinkedQueue<ByteBuffer>(); 
	public int maxPages = 0;
	public int pageSize = 0;
	
	public ConcurrentSkipListSet<MemorySlot> slots() {
		return slots;
	}

	private AtomicInteger usedMemory = new AtomicInteger(0);
	
	public BulkMemory(int pageSize, int maxPages) {
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

	private MemorySlot slotForBuffer(ByteBuffer buffer) {
		MemorySlot newSlot = new MemorySlot();
		newSlot.address = buffer.position();
		newSlot.size = buffer.limit();
		newSlot.buffer = buffer.duplicate();
		return newSlot;
	}
	
	private MemorySlot addMemoryPageAndGetFirstSlot() {		
		if (memoryPages.size() < maxPages) {
			logger.info("allocating a new memory page");
			
			ByteBuffer page = ByteBuffer.allocateDirect(pageSize);
			// setup beginning and limit of buffer
			page.position(0);
			page.mark();
			page.limit(pageSize);
			page.reset();

			MemorySlot firstSlot = slotForBuffer(page);
			memoryPages.add(page);
			slots.add(firstSlot);
			return firstSlot;
		} else {
			logger.debug("no memory pages left");
			return null;
		}
	}
	
	private void slice(MemorySlot slot2Slice, MemorySlot entry) {
		synchronized (slot2Slice) {
			// TODO: change buffer size and position management
			logger.debug("we removed it? " + slots.remove(slot2Slice));
			slot2Slice.size = slot2Slice.size - entry.size;
			slot2Slice.address = slot2Slice.address + entry.size;
			entry.buffer = slot2Slice.buffer.duplicate();
			slot2Slice.buffer.position(slot2Slice.address);
			usedMemory.addAndGet(entry.size);
			
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
		}
	}

	public MemorySlot allocate(int size) {
		// look for the smaller free buffer that can contain entry
		// it fails for almost equal buffers!!!	
		
		MemorySlot request = new MemorySlot(size);
		
		MemorySlot slot = slots.ceiling(request);

		if (slot == null) {
			// no large enough slots left at all, try to allocate another page
			slot = addMemoryPageAndGetFirstSlot();
		}

		if (slot == null) {
			// no free slots left free the last recently used
			if (logger.isDebugEnabled()) {
				MemorySlot first = slots.first();
				MemorySlot last = slots.last();
				logger.debug("cannot find a free slot of " + size + " bytes");
				logger.debug("slots=" + slots.size() + " first size is: " + first.size + " last size=" + last.size);
			}
			// try again
			slot = slots.ceiling(request);
			if (slot == null) {
				logger.error("error: no large enough free slots for " + request.size);
				return null;
			}
			if (slot.buffer == null) {
				logger.error("error: " + slot + " has an empty buffer (weird...)");
				return null;
			}
		}

		logger.debug("got slot for " + slot.size + " at address " + slot.address);
		
		slice(slot, request);
		
		return request;
	}	
	
	public void dispose() {
		slots.clear();
		for (ByteBuffer buffer : memoryPages) {
			buffer.clear();
		}
		memoryPages.clear();
		usedMemory.set(0);
//		addMemoryPageAndGetFirstSlot();
		logger.debug("off heap storage disposed");
	}
		
	public void free(MemorySlot slot) {
		if (slot != null) {
			usedMemory.addAndGet(-slot.size);
			slots.add(slot);
			logger.debug("added slot of " + slot.size + " bytes");
		}
	}
	
	@Override
	public String toString() {
		return new Formatter()
					.format(
							"OffHeap: used memory: %2d/%3d in %4d free slots", 
							usedMemory.get(),
							capacity(),
							slots.size()
							)
					.toString();
	}
}
