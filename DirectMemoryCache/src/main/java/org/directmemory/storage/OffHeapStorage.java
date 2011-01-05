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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class OffHeapStorage extends Storage {

	ConcurrentSkipListSet<CacheEntry> slots = new ConcurrentSkipListSet<CacheEntry>();
	ConcurrentLinkedQueue<ByteBuffer> memoryPages = new ConcurrentLinkedQueue<ByteBuffer>(); 
	public int maxPages = 0;
	public int pageSize = 0;

	private AtomicInteger usedMemory = new AtomicInteger(0);
	
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
			signalOverFlow(entry.size);
			slot = slots.ceiling(entry);
		}
		if (slot == null) {
			// no free memory left - I quit trying
			return null;
		}
		
		return slice(slot, entry);
	}	
	
	private void signalOverFlow(int size) {
		throw new NotImplementedException();
	}

	@Override
	protected boolean store(CacheEntry entry) {
		synchronized(entry) {
			ByteBuffer buf = bufferFor(entry); 
			if (buf != null){
				@SuppressWarnings({"rawtypes", "unchecked"})
				Class clazz = entry.clazz();
				try {
					entry.array = serializer.serialize(entry.object, entry.object.getClass());
				} catch (IOException e) {
					logger.debug("error serializing " + entry.key + ": pos=" + entry.position + " size=" + entry.size);
					return false;
				}
				entry.buffer = buf;
				entry.size = entry.array.length;
				
				// TODO: change buffer size and position management
				entry.clazz = clazz;
				//TODO: check this
				entry.buffer.reset();
				entry.position = entry.buffer.position();
				entry.buffer.put(entry.array);
				entry.array = null;
	
				usedMemory.addAndGet(entry.size);
				entries.put(entry.key, entry);
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
	protected boolean restore(CacheEntry entry) {
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
				entry.buffer = null;
				// change buffer size and position management
				CacheEntry freeSlot = new CacheEntry();
				
				freeSlot.buffer = buf;
				freeSlot.position = entry.position;
				// TODO: check this
				freeSlot.buffer.reset();
				//freeSlot.buffer.position(freeSlot.position);
				freeSlot.size = entry.size;

				slots.add(freeSlot);
				logger.debug("added slot of " + freeSlot.size + " bytes");
			}
			usedMemory.addAndGet(-source.length);
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
		// everything shuld be fine even in case of errors
		return true;
	}

}
