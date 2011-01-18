package org.directmemory.store;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.directmemory.cache.CacheEntry;
import org.directmemory.measures.Ram;
import org.directmemory.serialization.Serializer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SimpleOffHeapStore extends AbstractQueuedStore {

	@Override
	String storeName() {
		return "SimpleOffHeapStore";
	}

	public Serializer serializer;
	private long usedMemory = 0;
	
	@Override
	public CacheEntry remove(Object key) {
		CacheEntry entry = super.remove(key);
		if (entry != null) {
			if (entry.buffer != null) {
				entry.buffer.clear();
				entry.buffer= null;
			}
			usedMemory-=entry.size;
		}
		return entry;
	}
	
	private static final long serialVersionUID = 1L;
	
	@Override
	void asyncPopIn(CacheEntry entry) {
		if (entry.inHeap()) {
			try {
				// setup beginning and limit of buffer
				entry.array = serializer.serialize(entry.object, entry.object.getClass());
				entry.size = entry.array.length;
				usedMemory+=entry.size;
				entry.buffer = ByteBuffer.allocateDirect(entry.size);
				entry.buffer.put(entry.array);
				entry.array=null;
				entry.buffer.rewind();
				entry.clazz = entry.object.getClass();
				entry.object = null;
			} catch (IOException e) {
				logger.error("error serializing entry " + entry.key, e);
			}
		}
		entry.setStore(this); // done, take it		
	}
	

	@Override
	void popOut(CacheEntry entry) {
		if (entry.offHeap()) {
			try {
				entry.array = new byte[entry.size];
				entry.buffer.rewind();
				entry.buffer.get(entry.array);
				entry.object = serializer.deserialize(entry.array, entry.clazz());
				entry.array = null;
				entry.buffer.clear();
				entry.buffer= null;
				usedMemory-=entry.size;
				entry.size = 0;
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
	}

	@Override
	byte[] toStream(CacheEntry entry) {
		throw new NotImplementedException();
	}

	@Override
	Object toObject(CacheEntry entry) {
		throw new NotImplementedException();
	}
	
	@Override
	public void dispose() {
		queue.clear();
		for (CacheEntry entry : values()) {
			if (entry.buffer != null) {
				entry.buffer.clear();
				entry.buffer = null;
			}
		}
		usedMemory=0;
		super.dispose();
	}
	
	public long usedMemory() {
		return usedMemory;
	}
	
	@Override
	public String toString() {
		return super.toString() + " with a " + Ram.inMb(usedMemory) + " off-heap usage";
	}

}
