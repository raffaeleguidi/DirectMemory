package org.directmemory.memory;

import java.util.List;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryManager {
	private static Logger logger = LoggerFactory.getLogger(MemoryManager.class);
	public static List<OffHeapMemoryBuffer> buffers = new Vector<OffHeapMemoryBuffer>();
	public static OffHeapMemoryBuffer activeBuffer = null;
	
	private MemoryManager() {
		//static class
	}
	
	public static void init(int numberOfBuffers, int size) {
		for (int i = 0; i < numberOfBuffers; i++) {
			buffers.add(OffHeapMemoryBuffer.createNew(size, i));
		}
		activeBuffer = buffers.get(0);
	}
	
	public static Pointer store(byte[] payload, int expiresIn) {
		Pointer p = activeBuffer.store(payload, expiresIn);
		if (p == null) {
			if (activeBuffer.bufferNumber+1 == buffers.size()) {
				return null;
			} else {
				// try next buffer
				activeBuffer = buffers.get(activeBuffer.bufferNumber+1);
				p = activeBuffer.store(payload, expiresIn);
			}
		}
		return p;
	}
	
	public static Pointer store(byte[] payload) {
		return store(payload, 0);
	}
	
	public static Pointer update(Pointer pointer, byte[] payload) {
		Pointer p = activeBuffer.update(pointer, payload);
		if (p == null) {
			if (activeBuffer.bufferNumber == buffers.size()) {
				return null;
			} else {
				// try next buffer
				activeBuffer = buffers.get(activeBuffer.bufferNumber+1);
				p = activeBuffer.store(payload);
			}
		}
		return p;
	}
	
	public static byte[] retrieve(Pointer pointer) {
		return buffers.get(pointer.bufferNumber).retrieve(pointer);
	}
	
	public static void free(Pointer pointer) {
		buffers.get(pointer.bufferNumber).free(pointer);
	}
	
	public static void clear() {
		for (OffHeapMemoryBuffer buffer : buffers) {
			buffer.clear();
		}
		activeBuffer = buffers.get(0);
	}
	
	public static long capacity() {
		long totalCapacity = 0;
		for (OffHeapMemoryBuffer buffer : buffers) {
			totalCapacity += buffer.capacity();
		}
		return totalCapacity;
	}

	public static long disposeExpired() {
		long disposed = 0;
		for (OffHeapMemoryBuffer buffer : buffers) {
			disposed += buffer.disposeExpired();
		}
		return disposed;
	}

}
