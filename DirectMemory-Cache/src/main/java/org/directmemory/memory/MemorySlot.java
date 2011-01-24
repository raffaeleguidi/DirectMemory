package org.directmemory.memory;

import java.nio.ByteBuffer;

public class MemorySlot implements Comparable<MemorySlot>  {
	public int size;
	public int address;
	public ByteBuffer buffer;
	
	public MemorySlot() {
		super();
	}

	public MemorySlot(int size) {
		super();
		this.size = size;
	}

	@Override
	public int compareTo(MemorySlot other) {
		if (other.size > size) {
			return 1;
		} else if (other.size < size) {
			return -1;
		}
		// values are equal
		return 0;
	}
}
