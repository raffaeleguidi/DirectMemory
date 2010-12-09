package org.directcache.buffer;

import java.nio.ByteBuffer;

public class ThreadSafeDirectBuffer  {
	
	private ByteBuffer buffer;
	
	public ThreadSafeDirectBuffer(int capacity) {
		buffer = ByteBuffer.allocateDirect(capacity);
	}

	public byte[] get(int offset, int length) {
		byte[] dest = new byte[length];
		synchronized (buffer) {
			int oldPosition = buffer.position();
			buffer.position(offset);
			buffer.get(dest, 0, length);
			buffer.position(oldPosition);
		}
		return dest;
	}
	
	public void put(byte[] source, int offset) {
		synchronized (buffer) {
			int oldPosition = buffer.position();
			buffer.position(offset);
			buffer.put(source, 0, source.length);
			buffer.position(oldPosition);
		}
	}
	
	public void append(byte[] source) {
		synchronized (buffer) {
			buffer.put(source);
		}
	}
	
	public int remaining() {
		return buffer.remaining();
	}
	
	public int capacity() {
		return buffer.capacity();
	}
	
	public int position() {
		return buffer.position();
	}
	
}
