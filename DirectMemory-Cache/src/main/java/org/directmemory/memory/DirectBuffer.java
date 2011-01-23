package org.directmemory.memory;

import sun.misc.Unsafe;

public class DirectBuffer {
	private static final Unsafe unsafe = Unsafe.getUnsafe();
	private long address = -1; 
	private int size = -1;
	
	public DirectBuffer(int size) {
		super();
		allocate(size);
	}
	
	private void allocate(long size) {
		this.size = 0;
		this.address = unsafe.allocateMemory(size);
	}

	/** Gets the underlying system memory page size */
	public static long pageSize() {
		return unsafe.pageSize();
	}
	
	/** Allocates some arbitrary chunk of memory */
	public void allocateAndZero(long length) {
		allocate(length);
		unsafe.setMemory(address, length, (byte)0);
	}
	/**  Writes byte array to specified address quickly. */
//	public static void putBytes(byte [] buffer, long address) {
//		long bufferAddress = getObjectAddress(buffer);
//		unsafe.copyMemory(bufferAddress + Runtime.BARRAY_BASE_OFFSET, address, buffer.length);
//		assert (bufferAddress == getObjectAddress(buffer)); /* make sure the buffer wasn't moved */
//	}
	
	/**  Read byte array to specified address quickly. */
//	public static byte [] getBytes(long address, int length) {
//		byte [] buffer = new byte [length];
//		long bufferAddress = getObjectAddress(buffer);
//		unsafe.copyMemory(address, bufferAddress + Runtime.BARRAY_BASE_OFFSET, buffer.length);
//		assert (bufferAddress == getObjectAddress(buffer)); /* make sure the buffer wasn't moved */
//		return buffer;
//	}
	
//	public static void putBytes(byte [] buffer) {
//		int base = unsafe.arrayBaseOffset(byte[].class); 
//		int scale = unsafe.arrayIndexScale(byte[].class);
//		int elementIdx = 1;
//		int offsetForIdx = base + (elementIdx  * scale);
//		unsafe.copyMemory(scale + base , bufferaddress?, buffer.length);
//	}
	
	/** Writes byte array to specified address, one byte at a time. Safe, but slow. */
	public void put(byte[] src) {
		for (int i = 0; i < src.length; i++) {
			unsafe.putByte(address + i, src[i]);
		}
	}
	/** Writes byte array to specified address, one byte at a time. Safe, but slow. */
	public byte[] get() {
		final byte[] dest = new byte[size]; 
		for (int i = 0; i < size; i++) {
			dest[i] = unsafe.getByte(address + i);
		}
		return dest;
	}

	@Override
	protected void finalize() throws Throwable {
		unsafe.freeMemory(address);
		super.finalize();
	}
	
	public void dispose() {
		unsafe.freeMemory(address);
	}
	
}
