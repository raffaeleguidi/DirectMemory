package org.directmemory.memory.test;

import static org.junit.Assert.assertNotNull;

import org.directmemory.measures.Ram;
import org.directmemory.memory.MemoryManager;
import org.directmemory.memory.OffHeapMemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Starter {

	private static Logger logger = LoggerFactory.getLogger(MallocTests.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 3) {
			System.out.println("DirectMemory (for real testers only!) - usage:");
			System.out.println("	java -XX:MaxDirectMemorySize=XXXXm -XmxXXXXm -XmsXXXXm -jar dm-test.jar <buffers> <Mb for each buffer> <entries>");
			return;
		}
		
		int buffers = new Integer(args[0]); 
		int mb = new Integer(args[1]); 
		int entries = new Integer(args[2]); 
		
		logger.info("buffers: " + buffers);
		logger.info("mb: " + mb);
		logger.info("entries: " + entries);
		
		Starter starter = new Starter();
		starter.rawInsertMultipleBuffers(buffers, mb, entries);		
	}
	
	public void dump(OffHeapMemoryBuffer mem) {
		logger.info("off-heap - buffer: " + mem.pointers.get(1).bufferNumber);
		logger.info("off-heap - allocated: " + Ram.inMb(mem.capacity()));
		logger.info("off-heap - used:      " + Ram.inMb(mem.used()));
		logger.info("heap 	  - max: " + Ram.inMb(Runtime.getRuntime().maxMemory()));
		logger.info("heap     - allocated: " + Ram.inMb(Runtime.getRuntime().totalMemory()));
		logger.info("heap     - free : " + Ram.inMb(Runtime.getRuntime().freeMemory()));
		logger.info("************************************************");
	}

	public void rawInsert(int megabytes, int howMany) {
		OffHeapMemoryBuffer mem = OffHeapMemoryBuffer.createNew(megabytes * 1024 * 1024);
		assertNotNull(mem);
		int size = mem.capacity() / (howMany);
		size -= size / 100 * 1;
		logger.info("payload size=" + size);
		logger.info("entries=" + howMany);

		logger.info("starting...");
		
		long start = System.currentTimeMillis();

		byte[] payload = new byte[size];
		for (int i = 0; i < howMany; i++) {
			mem.store(payload);
		}
		
		logger.info("...done in " + (System.currentTimeMillis() - start) + " msecs.");
		logger.info("---------------------------------");
		dump(mem);
	}


	public void rawInsertMultipleBuffers(int buffers, int megabytes, int howMany) {
		MemoryManager.init(buffers, Ram.Mb(megabytes));
		int size = (int)(MemoryManager.capacity() / (howMany));
		size -= size / 100 * 1;
		logger.info("payload size=" + size);
		logger.info("entries=" + howMany);

		logger.info("starting...");
		
		long start = System.currentTimeMillis();

		byte[] payload = new byte[size];
		for (int i = 0; i < howMany; i++) {
			MemoryManager.store(payload);
		}
		
		logger.info("...done in " + (System.currentTimeMillis() - start) + " msecs.");
		logger.info("---------------------------------");

		for (OffHeapMemoryBuffer buf : MemoryManager.buffers) {
			dump(buf);
		}
	}


	

}
