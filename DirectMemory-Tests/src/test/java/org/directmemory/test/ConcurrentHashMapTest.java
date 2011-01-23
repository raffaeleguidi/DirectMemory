package org.directmemory.test;

import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.directmemory.cache.CacheEntry2;
import org.directmemory.measures.Monitor;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentHashMapTest {

	private static Logger logger=LoggerFactory.getLogger(ConcurrentHashMapTest.class);
	
	private String showMemory() {
		// Get current size of heap in bytes
//		long heapSize = Runtime.getRuntime().totalMemory();

		// Get maximum size of heap in bytes. The heap cannot grow beyond this size.
		// Any attempt will result in an OutOfMemoryException.
		long heapMaxSize = Runtime.getRuntime().maxMemory();

		// Get amount of free memory within the heap in bytes. This size will increase
		// after garbage collection and decrease as new objects are created.
		long heapFreeSize = Runtime.getRuntime().freeMemory();		
		
		return new Formatter().format("heapFree: %s heapMax: %s", Ram.inMb(heapFreeSize), Ram.inMb(heapMaxSize)).toString();
	}

	public void fillAndGet(int howMany, Map<String, CacheEntry2> entries, boolean createObject, boolean useSingleBuffer) {
		Monitor testMon = new Monitor(entries.getClass().getSimpleName());
		final long testStarted = testMon.start();
		logger.debug("*** " + entries.getClass().getSimpleName() + " ************************");
		logger.debug("inserting " + howMany + " entries; " + showMemory());
		ByteBuffer buffer = null;
		
		if (useSingleBuffer) {
			buffer = ByteBuffer.allocateDirect(Ram.Mb(512));
		}
		int singleItemSize = Ram.Mb(512)/howMany;
		
		Monitor putMon = new Monitor("put");
		
		byte[] buf = new byte[singleItemSize];
		
		for (int i = 0; i < singleItemSize; i++) {
			buf[i] = (byte)i;
		}
		
		for (int i = 0; i < howMany; i++) {
			final long startedAt = putMon.start();
			final CacheEntry2 entry = new CacheEntry2();
			entry.key = "test" + i;
			if (createObject)
				entry.object=new DummyPojo("test"+i, Ram.Kb(2));
			if (!useSingleBuffer) {
//				entry.identity = new DirectBuffer(singleItemSize);
//				((DirectBuffer)entry.identity).put(buf);
				entry.buffer = ByteBuffer.allocateDirect(singleItemSize);
			} else {
				entry.buffer = buffer.duplicate();
			}
			entries.put(entry.key, entry);
			putMon.stop(startedAt);
		}
		logger.debug("inserted " + howMany + " entries; average: " + putMon.average() + "ns total: " + (putMon.totalTime()/1000000000)+ "s - " + showMemory());
		logger.debug("test single get");
		CacheEntry2 entry = entries.get("test" + (howMany/2));
		assertNotNull("entry not found", entry);
		logger.debug("finished test single get");

		Monitor getMon = new Monitor("put");
		logger.debug("starting get massive get test");
		for (int i = 0; i < howMany; i++) {
			final long startedAt = getMon.start();
			CacheEntry2 entry2 = entries.get("test" + (howMany/2));
//			((DirectBuffer)entry.identity).dispose();
			getMon.stop(startedAt);
			assertNotNull("entry not found", entry2);
		}
		logger.debug("inserted " + howMany + " entries; average: " + getMon.average() + "ns total: " + (getMon.totalTime()/1000000000)+ "s - " + showMemory());
		logger.debug("clearing and forcing garbage collection");
		entries.clear();
		Runtime.getRuntime().gc();
		logger.debug("garbage collection ended");
		testMon.stop(testStarted);
		logger.info(testMon.toString());
	}
	
	@Test
	public void oneMillionPutsAndGets() {
		fillAndGet(1000000, new ConcurrentHashMap<String, CacheEntry2>(), false, true);
		fillAndGet(1000000, new ConcurrentSkipListMap<String, CacheEntry2>(), false, true);
	}

//	@Test
//	public void twoMillionPutsAndGets() {
//		fillAndGet(2000000);
//	}
//
//	@Test
//	public void threeMillionPutsAndGets() {
//		fillAndGet(3000000);
//	}
//
//	@Test
//	public void fourMillionPutsAndGets() {
//		fillAndGet(4000000);
//	}
//	@Test
//	public void fiveMillionPutsAndGets() {
//		fillAndGet(5000000);
//	}
}
