package org.directmemory.test;

import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.directmemory.cache.CacheEntry;
import org.directmemory.measures.Ram;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertNotNull;

public class ConcurrentHashMapTest {

	private static Logger logger=LoggerFactory.getLogger(ConcurrentHashMapTest.class);
	
	private String showMemory() {
		// Get current size of heap in bytes
		long heapSize = Runtime.getRuntime().totalMemory();

		// Get maximum size of heap in bytes. The heap cannot grow beyond this size.
		// Any attempt will result in an OutOfMemoryException.
		long heapMaxSize = Runtime.getRuntime().maxMemory();

		// Get amount of free memory within the heap in bytes. This size will increase
		// after garbage collection and decrease as new objects are created.
		long heapFreeSize = Runtime.getRuntime().freeMemory();		
		
		return new Formatter().format("heapSize %s heapMaxSize %s heapFreeSize %s", Ram.inMb(heapSize), Ram.inMb(heapMaxSize), Ram.inMb(heapFreeSize)).toString();
	}

	public void fillAndGet(int howMany) {
		Map<String, CacheEntry> entries = new ConcurrentHashMap<String, CacheEntry>();
		logger.debug("\r\n*******************************************************");
		logger.debug("inserting " + howMany + " entries\r\n" + showMemory());
		for (int i = 0; i < howMany; i++) {
			CacheEntry entry = new CacheEntry();
			entry.key = "test" + i;
			entries.put(entry.key, entry);
		}
		logger.debug("test single get");
		CacheEntry entry = entries.get("test" + (howMany/2));
		assertNotNull("entry not found", entry);
		logger.debug("finished test single get");

		logger.debug("inserted " + howMany + " entries\r\n" + showMemory());
		logger.debug("test get");
		for (int i = 0; i < howMany; i++) {
			CacheEntry entry2 = entries.get("test" + (howMany/2));
			assertNotNull("entry not found", entry2);
		}
		logger.debug(howMany + " entries retrieved");
		logger.debug("clearing and forcing garbage collection");
		entries.clear();
		Runtime.getRuntime().gc();
		logger.debug("garbage collection ended");
	}

	@Test
	public void oneMillionPutsAndGets() {
		fillAndGet(1000000);
	}

	@Test
	public void twoMillionPutsAndGets() {
		fillAndGet(2000000);
	}

	@Test
	public void threeMillionPutsAndGets() {
		fillAndGet(3000000);
	}

	@Test
	public void fourMillionPutsAndGets() {
		fillAndGet(4000000);
	}
	@Test
	public void fiveMillionPutsAndGets() {
		fillAndGet(5000000);
	}
}
