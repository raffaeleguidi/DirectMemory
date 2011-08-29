package org.directmemory.memory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.Random;

import org.directmemory.measures.Ram;
import org.directmemory.memory.MemoryManager;
import org.directmemory.memory.OffHeapMemoryBuffer;
import org.directmemory.memory.Pointer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.Maps;

public class MemoryManagerTests {
//	@Rule
	public MethodRule benchmarkRun = new BenchmarkRule();
	
	
	@BeforeClass
	public static void init() {
		logger.info("init");
		MemoryManager.init(1, Ram.Mb(100));
	}
	
	@Test
	public void smokeTest() {
		Random rnd = new Random();
		int size = rnd.nextInt(10) * (int)MemoryManager.capacity() / 100;
		logger.info("payload size=" + Ram.inKb(size));
		Pointer p = MemoryManager.store(new byte[size]);
		logger.info("stored");
		assertNotNull(p);
		assertEquals(size,p.end);
		assertEquals(size, MemoryManager.activeBuffer.used());
		MemoryManager.free(p);
		assertEquals(0, MemoryManager.activeBuffer.used());		
		logger.info("end");
	}

	byte[] payload = "012345678901234567890123456789012345678901234567890123456789".getBytes();

	@Test
	public void fillupTest() {
		MemoryManager.clear();
		logger.info("payload size=" + Ram.inKb(payload.length));
		long howMany = (MemoryManager.capacity() / payload.length);
		howMany=(howMany*90)/100;
		
		
		for (int i = 0; i < howMany ; i++) {
			Pointer p = MemoryManager.store(payload);
			assertNotNull(p);
		}
		
		logger.info("" + howMany + " items stored");
	}
	
	
	@Test
	public void readTest() {
		for (OffHeapMemoryBuffer buffer : MemoryManager.buffers) {
			for (Pointer ptr : buffer.pointers) {
				if (!ptr.free) {
					byte[] res = MemoryManager.retrieve(ptr);
					assertNotNull(res);
					assertEquals(new String(payload), new String(res));
				}
			}
		}
	}
	

	private static Logger logger = LoggerFactory.getLogger(MallocTests.class);

	final static Map<String, Byte> test = Maps.newHashMap();

}
