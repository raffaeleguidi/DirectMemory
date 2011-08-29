package org.directmemory.memory.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import org.directmemory.measures.Ram;
import org.directmemory.memory.OffHeapMemoryBuffer;
import org.directmemory.memory.Pointer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.google.common.collect.MapMaker;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart()
@BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 5)

public class MallocTests {
	
	@Rule
	public MethodRule benchmarkRun = new BenchmarkRule();

	Random rnd = new Random();

	private static Logger logger = LoggerFactory.getLogger(MallocTests.class);

	@After
	public void dump() {
		logger.info("off-heap allocated: " + Ram.inMb(mem.capacity()));
		logger.info("off-heap used:      " + Ram.inMb(mem.used()));
		logger.info("heap - max: " + Ram.inMb(Runtime.getRuntime().maxMemory()));
		logger.info("heap - allocated: " + Ram.inMb(Runtime.getRuntime().totalMemory()));
		logger.info("heap - free : " + Ram.inMb(Runtime.getRuntime().freeMemory()));
		logger.info("************************************************");
	}

	OffHeapMemoryBuffer mem = OffHeapMemoryBuffer.createNew(512 * 1024 * 1024);
  
	@Test
	public void oneMillionEntries() {
		assertNotNull(mem);
		int howMany = 1000000;
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
	}
	
	@Test
	public void twoMillionEntries() {
		
		assertNotNull(mem);
		int howMany = 2000000;
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
	}

	@Test
	public void fiveMillionEntries() {
		
		assertNotNull(mem);
		int howMany = 5000000;
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
	}

	
	@Test
	public void withMap() {

		ConcurrentMap<Long, Pointer> map = new MapMaker()
			.concurrencyLevel(4)
			.initialCapacity(500000)
			.makeMap();

		String str = "This is the string to store into the off-heap memory";
		
		int size = str.length();
		int howMany = 1000000;
		byte[] payload = str.getBytes();

		logger.info("adding " + howMany + " strings of " + size + " bytes...");
		for (long i = 0; i < howMany; i++) {
			Pointer p = mem.store(payload);
			map.put(i, p);
		}
		logger.info("...done");

	}
	
	@Before
	public void before(){
		mem.clear();
	}
	



	@Test
	public void oneMillionEntriesWithRead() {
	
		logger.info("total capacity=" + Ram.inMb(mem.capacity()));
		assertNotNull(mem);
		int size = 400;
		int howMany = 1000000;
		logger.info("payload size=" + Ram.inKb(size));
		logger.info("entries=" + howMany);
		String test = "this is a nicely crafted test";
		byte[] payload = test.getBytes();
		for (int i = 0; i < howMany; i++) {
			Pointer p = mem.store(payload);
			byte[] check = mem.retrieve(p);
			assertNotNull(check);
			assertEquals(test, new String(check));
		}
		
		logger.info("total used=" + Ram.inMb(mem.used()));
	}
}
	
	

