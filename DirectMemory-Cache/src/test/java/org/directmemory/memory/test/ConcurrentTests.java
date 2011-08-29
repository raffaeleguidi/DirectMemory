package org.directmemory.memory.test;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.directmemory.measures.Ram;
import org.directmemory.memory.OffHeapMemoryBuffer;
import org.directmemory.memory.Pointer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 5)

public class ConcurrentTests {
	
	private final static int entries = 100000;
	public static AtomicInteger count = new AtomicInteger();
	private static AtomicInteger got = new AtomicInteger(); 
	private static AtomicInteger missed = new AtomicInteger(); 
	private static AtomicInteger good = new AtomicInteger(); 
	private static AtomicInteger bad = new AtomicInteger(); 
	private static AtomicInteger read = new AtomicInteger();
	public static OffHeapMemoryBuffer mem = OffHeapMemoryBuffer.createNew(512 * 1024 * 1024);

	public static ConcurrentMap<String, Pointer> map = new MapMaker()
		.concurrencyLevel(4)
		.initialCapacity(100000)
		.makeMap();

	
	@BenchmarkOptions(benchmarkRounds = 100000, warmupRounds=0, concurrency=100)
  	@Test
  	public void store() {
  		final String key = "test-" + count.incrementAndGet();
  		map.put(key, mem.store(key.getBytes()));
  	}
	
	@BenchmarkOptions(benchmarkRounds = 1000000, warmupRounds=0, concurrency=100)
  	@Test
  	public void retrieveCatchThemAll() {
  		String key = "test-" + (rndGen.nextInt(entries)+1);
  		Pointer p = map.get(key);
		read.incrementAndGet();
  		if (p != null) {
  			got.incrementAndGet();
  			byte [] payload = mem.retrieve(p);
  	  		if (key.equals(new String(payload)))
  	  			good.incrementAndGet();
  	  		else
  	  			bad.incrementAndGet();
  		} else {
  			logger.info("did not find key " + key);
  			missed.incrementAndGet();
  		}
  	}
	
	@BenchmarkOptions(benchmarkRounds = 1000000, warmupRounds=0, concurrency=100)
  	@Test
  	public void retrieveCatchHalfOfThem() {
  		String key = "test-" + (rndGen.nextInt(entries*2)+1);
  		Pointer p = map.get(key);
		read.incrementAndGet();
  		if (p != null) {
  			got.incrementAndGet();
  			byte [] payload = mem.retrieve(p);
  	  		if (key.equals(new String(payload)))
  	  			good.incrementAndGet();
  	  		else
  	  			bad.incrementAndGet();
  		} else {
  			missed.incrementAndGet();
  		}
  	}
	
	private void put(String key) {
  		map.put(key, mem.store(key.getBytes()));
	}
  
	@BenchmarkOptions(benchmarkRounds = 1000000, warmupRounds=0, concurrency=10)
  	@Test
  	public void write3Read7() {
  		String key = "test-" + (rndGen.nextInt(entries*2)+1);
  		
  		int what = rndGen.nextInt(10);
  		
  		switch (what) {
			case 0: 
			case 1: 
			case 2: 
  				put(key);
  				break; 
  			default:
  				get(key);
  				break;
  				
  		}
  		
  	}
  
	@BenchmarkOptions(benchmarkRounds = 1000000, warmupRounds=0, concurrency=10)
  	@Test
  	public void write1Read9() {
  		String key = "test-" + (rndGen.nextInt(entries*2)+1);
  		
  		int what = rndGen.nextInt(10);
  		
  		switch (what) {
			case 0: 
  				put(key);
  				break; 
  			default:
  				get(key);
  				break;
  				
  		}
  		
  	}
	private void get(String key) {
  		Pointer p = map.get(key);
		read.incrementAndGet();
  		if (p != null) {
  			got.incrementAndGet();
  			byte [] payload = mem.retrieve(p);
  	  		if (key.equals(new String(payload)))
  	  			good.incrementAndGet();
  	  		else
  	  			bad.incrementAndGet();
  		} else {
  			missed.incrementAndGet();
  		}
	}

	Random rndGen = new Random();
	
	@Rule
	public MethodRule benchmarkRun = new BenchmarkRule();


	private static Logger logger = LoggerFactory.getLogger(ConcurrentTests.class);

	@BeforeClass
	@AfterClass
	public static void dump() {
		logger.info("off-heap allocated: " + Ram.inMb(mem.capacity()));
		logger.info("off-heap used:      " + Ram.inMb(mem.used()));
		logger.info("heap - max: " + Ram.inMb(Runtime.getRuntime().maxMemory()));
		logger.info("heap - allocated: " + Ram.inMb(Runtime.getRuntime().totalMemory()));
		logger.info("heap - free : " + Ram.inMb(Runtime.getRuntime().freeMemory()));
		logger.info("************************************************");
		logger.info("entries: " + entries);
		logger.info("inserted: " + map.size());
		logger.info("reads: " + read);
		logger.info("count: " + count);
		logger.info("got: " + got);
		logger.info("missed: " + missed);
		logger.info("good: " + good);
		logger.info("bad: " + bad);
		logger.info("************************************************");
	}

}
	
	

