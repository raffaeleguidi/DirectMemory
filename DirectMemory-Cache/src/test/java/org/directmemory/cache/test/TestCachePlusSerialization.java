package org.directmemory.cache.test;

import static org.junit.Assert.*;

import java.util.Random;

import org.directmemory.cache.Cache;
import org.directmemory.measures.Monitor;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;


public class TestCachePlusSerialization {
	@Rule
	public MethodRule benchmarkRun = new BenchmarkRule();

	private static Logger logger = LoggerFactory.getLogger(TestCachePlusSerialization.class);
	
	Random rnd = new Random();
	
	@BeforeClass
	public static void init() {
		logger.info("test started");
		Cache.init(1, Ram.Mb(100));
	}
	
	@AfterClass
	public static void end() {
		Cache.dump();
		Monitor.dump();
		logger.info("test ended");
	}
	
	@BenchmarkOptions(benchmarkRounds = 50000, warmupRounds=0, concurrency=1)
	@Test
	public void basicBench() {
		
		DummyPojo d = new DummyPojo("test-" + rnd.nextInt(100000), 1024 + rnd.nextInt(1024));
		Cache.put(d.name, d);
		DummyPojo d2 = (DummyPojo) Cache.retrieve(d.name);
		
		assertEquals(d.name, d2.name);

	}

}
