package org.directmemory.preliminary.test;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.directmemory.measures.Ram;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

public class PreliminarBenchmarks extends AbstractBenchmark {

	private static Logger logger = LoggerFactory.getLogger(PreliminarBenchmarks.class);
	final static byte payload[] = new byte[1024];

//	@Before
//	@After
	public void cleanup() {
		dump("Before cleanup");
		Runtime.getRuntime().gc();
		dump("After cleanup");
		logger.info("************************************************");
	}

	private void dump(String message) {
		logger.info(message);
		logger.info("Memory - max: " + Ram.inMb(Runtime.getRuntime().maxMemory()));
		logger.info("Memory - allocated: " + Ram.inMb(Runtime.getRuntime().totalMemory()));
		logger.info("Memory - free : " + Ram.inMb(Runtime.getRuntime().freeMemory()));
	}
	
	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void justMap() {
		final Map<String, byte[]> test = Maps.newHashMap();
		long ops = 100000;
		for (int i = 0; i < ops; i++) {
			final String key =  "test-"+i;
			test.put(key, payload.clone());
		}
		logger.info("stored " + Ram.inMb(payload.length * ops));
	}

	@BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
	@Test
	public void oneMillionSmallWithDirectBuffersOneAllocation() {
		
		logger.info("payload is " + payload.length + " bytes");
		final byte payload[] = new byte[500];
		int ops = 1000000;

		pumpWithOneAllocation(ops, payload);
		
	}
	
	@BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
	@Test
	public void lessButLargerWithDirectBuffersOneAllocation() {
		
		logger.info("payload is " + payload.length + " bytes");
		final byte payload[] = new byte[2048];
		int ops = 210000;

		pumpWithOneAllocation(ops, payload);
		
	}
	
	
	/*
	 * 
	 * 
	 * ExecutorService executor = Executors.newCachedThreadPool();
Callable<Object> task = new Callable<Object>() {
   public Object call() {
      return something.blockingMethod();
   }
}
Future<Object> future = executor.submit(task);
try {
   Object result = future.get(5, TimeUnit.SECONDS); 
} catch (TimeoutException ex) {
   // handle the timeout
} finally {
   future.cancel(); // may or may not desire this
}
	 */

	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void withDirectBuffers150k() {
		
		logger.info("payload is " + payload.length + " bytes");
		int ops = 150000;

		pump(ops);
	}

	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void withDirectBuffers180k() {
		
		logger.info("payload is " + payload.length + " bytes");
		int ops = 180000;

		pump(ops);
	}
	
	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void withDirectBuffers150kAgain() {
		
		logger.info("payload is " + payload.length + " bytes");
		int ops = 150000;

		pump(ops);
	}

	@BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
	@Test
	public void testAllocation() {
		
		logger.info("payload is " + payload.length + " bytes");
		logger.info("allocating " + Ram.inMb(payload.length * 200000) );
		ByteBuffer buf = ByteBuffer.allocateDirect(payload.length * 200000);
		assertNotNull(buf);
		logger.info("done");
	}

	
	private void pumpWithOneAllocation(int ops, byte[] payload) {

		ConcurrentMap<String, ByteBuffer> test = new MapMaker()
			.concurrencyLevel(4)
			.maximumSize(ops)
			.expireAfterWrite(10, TimeUnit.MINUTES)
			.makeMap();

		logger.info(Ram.inMb(ops*payload.length) + " in " + ops + " slices to store");
		
		ByteBuffer bulk = ByteBuffer.allocateDirect(ops * payload.length);
		
		double started = System.currentTimeMillis();
		
		for (int i = 0; i < ops; i++) {
			bulk.position(i*payload.length);
			final ByteBuffer buf = bulk.duplicate();
			buf.put(payload);
			test.put("test-"+i, buf);
		}
		
		double finished = System.currentTimeMillis();
		
		logger.info("done in " + (finished-started)/1000 + " seconds");
		
		for (ByteBuffer buf : test.values()) {
			buf.clear();
		}
	}

	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void withDirectBuffers100k() {
		
		logger.info("payload is " + payload.length + " bytes");
		int ops = 100000;
	
		pump(ops);
	}

	private void pump(int ops) {
		ConcurrentMap<String, ByteBuffer> test = new MapMaker()
			.concurrencyLevel(4)
			.maximumSize(ops)
			.expireAfterWrite(10, TimeUnit.MINUTES)
			.makeMap();
	
		logger.info(Ram.inMb(ops*payload.length) + " to store");
		
		double started = System.currentTimeMillis();
		
		for (int i = 0; i < ops; i++) {
			ByteBuffer buf = ByteBuffer.allocateDirect(payload.length);
			buf.put(payload);
			test.put("test-"+i, buf);
		}
		
		double finished = System.currentTimeMillis();
		
		logger.info("done in " + (finished-started)/1000 + " seconds");
		
		for (ByteBuffer buf : test.values()) {
			buf.clear();
		}
	}
	
}
