package org.directmemory.misc;
import java.io.IOException;
import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager;
import org.directmemory.serialization.StandardSerializer;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

public class Starter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	
	static int serBufferSize = 512;
	
	public static StandardSerializer serializer = new StandardSerializer();
	
	public static void main(String[] args) {
		CacheManager cache = new CacheManager(100, CacheManager.MB(5), 1);
		cache.put("test", "questo è un test");
		cache.reset();
	}

	
	public static void oldmain(String[] args) throws IOException {
		
		int cacheSize = 5 * 1024 * 1024;
		int objectSize = 2 * 1024;
		
		Stopwatch setupMon = SimonManager.getStopwatch("setupMon");
		Stopwatch readerMon = SimonManager.getStopwatch("readerMon");
		Stopwatch writerMon = SimonManager.getStopwatch("writerMon");
		Stopwatch serializeMon = SimonManager.getStopwatch("serializeMon");
		Stopwatch deserializeMon = SimonManager.getStopwatch("deserializeMon");
		
//		long started = Calendar.getInstance().getTimeInMillis();
		System.out.println("started one thread test for a " + (cacheSize / 1024 / 1024) + "mb cache" + " objectSize=" + objectSize + " serialization buffer=" + serBufferSize);
		
		ByteBuffer buffer = ByteBuffer.allocateDirect(cacheSize);

		Map<String, CacheEntry> map = new ConcurrentHashMap<String, CacheEntry>();
		
		int memoryUsed = 0;
		int count = 0;
		
		while (memoryUsed <= cacheSize - objectSize) {
			CacheEntry entry = new CacheEntry();
			entry.key = "key"+(count++);
			DummyPojo obj = new DummyPojo(entry.key, objectSize);
			
			Split serSplit = serializeMon.start();
			byte[] src = serialize(obj);
			serSplit.stop();
			Split split = setupMon.start();
			entry.size = src.length;
			entry.buffer = buffer.duplicate();
			entry.position = memoryUsed;
			entry.buffer.position(memoryUsed);
			
			
			try {
				entry.buffer.put(src);
			} catch (BufferOverflowException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			map.put(entry.key, entry);
			memoryUsed+=entry.size;
			split.stop();
		}
				
        System.out.println("setupMon: " + setupMon);
		System.out.println("average append duration: " + (double)(setupMon.getTotal() / setupMon.getCounter())/1000000 + "ms");
		System.out.println("average serialize duration: " + (double)(serializeMon.getTotal() / serializeMon.getCounter())/1000000 + "ms");

		int numOfThreads = 10;
		
		System.out.println("multithread test - starting " + numOfThreads + " threads");
		Thread last = null;
		
		for (int i = 0; i < numOfThreads; i++) {
			Reader reader = new Reader();
			reader.map = map;
			reader.start();
			last = reader;
			Thread.yield();
			Writer writer = new Writer();
			writer.map = map;
			writer.start();
			Thread.yield();
			last = writer;
		}

		while (last.isAlive())
			Thread.yield();

		System.out.println(readerMon);
        System.out.println(writerMon);
		System.out.println(deserializeMon);
        System.out.println(serializeMon);
		System.out.println("average read duration: " + (double)(readerMon.getTotal() / readerMon.getCounter())/1000000 + "ms");
		System.out.println("average write duration: " + (double)(writerMon.getTotal() / writerMon.getCounter())/1000000 + "ms");
		System.out.println("average deserialize duration: " + (double)(deserializeMon.getTotal() / deserializeMon.getCounter())/1000000 + "ms");
		System.out.println("average serialize duration: " + (double)(serializeMon.getTotal() / serializeMon.getCounter())/1000000 + "ms");
	}

	public static byte[] serialize(DummyPojo obj) throws IOException {
		 Schema<DummyPojo> schema = RuntimeSchema.getSchema(DummyPojo.class);
		 final LinkedBuffer buffer = LinkedBuffer.allocate(serBufferSize);
		 byte[] protostuff = null;

		 try {
			 protostuff = ProtostuffIOUtil.toByteArray(obj, schema, buffer);
		 } finally {
			 buffer.clear();
		 }		

		 return protostuff;
	}

	public static byte[] serializeOld(DummyPojo obj) throws IOException {
		byte[] src = serializer.serialize((Serializable) obj, null);
		return src;
	}

	public static DummyPojo deserialize(byte[] source) throws IOException, ClassNotFoundException {
		final DummyPojo obj2 = new DummyPojo();
		Schema<DummyPojo> schema2 = RuntimeSchema.getSchema(DummyPojo.class);
		ProtostuffIOUtil.mergeFrom(source, obj2, schema2);
		return obj2;
	}

	public static DummyPojo deserializeOld(byte[] dest) throws IOException, ClassNotFoundException {
		return (DummyPojo) serializer.deserialize(dest, null);
	}
}

/*
started one thread test for a 5mb cache with 2048 bytes payload - protostuff
setupMon: Simon Stopwatch: [setupMon INHERIT] total 67.0 ms, counter 2540, max 243 us, min 11.7 us
average append duration: 0.026386ms
average serialize duration: 0.104292ms
multithread test - starting 10 threads
Simon Stopwatch: [readerMon INHERIT] total 4.57 s, counter 50709, max 136 ms, min 3.35 us
Simon Stopwatch: [writerMon INHERIT] total 129 ms, counter 25310, max 9.63 ms, min 2.24 us
Simon Stopwatch: [deserializeMon INHERIT] total 6.28 s, counter 50712, max 138 ms, min 10.1 us
Simon Stopwatch: [serializeMon INHERIT] total 1.94 s, counter 27853, max 69.9 ms, min 18.7 us
average read duration: 0.090161ms
average write duration: 0.005097ms
average deserialize duration: 0.123731ms
average serialize duration: 0.069549ms

started one thread test for a 5mb cache objectSize=2048 serbuffer=1024
setupMon: Simon Stopwatch: [setupMon INHERIT] total 64.3 ms, counter 2540, max 479 us, min 10.3 us
average append duration: 0.025331ms
average serialize duration: 0.107786ms
multithread test - starting 10 threads
Simon Stopwatch: [readerMon INHERIT] total 5.68 s, counter 49808, max 196 ms, min 3.63 us
Simon Stopwatch: [writerMon INHERIT] total 207 ms, counter 24408, max 16.6 ms, min 2.24 us
Simon Stopwatch: [deserializeMon INHERIT] total 4.17 s, counter 49809, max 105 ms, min 9.50 us
Simon Stopwatch: [serializeMon INHERIT] total 2.11 s, counter 26949, max 105 ms, min 26.5 us
average read duration: 0.114032ms
average write duration: 0.008465ms

serbuffer 512
average deserialize duration: 0.083712ms
average serialize duration: 0.078181ms
serbuffer 512
average deserialize duration: 0.049835ms
average serialize duration: 0.070213ms

started one thread test for a 5mb cache objectSize=2048 serialization buffer=2048
setupMon: Simon Stopwatch: [setupMon INHERIT] total 65.3 ms, counter 2540, max 517 us, min 12.0 us
average append duration: 0.025704ms
average serialize duration: 0.108581ms
multithread test - starting 10 threads
Simon Stopwatch: [readerMon INHERIT] total 6.66 s, counter 50529, max 133 ms, min 3.63 us
Simon Stopwatch: [writerMon INHERIT] total 390 ms, counter 25129, max 48.5 ms, min 2.24 us
Simon Stopwatch: [deserializeMon INHERIT] total 3.60 s, counter 50532, max 93.3 ms, min 9.50 us
Simon Stopwatch: [serializeMon INHERIT] total 1.96 s, counter 27672, max 85.7 ms, min 26.0 us
average read duration: 0.131856ms
average write duration: 0.015501ms
average deserialize duration: 0.071205ms
average serialize duration: 0.070855ms

started one thread test for a 5mb cache objectSize=2048 serialization buffer=2048
setupMon: Simon Stopwatch: [setupMon INHERIT] total 66.8 ms, counter 2540, max 490 us, min 11.2 us
average append duration: 0.026308ms
average serialize duration: 0.110396ms
multithread test - starting 10 threads
Simon Stopwatch: [readerMon INHERIT] total 4.42 s, counter 44102, max 179 ms, min 3.35 us
Simon Stopwatch: [writerMon INHERIT] total 229 ms, counter 19408, max 20.8 ms, min 2.24 us
Simon Stopwatch: [deserializeMon INHERIT] total 6.90 s, counter 44098, max 146 ms, min 9.78 us
Simon Stopwatch: [serializeMon INHERIT] total 1.61 s, counter 21948, max 71.5 ms, min 26.3 us
average read duration: 0.100195ms
average write duration: 0.011819ms
average deserialize duration: 0.156544ms
average serialize duration: 0.073541ms



started one thread test for a 5mb cache with 200 bytes payload - protostuff
setupMon: Simon Stopwatch: [setupMon INHERIT] total 107 ms, counter 24324, max 822 us, min 2.51 us
average append duration: 0.004417ms
average serialize duration: 0.011701ms
multithread test - starting 10 threads
Simon Stopwatch: [readerMon INHERIT] total 36.1 s, counter 481841, max 261 ms, min 1.96 us
Simon Stopwatch: [writerMon INHERIT] total 5.78 s, counter 238603, max 94.9 ms, min 1.68 us
Simon Stopwatch: [deserializeMon INHERIT] total 18.5 s, counter 481846, max 188 ms, min 3.63 us
Simon Stopwatch: [serializeMon INHERIT] total 9.05 s, counter 262932, max 73.8 ms, min 5.31 us
average read duration: 0.074986ms
average write duration: 0.024217ms
average deserialize duration: 0.038346ms
average serialize duration: 0.034423ms



started one thread test for a 5mb cache with 2048 bytes payload - serialization
setupMon: Simon Stopwatch: [setupMon INHERIT] total 69.5 ms, counter 2442, max 132 us, min 11.5 us
average append duration: 0.028475ms
average serialize duration: 0.127725ms
multithread test - starting 10 threads
Simon Stopwatch: [readerMon INHERIT] total 4.96 s, counter 48840, max 120 ms, min 3.63 us
Simon Stopwatch: [writerMon INHERIT] total 179 ms, counter 24420, max 12.0 ms, min 2.24 us
Simon Stopwatch: [deserializeMon INHERIT] total 15.2 s, counter 48840, max 138 ms, min 43.6 us
Simon Stopwatch: [serializeMon INHERIT] total 2.26 s, counter 26862, max 48.8 ms, min 29.6 us
average read duration: 0.101616ms
average write duration: 0.007332ms
average deserialize duration: 0.310448ms
average serialize duration: 0.084148ms


started one thread test for a 5mb cache with 200 bytes payload - serialization
setupMon: Simon Stopwatch: [setupMon INHERIT] total 84.5 ms, counter 17513, max 311 us, min 2.79 us
average append duration: 0.004823ms
average serialize duration: 0.017408ms
multithread test - starting 10 threads
Simon Stopwatch: [readerMon INHERIT] total 58.4 s, counter 348653, max 261 ms, min 2.23 us
Simon Stopwatch: [writerMon INHERIT] total 2.84 s, counter 173524, max 108 ms, min 1.96 us
Simon Stopwatch: [deserializeMon INHERIT] total 35.2 s, counter 348656, max 230 ms, min 19.3 us
Simon Stopwatch: [serializeMon INHERIT] total 6.24 s, counter 191040, max 83.3 ms, min 10.1 us
average read duration: 0.167602ms
average write duration: 0.016341ms
average deserialize duration: 0.101026ms
average serialize duration: 0.03265ms

*/