package org.directmemory.misc;
import java.io.IOException;
import java.util.Map;

import org.directmemory.CacheEntry;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;


public class Reader extends Thread {
	
	public Map<String, CacheEntry> map;
	
	@Override
	public void run() {
//		long started = Calendar.getInstance().getTimeInMillis();
		
        Stopwatch readerMon = SimonManager.getStopwatch("readerMon");
		Stopwatch deserializeMon = SimonManager.getStopwatch("deserializeMon");

//		System.out.println("reader thread started");
		int count = 0;
		for (CacheEntry entry : map.values()) {
			Split split = readerMon.start();
			//System.out.println(entry.key + " of size" + entry.size);
			byte[] dest = new byte[entry.size];
			synchronized (entry.buffer) {
				entry.buffer.position(entry.position);
				entry.buffer.get(dest);
			}
			split.stop();
			try {
				Split deserSplit = deserializeMon.start();
				@SuppressWarnings("unused")
				DummyPojo obj = Starter.deserialize(dest);
				deserSplit.stop();
//				System.out.println(obj.getName() + " count=" + count);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			count++;
		}
		
//        System.out.println(readerMon.sample());
		
//		long elapsed = Calendar.getInstance().getTimeInMillis()-started;
//		System.out.println("reader did " + count + " reads in " + (elapsed) + " msecs - " + ((double)count / (double)elapsed) + " ops per msec");
//		System.out.println("reader ops took " + ((double)elapsed / (double)count) + " msecs each");
	}
}
