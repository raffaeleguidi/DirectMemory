package org.directmemory.misc;
import java.io.IOException;
import java.util.Map;

import org.directmemory.CacheEntry;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;


public class Writer extends Thread {
	
	public Map<String, CacheEntry> map;
	
	@Override
	public void run() {
        Stopwatch readerMon = SimonManager.getStopwatch("readerMon");
        Stopwatch writerMon = SimonManager.getStopwatch("writerMon");
		Stopwatch serializeMon = SimonManager.getStopwatch("serializeMon");
		Stopwatch deserializeMon = SimonManager.getStopwatch("deserializeMon");

		int count = 0;

		for (CacheEntry entry : map.values()) {
			Split split = readerMon.start();
			byte[] dest = new byte[entry.size];
			synchronized (entry.buffer) {
				entry.buffer.position(entry.position);
				entry.buffer.get(dest);
			}
			split.stop();
			DummyPojo obj = null;
			try {
				Split deserSplit = deserializeMon.start();
				obj = Starter.deserialize(dest);
				deserSplit.stop();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (StringIndexOutOfBoundsException e1) {
				e1.printStackTrace();				
			}
			
			byte[] src = null;
			try {
				Split serSplit = serializeMon.start();
				src = Starter.serialize(obj);
				serSplit.stop();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Split split2 = writerMon.start();
			synchronized (entry.buffer) {
				entry.buffer.position(entry.position);
				entry.buffer.put(src);
			}
			count++;
			split2.stop();
		}
		
//		long elapsed = Calendar.getInstance().getTimeInMillis()-started;
//		System.out.println("writer did " + count + " writes in " + (elapsed) + " msecs - " + ((double)count / (double)elapsed) + " ops per msec");
//		System.out.println("writer ops took " + ((double)elapsed / (double)count) + " msecs each");
	}
}
