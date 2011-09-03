package org.directmemory.measures;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.directmemory.misc.Format;

public class Monitor {
	private AtomicLong hits = new AtomicLong(0);
	private long totalTime = 0;
	private long min = -1;
	private long max = -1;
	private String name;
	
	public static Map<String, Monitor> monitors = new HashMap<String, Monitor>();
	
	public static Monitor get(String key) {
		Monitor mon = monitors.get(key);
		if (mon == null) {
			mon = new Monitor(key);
			monitors.put(key, mon);
		}
		return mon;
	}
	
	public Monitor(String name) {
		this.name = name;
	}
	
	public long start() {
		return System.nanoTime();
	}
	public long stop(long begunAt) {
		hits.incrementAndGet();
		final long lastAccessed = System.nanoTime();
		final long elapsed = lastAccessed - begunAt;
		totalTime+=elapsed;
		if (elapsed > max && hits.get() > 0) max = elapsed;
		if (elapsed < min && hits.get() > 0) min = elapsed;
		return elapsed;
	}
	public long hits() {
		return hits.get();
	}
	public long totalTime() {
		return totalTime;
	}
	public long average() {
		return totalTime/hits.get();
	}
	public String toString() {
		return Format.it("%1$s - hits: %2$d, average: %3$s ms, total: %4$s seconds", 
								name, 
								hits.get(), 
								new DecimalFormat("####.##").format((double)average()/1000000), 
								new DecimalFormat("####.##").format((double)totalTime/1000000000)
						);
	}
}
