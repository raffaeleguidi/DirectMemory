package org.directmemory.measures;

import java.util.Formatter;

public class Monitor {
	private long hits = 0;
	private long totalTime = 0;
	private long min = -1;
	private long max = -1;
	private String name;
	
	public Monitor(String name) {
		this.name = name;
	}
	
	public long start() {
		return System.nanoTime();
	}
	public long stop(long begunAt) {
		hits++;
		final long lastAccessed = System.nanoTime();
		final long elapsed = lastAccessed - begunAt;
		totalTime+=elapsed;
		if (elapsed > max && hits > 0) max = elapsed;
		if (elapsed < min && hits > 0) min = elapsed;
		return elapsed;
	}
	public long hits() {
		return hits;
	}
	public long totalTime() {
		return totalTime;
	}
	public long average() {
		return totalTime/hits;
	}
	public String toString() {
		return new Formatter().format("%s - hits: %s, average: %ss ns, total: %s seconds", name, hits, average(), ((double)totalTime/1000000000)).toString();
	}
}
