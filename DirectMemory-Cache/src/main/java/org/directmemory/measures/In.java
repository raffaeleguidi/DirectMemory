package org.directmemory.measures;

public class In {
	private double measure;
	
	public In(double measure) {
		this.measure = measure;
	}
	
	public long seconds() {
		return seconds(measure);
	}
	
	public long minutes() {
		return minutes(measure);
	}
	
	public long hours() {
		return hours(measure);
	}
	
	public long days() {
		return days(measure);
	}
	
	public static long seconds(double seconds) {
		return (long)seconds * 1000;
	}
	public static long minutes(double minutes) {
		return (long)seconds(minutes * 60);
	}
	public static long hours(double hours) {
		return (long)minutes(hours * 60);
	}
	public static long days(double days) {
		return (long)hours(days * 24);
	}
	
	public static In just(double measure) {
		return new In(measure);
	}
	public static In exactly(double measure) {
		return new In(measure);
	}
	public static In only(double measure) {
		return new In(measure);
	}
}
