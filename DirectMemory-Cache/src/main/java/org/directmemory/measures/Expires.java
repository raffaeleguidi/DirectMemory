package org.directmemory.measures;

public class Expires extends In {

	public Expires(double measure) {
		super(measure);
	}

	public static In in(double measure) {
		return new In(measure);
	}
	public static long never() {
		return -1L;
	}
}
