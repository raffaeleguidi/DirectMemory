package org.directmemory.measures;

public class Sizing {
	public static int Gb(double giga) {
		return (int)giga * 1024 * 1024 * 1024;
	}

	public static int Mb(double mega) {
		return (int)mega * 1024 * 1024;
	}
	
	public static int Kb(double kilo) {
		return (int)kilo * 1024;
	}
	public static int unlimited() {
		return -1;
	}
}
