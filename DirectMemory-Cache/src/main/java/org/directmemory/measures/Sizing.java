package org.directmemory.measures;

import java.util.Formatter;

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
	public static String inKb(int i) {		
		return new Formatter().format("%(,.1fKb", (double)i/1024).toString();
	}
	public static String inMb(int i) {		
		return new Formatter().format("%(,.1fMb", (double)i/1024/1024).toString();
	}
	public static String inGb(int i) {		
		return new Formatter().format("%(,.1fKb", (double)i/1024/1024/1024).toString();
	}
}
