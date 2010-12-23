package org.directmemory.misc;
import java.io.Serializable;


public class DummyPojo implements Serializable {
	/**
	 * A dummy pojo implementation for test purposes
	 */
	private static final long serialVersionUID = 1L;
	public DummyPojo() {
		
	}
	
	public DummyPojo(String name, int size) {
		this.name = name;
		this.size = size;
		
		for (int i = 0; i < size; i++) {
			payLoad += "x";
		}
		
	}
	public String name;
	public int size;
	public String payLoad = "";
}
