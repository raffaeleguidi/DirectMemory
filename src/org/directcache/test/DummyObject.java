package org.directcache.test;

import java.io.Serializable;

public class DummyObject implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2391812518800537726L;
	private String name;
	public DummyObject(String name) {
		this.name = name;
	}
	public DummyObject(String key, int size) {
		this.name = key;
		obj = new Object[size];
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getName() {
		return name;
	}

	public Object[] obj;
	
	public byte[] PayLoad;
} 
