package org.directmemory.demo;

import org.directmemory.misc.DummyPojo;
import org.directmemory.storage.Storage;

public class MyThread extends Thread {
	public Storage storage;
	public DummyPojo pojo;
	public int index;
	public String name;

	public MyThread() {
		super();
	}
	public MyThread(ThreadGroup group, String name) {
		super(group, name);
		this.name = name;
	}
}
