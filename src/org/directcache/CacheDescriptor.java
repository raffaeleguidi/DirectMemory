package org.directcache;

public class CacheDescriptor {
	public CacheDescriptor(String key, int size, int position) {
		this.key = key;
		this.size = size;
		this.position = position;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public int getPosition() {
		return position;
	}
	public void setPosition(int position) {
		this.position = position;
	}
	String key;
	int size;
	int position;
}
