package org.directcache.exceptions;

public class DirectCacheException extends Exception {

	public DirectCacheException(String error) {
		super(error);
	}

	public DirectCacheException(String error, int exceding) {
		super(error);
		this.exceeding = exceding;
	}

	public int exceeding = 0;
	/**
	 * 
	 */
	private static final long serialVersionUID = 7949226101824404825L;

}
