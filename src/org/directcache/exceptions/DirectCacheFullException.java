package org.directcache.exceptions;

public class DirectCacheFullException extends DirectCacheException {

	public DirectCacheFullException(String error) {
		super(error);
	}

	public DirectCacheFullException(String string, int length) {
		super(string, length);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 6791714230509252068L;

}
