package org.directcache.exceptions;

public class BufferTooFragmentedException extends DirectCacheException {

	public BufferTooFragmentedException(String error) {
		super(error);
	}

	public BufferTooFragmentedException(String string, int length) {
		super(string, length);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 6791714230509252068L;

}
