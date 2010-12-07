package org.directcache.test;

import org.directcache.DirectCache;
import org.junit.Test;


public class FiltersTest {
	@Test
	public void prova() throws Exception {
		DirectCache cache = new DirectCache(1);
		cache.storeObject("test", "ciao", 1);
	}
}
