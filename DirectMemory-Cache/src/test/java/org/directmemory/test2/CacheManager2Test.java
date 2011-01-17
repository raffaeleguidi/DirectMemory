package org.directmemory.test2;

import java.util.Calendar;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager2;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.junit.Test;

public class CacheManager2Test {
	
	@Test
	public void only10puts() {
		long startedAt = Calendar.getInstance().getTimeInMillis();
		CacheManager2 cache = new CacheManager2();
		cache.limit(10);
		for (int i = 0; i < 50; i++) {
			DummyPojo pojo = new DummyPojo("test" + i, Ram.Kb(2));
			cache.put(pojo.name, pojo);
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("ended in " + (finishedAt-startedAt) + " milliseconds");
	}
	
	@Test
	public void onlyCreates() {
		long startedAt = Calendar.getInstance().getTimeInMillis();
		CacheManager2 cache = new CacheManager2();
		cache.limit(100);
		for (int i = 0; i < 50000; i++) {
			DummyPojo pojo = new DummyPojo("test" + i, Ram.Kb(2));
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("ended in " + (finishedAt-startedAt) + " milliseconds");
	}
	@Test
	public void manyPuts() {
		long startedAt = Calendar.getInstance().getTimeInMillis();
		CacheManager2 cache = new CacheManager2();
		cache.limit(100);
		for (int i = 0; i < 50000; i++) {
			DummyPojo pojo = new DummyPojo("test" + i, Ram.Kb(2));
			cache.put(pojo.name, pojo);
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("ended in " + (finishedAt-startedAt) + " milliseconds");
	}
	
}
