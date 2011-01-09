package org.directmemory.test;

import org.directmemory.cache.CacheManager;
import org.directmemory.measures.Ram;
import org.directmemory.serialization.StandardSerializer;
import org.directmemory.storage.FileStorage;
import org.directmemory.storage.HeapStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.OrientDBBinaryStorage;
import org.directmemory.storage.OrientDBStorage;
import org.directmemory.supervisor.SimpleSupervisor;
import org.junit.Test;

public class StorageChainTest {

	@Test
	public void startHere() {
		CacheManager cache = new CacheManager();
		cache.addStorage(new HeapStorage());
		cache.addStorage(new OffHeapStorage(Ram.Mb(100), 10));
		cache.addStorage(new FileStorage());
		cache.addStorage(new OrientDBStorage());
		cache.addStorage(new OrientDBBinaryStorage());
		cache.setSerializer(new StandardSerializer());
		cache.setSupervisor(new SimpleSupervisor());
		cache.reset();
	}
}
