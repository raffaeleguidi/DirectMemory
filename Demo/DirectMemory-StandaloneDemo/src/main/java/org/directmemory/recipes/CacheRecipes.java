package org.directmemory.recipes;

import org.directmemory.cache.CacheManager;
import org.directmemory.measures.Ram;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.directmemory.storage.HeapStorage;
import org.directmemory.storage.OffHeapStorage;
import org.directmemory.storage.OrientDBStorage;
import org.directmemory.supervisor.SimpleSupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheRecipes {
	
	private static Logger logger=LoggerFactory.getLogger(CacheRecipes.class);

	public static CacheManager MixAndMatch() {
		int entriesLimit = 10000; 
		int pageSize=Ram.Mb(512); 
		int maxPages=1; 
		int pstuffBufferSizeInKb = 10;
		
		logger.info("Cache initialization started");
		CacheManager cache = new CacheManager();
		cache.addStorage(new HeapStorage(entriesLimit));
		cache.addStorage(new OffHeapStorage(pageSize, maxPages));
		cache.addStorage(new OrientDBStorage());
		cache.setSerializer(new ProtoStuffSerializer(Ram.Kb(pstuffBufferSizeInKb)));
		cache.setSupervisor(new SimpleSupervisor());
		logger.info("Cache initialization ok");
		return cache;
	}
	public static CacheManager CreateYourOwn(int entriesLimit, int pageSize, int maxPages, int pstuffBufferSizeInKb) {
		logger.info("Cache initialization started");
		CacheManager cache = new CacheManager();
		cache.addStorage(new HeapStorage(entriesLimit));
		cache.addStorage(new OffHeapStorage(pageSize, maxPages));
		cache.addStorage(new OrientDBStorage());
		cache.setSerializer(new ProtoStuffSerializer(Ram.Kb(pstuffBufferSizeInKb)));
		cache.setSupervisor(new SimpleSupervisor());
		logger.info("Cache initialization ok");
		return cache;
	}
}
