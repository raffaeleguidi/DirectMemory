package org.directmemory.demo;

import org.directmemory.cache.CacheManager;
import org.directmemory.measures.For;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.recipes.CacheRecipes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class DemoApp 
{
	private static Logger logger=LoggerFactory.getLogger(DemoApp.class);

	static CacheManager cache = null;
	
    public static void main( String[] args ) throws InterruptedException
    {
    	int howMany = 1000000;
		int payloadSize = Ram.Kb(2);
		
    	logger.info("DirectMemory Cache - Standalone Demo Starting");
    	cache = CacheRecipes.MixAndMatch();        
        logger.info("Cache initialized - " + cache.toString());
        
        for (int i = 0; i < howMany; i++) {
			cache.put("test" + i, new DummyPojo("test" + i, payloadSize));
		}
        logger.info("Cache after " + howMany + " inserts - " + cache.toString());
        logger.info(CacheManager.getTimings());

        logger.info("Beginning check reads");
        for (int i = 0; i < howMany; i++) {
			DummyPojo check = (DummyPojo)cache.get("test" + i);
			if (!check.name.equals("test"+i)) {
		        logger.info("check " + check.name + " doesn't match");
			}
		}
        logger.info("Cache after " + howMany + " reads - " + cache.toString());
        logger.info(CacheManager.getTimings());
        cache.dispose();
    	logger.info("DirectMemory Cache - Goodbye!");
    }
}
