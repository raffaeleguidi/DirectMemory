package org.directmemory.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.directmemory.cache.CacheManager;
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
	static Properties properties = new Properties();
	
	static void loadProperties(String fileName) {
		try {
			File file = new File(fileName + ".properties");
			logger.info("loading file '" + file.getAbsolutePath() + "'");
		    properties.load(new FileInputStream(file));
			logger.info("loaded file '" + fileName + ".properties'");
		} catch (IOException e) {
			logger.warn("file '" + fileName + ".properties' not found, using defaults");
		}
	}
	
    public static void main( String[] args ) throws InterruptedException, IOException
    {
    	logger.info("DirectMemory Cache - Standalone Demo Starting");
    	loadProperties("conf/demo");
    	int howMany = new Integer(properties.getProperty("demo.numberOfEntries","10000")).intValue();
    	int entriesInHeap = new Integer(properties.getProperty("demo.entriesInHeap","1000")).intValue();
		int payloadSize = Ram.Kb(new Integer(properties.getProperty("demo.payloadInKb","2")).intValue());
		int pstuffBufferSizeInKb = Ram.Kb(new Integer(properties.getProperty("demo.pstuffBufferSizeInKb","8")).intValue());
		int pageSize = Ram.Mb(new Integer(properties.getProperty("demo.pageSizeInMb","256")).intValue());
		int maxPages = new Integer(properties.getProperty("demo.maxPages","1")).intValue();
		int batchSize = new Integer(properties.getProperty("demo.batchSize","100")).intValue();
		int logEvery = new Integer(properties.getProperty("demo.logEvery","100")).intValue();
		int showStatusEvery = new Integer(properties.getProperty("demo.showStatusEvery","1000")).intValue();
		
		logger.info("demo.numberOfEntries=" + howMany);
		logger.info("demo.entriesInHeap=" + entriesInHeap);
		logger.info("demo.payloadInKb=" + payloadSize);
		logger.info("demo.pstuffBufferSizeInKb=" + pstuffBufferSizeInKb);
		logger.info("demo.pageSizeInMb=" + pageSize);
		logger.info("demo.maxPages=" + maxPages);
		logger.info("demo.batchSize=" + batchSize);
		logger.info("demo.logEvery=" + logEvery);
		logger.info("demo.showStatusEvery=" + showStatusEvery);
		
    	cache = CacheRecipes.CreateYourOwn(entriesInHeap, pageSize, maxPages, pstuffBufferSizeInKb, batchSize);        
        logger.info("Cache initialized - " + cache.toString());
        
        int partial = 0;
        int partialShow = 0;
        
        logger.info("Starting inserting " + howMany + " entries");
        for (int i = 0; i < howMany; i++) {
			cache.put("test" + i, new DummyPojo("test" + i, payloadSize));
			if (partial++ == logEvery) {
				logger.debug("entry " + i + " inserted");
				partial = 0;
			}
			if (partialShow++ == showStatusEvery) {
		        logger.info(cache.toString());
		        logger.info(CacheManager.getTimings());
				partialShow = 0;
			}
		}
        logger.info("Cache after " + howMany + " inserts - " + cache.toString());
        logger.info(CacheManager.getTimings());

        logger.info("Beginning check reads");
        for (int i = 0; i < howMany; i++) {
			DummyPojo check = (DummyPojo)cache.get("test" + i);
			if (partial++ == logEvery) {
				logger.debug("entry " + i + " read");
				partial = 0;
			}
			if (partialShow++ == showStatusEvery) {
		        logger.info(cache.toString());
		        logger.info(CacheManager.getTimings());
				partialShow = 0;
			}
			if (!check.name.equals("test"+i)) {
		        logger.error("check " + check.name + " doesn't match");
			}
		}
        logger.info("Cache after " + howMany + " reads - " + cache.toString());
        logger.info(CacheManager.getTimings());
        cache.dispose();
    	logger.info("DirectMemory Cache - Goodbye!");
    }
}
