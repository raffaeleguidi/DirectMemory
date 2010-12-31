package org.directmemory.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.directmemory.CacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStorage extends Storage {
	
	private static Logger logger=LoggerFactory.getLogger(Storage.class);

	public String baseDir = "data";

	@Override
	protected boolean store(CacheEntry entry) {
		File output = new File("data/" + entry.clazz().getCanonicalName() + "/" + entry.key);
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(output);
			// how can I be sure data is serialized using the same serializer?
			byte[] data =  entry.rawData();
			if (data == null) {
				data = serializer.serialize(entry.object, entry.object.getClass());
			} 
			fos.write(data);
			fos.flush();
			fos.close();
			// modify entry
			entry.path = output.getName();
			entry.array = null; // just to be sure
		} catch (FileNotFoundException e) {
			logger.error("file not found");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			logger.error("IO exception");
			e.printStackTrace();
			return false;
		}
		logger.debug("succesfully stored entry " + entry.key);
		return true;
	}

	@Override
	protected boolean restore(CacheEntry entry) {
		File input = new File(entry.path);
		FileInputStream fis;
		try {
			fis = new FileInputStream(input);
			entry.array = new byte[entry.size];
			fis.read(entry.array);
			fis.close();
			// modify entry
			entry.object = serializer.deserialize(entry.array, entry.clazz());
			entry.path = null;
			entry.array = null; // just to be sure
		} catch (FileNotFoundException e) {
			logger.error("file not found");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			logger.error("IO Exception");
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			logger.error("class not found");
			e.printStackTrace();
		} catch (InstantiationException e) {
			logger.error("instantiation exception");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			logger.error("illegal access exception");
			e.printStackTrace();
		}
		logger.debug("succesfully restored entry " + entry.key);
		return true;
	}

}
