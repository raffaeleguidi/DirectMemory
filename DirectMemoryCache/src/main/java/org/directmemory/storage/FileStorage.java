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
	
	public String baseDir = "data";
	
	public FileStorage() {
		File base = new File(baseDir);
		if (base.exists()) {
			logger.info("Base directory: " + base.getPath() + " checked ok");
		} else if (base.mkdir()) {
			logger.info("Base directory: " + base.getPath() + " created");
			return;
		} else {
			logger.error("Could not create base directory: " + base.getPath());
		}
	}
	

	@Override
	protected boolean store(CacheEntry entry) {
		File output = new File(baseDir + "/" + entry.key + ".object");
		FileOutputStream fos;
		try {
			output.createNewFile();
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
			// also entry.buffer should be reset 
			// but it must be reused - I leave it to the caller
		} catch (FileNotFoundException e) {
			logger.error("file not found");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			logger.error("IO exception");
			e.printStackTrace();
			return false;
		}
		logger.debug("succesfully stored entry " + entry.key + " to disk (" + baseDir + "/" + entry.key + ".object)");
		return true;
	}

	@Override
	protected boolean restore(CacheEntry entry) {
		File input = new File(baseDir + "/" + entry.path);
		FileInputStream fis;
		try {
			fis = new FileInputStream(input);
			entry.array = new byte[entry.size];
			fis.read(entry.array);
			fis.close();
			input.delete();
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
		logger.debug("succesfully restored entry " + entry.key + " from disk (" + baseDir + "/" + entry.key + ".object)");
		return true;
	}

}
