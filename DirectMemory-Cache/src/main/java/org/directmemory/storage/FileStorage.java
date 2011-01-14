package org.directmemory.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Formatter;

import org.directmemory.cache.CacheEntry;

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
	protected boolean moveIn(CacheEntry entry) {
		final File output = new File(baseDir + "/" + entry.key + ".object");
		final FileOutputStream fos;
		try {
			output.createNewFile();
			fos = new FileOutputStream(output);
			final byte[] data =  entry.bufferData();
			if (data == null) {
				// it seems item was in heap
				entry.array = serializer.serialize(entry.object, entry.object.getClass());
				fos.write(entry.array);
				entry.size = entry.array.length;
			} else {
				fos.write(data);
				entry.size = data.length;
			}
			fos.flush();
			fos.close();
			// modify entry
			entry.path = output.getName();
			entry.array = null; // just to be sure
			entry.object = null;
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
		logger.debug("entries=" + entries.size() + " lru=" + lruQueue.size());
		return true;
	}

	@Override
	public boolean moveToHeap(CacheEntry entry) {
		final File input = new File(baseDir + "/" + entry.path);
		final FileInputStream fis;
		try {
			fis = new FileInputStream(input);
			entry.array = new byte[(int)input.length()];
			fis.read(entry.array);
			fis.close();
			entry.object = serializer.deserialize(entry.array, entry.clazz());
			entry.array = null; // just to be sure
//			File file2delete = new File(entry.path);
//			file2delete.delete();
			entry.path = null;
			// removed: this is done by storage
//			remove(entry);
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
		logger.debug("entries=" + entries.size() + " lru=" + lruQueue.size());
		return true;
	}
	@Override
	public void dispose() {
		super.dispose();
		// TODO: a unit test would be useful
		File baseFolder = new File(baseDir);
		for (String fileName : baseFolder.list()) {
			new File(fileName).delete();
		}
		baseFolder.delete();
		logger.debug("file storage disposed");
	}
	
	@Override
	public CacheEntry delete(String key) {
		CacheEntry entry = entries.get(key);
		if (entry != null) {
			File file2delete = new File(entry.path);
			file2delete.delete();
			entry.path = null;
			return super.delete(key);
		}
		return entry;
	}
	
	@Override
	public String toString() {
		return new Formatter()
					.format(
							"Disk: entries %1d", 
							entries.size()
							)
					.toString();
	}}
