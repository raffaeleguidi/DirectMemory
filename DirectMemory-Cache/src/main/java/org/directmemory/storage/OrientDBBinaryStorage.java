package org.directmemory.storage;

import java.io.File;
import java.io.IOException;
import java.util.Formatter;

import org.directmemory.cache.CacheEntry;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public class OrientDBBinaryStorage extends Storage {
	
	public String baseDir = "data";
	public String binaryDir = "binary";
	
	ODatabaseDocument database;
	
	private void createDatabase() {
        File base = new File(baseDir + "\\" + binaryDir);
		if (base.exists()) {
			logger.info("Base folder: " + base.getPath() + " checked ok");
		} else if (base.mkdir()) {
			logger.info("Base folder: " + base.getPath() + " created");
		} else {
			logger.error("Could not create base directory: " + base.getPath());
		}
		database = new ODatabaseDocumentTx("local:" + base.getAbsolutePath() + "\\data");
		database.delete();
		database.create();
		logger.info("OrientDB document database: " + database.getURL() + " created");
	}
	
	@Override
	public void dispose() {
		super.dispose();
		database.close();
		logger.debug("OrientDB database closed");
	}
	
	public OrientDBBinaryStorage() {
		try {
	        File base = new File(baseDir);
	        if (!base.exists()) {
	        	base.mkdir();
	        }
			createDatabase();
		} catch (Exception e) {
			logger.error("OrientDB database: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	protected void finalize() throws Throwable
	{
		database.close();
		super.finalize(); 
	} 
	
	@Override
	protected boolean moveIn(CacheEntry entry) {
		// try to delete it just to be sure 
//		deleteFromDb(entry);			
		
		byte [] buffer = entry.bufferData();
		if (buffer == null) {
			// it seems entry was in heap
			try {
				buffer = serializer.serialize(entry.object, entry.object.getClass());
			} catch (IOException e) {
				logger.error("error serializing entry " + entry.key);
				e.printStackTrace();
			}
		} 
		
		if (database.isClosed()) {
			createDatabase();
		}
		
        ORecordBytes record = new ORecordBytes(database, buffer);
		record.save();

		logger.debug("succesfully stored entry " + entry.key + " to database " + database.getURL());
		
		entry.identity = record.getIdentity();
		entry.array = null; // just to be sure
		entry.object = null;
		entry.size = buffer.length;

		return true;
	}

	@Override
	public boolean moveToHeap(CacheEntry entry) {		
		try {
			ORecordBytes record = new ORecordBytes(database, (ORID) entry.identity);
			record.load();
			entry.array = record.toStream();
			entry.object = serializer.deserialize(entry.array, entry.clazz());
			entry.identity = null;
			entry.array = null;
			logger.debug("succesfully restored entry " + entry.key + " from database " + database.getURL());
			record.delete();
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return false;
	}

	@Override
	public CacheEntry delete(String key) {
		CacheEntry entry = entries.get(key);
		if (entry != null) {
			deleteFromDb(entry);			
			logger.debug("entry " + key + " deleted from the database");
			return super.delete(key);
		} else {
			logger.debug("no entry " + key + " found in the database");
			return null;			
		} 
	}

	private void deleteFromDb(CacheEntry entry) {
		try {
//			OCommandSQL sql = new OCommandSQL("delete * from Entry where key = '" + entry.key + "'");
//			docDb.command(sql);
			ODocument doc = database.load((ORID) entry.identity);
			doc.delete();
			entry.identity = null;
		} catch (Exception e) {
			logger.error("error deleting previous entry with key " + entry.key);
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString() {
		return new Formatter()
					.format(
							"OrientDB: entries %1d", 
							entries.size()
							)
					.toString();
	}
}
