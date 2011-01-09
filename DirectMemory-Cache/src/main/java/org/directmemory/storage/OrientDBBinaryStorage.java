package org.directmemory.storage;

import java.io.File;
import java.io.IOException;

import org.directmemory.cache.CacheEntry;

import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public class OrientDBBinaryStorage extends Storage {
	
	public String baseDir = "orientdb";
	
	ODatabaseBinary db;
	
	private void createBinaryDb() {
        File base = new File(baseDir + "\\bin");
		if (base.exists()) {
			logger.info("Base folder: " + base.getPath() + " checked ok");
		} else if (base.mkdir()) {
			logger.info("Base folder: " + base.getPath() + " created");
			return;
		} else {
			logger.error("Could not create base directory: " + base.getPath());
		}
		db = new ODatabaseBinary("local:" + base.getAbsolutePath() + "\\data");
		db.delete();
		db.create();
		logger.info("OrientDB binary database: " + db.getURL() + " created");
	}
	

	
	public OrientDBBinaryStorage() {
		try {
	        File base = new File(baseDir);
	        if (!base.exists()) {
	        	base.mkdir();
	        }
			createBinaryDb();
		} catch (Exception e) {
			logger.error("OrientDB database: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	protected void finalize() throws Throwable
	{
		db.close();
		super.finalize(); 
	} 
	
	@Override
	protected boolean moveIn(CacheEntry entry) {
		// try to delete it just to be sure 
		try {
			ORecordBytes recBytes = db.load((ORID) entry.identity);
			recBytes.delete();
			entry.identity = null;
		} catch (Exception e1) {
			logger.error("error deleting previous entry with key " + entry.key);
			e1.printStackTrace();
		}
		
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
		
		ORecordBytes recBytes = new ORecordBytes(db, buffer);
		try {
			logger.debug("record is new? " + recBytes.getIdentity().isNew());
			recBytes.setDatabase(db);
			recBytes.save();
			logger.debug("record is valid? " + recBytes.getIdentity().isValid());
			if (!recBytes.getIdentity().isValid()) {
				logger.debug("cannot store entry " + entry.key + " to database " + db.getURL());
				return false;
			}
			entry.identity = recBytes.getIdentity();
		} catch (Exception e) {
			logger.error("error saving buffer for entry " + entry.key);
			e.printStackTrace();
		}
		
		logger.debug("succesfully stored entry " + entry.key + " to database " + db.getURL());
		
		entry.array = null; // just to be sure
		entry.object = null;
		entry.size = buffer.length;

		return true;
	}

	@Override
	public boolean moveToHeap(CacheEntry entry) {		
		try {
			ORecordBytes recBytes = db.load((ORID) entry.identity);
			if (recBytes == null) {
				logger.error("row with ORID " + entry.identity + " not found");
				return false;
			}
			entry.object = serializer.deserialize(recBytes.toStream(), entry.clazz());
			recBytes.delete();
			entry.identity = null;
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public void reset() {
		super.reset();
		db.close();
		db.delete();
		db.open("admin", "admin");
		logger.debug("OrientDB database deleted and created");
	}
	
	@Override
	public CacheEntry delete(String key) {
		CacheEntry entry = entries.get(key);
		if (entry != null) {
			ORecordBytes recBytes = db.load((ORID) entry.identity);
			recBytes.delete();
			entry.identity = null;
			logger.debug("entry " + key + " deleted from the database");
			return super.delete(key);
		} else {
			logger.debug("no entry " + key + " found in the database");
			return null;			
		} 
	}
}
