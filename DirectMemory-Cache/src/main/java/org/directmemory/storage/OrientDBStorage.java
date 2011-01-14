package org.directmemory.storage;

import java.io.File;
import java.io.IOException;
import java.util.Formatter;

import org.directmemory.cache.CacheEntry;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OrientDBStorage extends Storage {
	
	public String baseDir = "orientdb";
	public String docDir = "db";
	
	ODatabaseDocumentTx docDb;
	
	private void createDocDb() {
        File base = new File(baseDir + "\\" + docDir);
		if (base.exists()) {
			logger.info("Base folder: " + base.getPath() + " checked ok");
		} else if (base.mkdir()) {
			logger.info("Base folder: " + base.getPath() + " created");
		} else {
			logger.error("Could not create base directory: " + base.getPath());
		}
		docDb = new ODatabaseDocumentTx("local:" + base.getAbsolutePath() + "\\data");
		docDb.delete();
		docDb.create();
		logger.info("OrientDB document database: " + docDb.getURL() + " created");
	}
	
	@Override
	public void dispose() {
		super.dispose();
		docDb.close();
		logger.debug("OrientDB database closed");
	}
	
	public OrientDBStorage() {
		try {
	        File base = new File(baseDir);
	        if (!base.exists()) {
	        	base.mkdir();
	        }
			createDocDb();
		} catch (Exception e) {
			logger.error("OrientDB database: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	protected void finalize() throws Throwable
	{
		docDb.close();
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
		
		if (docDb.isClosed()) {
			createDocDb();
		}
		
		ODocument doc = new ODocument(docDb, "Entry");
		doc.field("buffer", buffer, OType.BINARY);
		doc.field("key", entry.key );
		doc.field("expiresOn" , entry.expiresOn, OType.DATE);
		doc.field("clazz" , entry.clazz().toString());
		doc.save();

		logger.debug("succesfully stored entry " + entry.key + " to database " + docDb.getURL());
		
		entry.identity = doc.getIdentity();
		entry.array = null; // just to be sure
		entry.object = null;
		entry.size = buffer.length;

		return true;
	}

	@Override
	public boolean moveToHeap(CacheEntry entry) {		
		try {
			ODocument doc = docDb.load((ORID) entry.identity);
			entry.array = doc.field("buffer", OType.BINARY);
			entry.object = serializer.deserialize(entry.array, entry.clazz());
			entry.identity = null;
			entry.array = null;
			logger.debug("succesfully restored entry " + entry.key + " from database " + docDb.getURL());
			doc.delete();
			return true;
//			List<ODocument> result = docDb.query(
//					  new OSQLSynchQuery<ODocument>("select * from Entry where key = '" + entry.key + "'"));
//			if (result.size() == 1) {
//				ODocument doc = result.get(0);
//				entry.array = doc.field("buffer", OType.BINARY);
//				entry.object = serializer.deserialize(entry.array, entry.clazz());
//				entry.array = null;
//				logger.debug("succesfully restored entry " + entry.key + " from database " + docDb.getURL());
//				doc.delete();
//				return true;
//			}
		} catch (Exception e) {
			logger.error(e.getMessage());
//			e.printStackTrace();
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
			ODocument doc = docDb.load((ORID) entry.identity);
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
