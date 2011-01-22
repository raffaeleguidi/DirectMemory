package org.directmemory.store;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheEntry2;
import org.directmemory.serialization.Serializer;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class OrientDBStore extends AbstractQueuedStore {

	@Override
	String storeName() {
		return "OrientDBStore";
	}

	public Serializer serializer;
	
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
        database.declareIntent(new OIntentMassiveInsert());
        database.begin(TXTYPE.NOTX);
		logger.info("OrientDB document database: " + database.getURL() + " created");
	}
	
	@Override
	public void dispose() {
		super.dispose();
		database.commit();
		database.close();
		logger.debug("OrientDB database closed");
	}
	
	@Override
	public CacheEntry2 remove(Object key) {
		CacheEntry2 entry = super.remove(key);
		if (entry != null) {
			// delete from db? no, don't waste time
			entry.identity = null;
		}
		return entry;
	}
	
	public OrientDBStore() {
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
	
	private static final long serialVersionUID = 1L;
	
	@Override
	void asyncPopIn(CacheEntry2 entry) {
		// read the object
		if (entry.inHeap()) {
			try {
				entry.array = serializer.serialize(entry.object, entry.object.getClass());
			} catch (IOException e) {
				logger.error("error serializing entry " + entry.key);
				e.printStackTrace();
			}
			
			if (database.isClosed()) {
				createDatabase();
			}
			
	        ORecordBytes record = new ORecordBytes(database, entry.array);
			record.save();

//				logger.debug("succesfully stored entry " + entry.key + " to database " + database.getURL());
			
			entry.identity = record.getIdentity();
			entry.clazz = entry.object.getClass();
			entry.object = null;
			entry.size = entry.array.length;
			entry.array = null; // just to be sure
			// serialize the object
			// queuedEntry.buffer = getBufferFor(...);
		}
		entry.setStore(this); // done, take it		
		
	}
	

	@Override
	void popOut(CacheEntry2 entry) {
		ORecordBytes record = new ORecordBytes(database, (ORID) entry.identity);
		record.load();
		try {
			entry.object = serializer.deserialize(record.toStream(), entry.clazz());
		} catch (EOFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		entry.identity = null;
		entry.array = null;
//		logger.debug("succesfully restored entry " + entry.key + " from database " + database.getURL());
//		record.delete();
	}

	@Override
	byte[] toStream(CacheEntry entry) {
		throw new NotImplementedException();
	}

	@Override
	Object toObject(CacheEntry entry) {
		throw new NotImplementedException();
	}

}
