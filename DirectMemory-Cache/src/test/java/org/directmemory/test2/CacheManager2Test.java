package org.directmemory.test2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedHashMap;

import org.directmemory.cache.CacheEntry;
import org.directmemory.cache.CacheManager2;
import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
import org.directmemory.store.SimpleOffHeapStore;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

public class CacheManager2Test {
	
//	@Test
	public void only10puts() {
		long startedAt = Calendar.getInstance().getTimeInMillis();
		CacheManager2 cache = new CacheManager2(10);
		for (int i = 0; i < 50; i++) {
			DummyPojo pojo = new DummyPojo("test" + i, Ram.Kb(2));
			cache.put(pojo.name, pojo);
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("ended in " + (finishedAt-startedAt) + " milliseconds");
	}
	
//	@Test
	public void onlyCreates() {
		long startedAt = Calendar.getInstance().getTimeInMillis();
		for (int i = 0; i < 50000; i++) {
			@SuppressWarnings("unused")
			DummyPojo pojo = new DummyPojo("test" + i, Ram.Kb(2));
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("ended in " + (finishedAt-startedAt) + " milliseconds");
	}
//	@Test
	public void manyPuts() {
		long startedAt = Calendar.getInstance().getTimeInMillis();
		CacheManager2 cache = new CacheManager2(100);
		for (int i = 0; i < 50000; i++) {
			DummyPojo pojo = new DummyPojo("test" + i, Ram.Kb(2));
			cache.put(pojo.name, pojo);
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("ended in " + (finishedAt-startedAt) + " milliseconds");
	}
//	@Test
	public void manyPutsManyReads() {
		CacheManager2 cache = new CacheManager2(100);
		for (int i = 0; i < 10000; i++) {
			DummyPojo pojo = new DummyPojo("test" + i, Ram.Kb(2));
			cache.put(pojo.name, pojo);
		}
		long startedAt = Calendar.getInstance().getTimeInMillis();
		for (int i = 0; i < 10000; i++) {
			DummyPojo pojo = (DummyPojo)cache.get("test" + i);
			assertNotNull("object not found: test" + i, pojo);
			assertEquals("test" + i, pojo.name);
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("ended in " + (finishedAt-startedAt) + " milliseconds");
	}
	

//    @Test
	public void onlyOrient() throws IOException {
        final int        PAYLOAD_SIZE    = 2000;
        ODatabaseDocument       database;
        ORecordBytes                    record;
        byte[]                                          payload;

        payload = new byte[PAYLOAD_SIZE];
        for (int i = 0; i < PAYLOAD_SIZE; ++i) {
            payload[i] = (byte) i;
        }
        
    	String baseDir = "data";
    	String binaryDir = "binary2";
    	
        File base = new File(baseDir + "\\" + binaryDir);
		if (!base.exists())
				base.mkdir();
		
		database = new ODatabaseDocumentTx("local:" + base.getAbsolutePath() + "\\data");
		database.delete();
		database.create();
        database.declareIntent(new OIntentMassiveInsert());
        database.begin(TXTYPE.NOTX);

        database.declareIntent(new OIntentMassiveInsert());
        database.begin(TXTYPE.NOTX);
	
        LinkedHashMap<String, CacheEntry> map = new LinkedHashMap<String, CacheEntry> ();
        
        Serializer ser = new ProtoStuffSerializer(Ram.Kb(4));
        
		long startedAt = Calendar.getInstance().getTimeInMillis();
        for (int i = 0; i < 100000; i++) {
			DummyPojo pojo = new DummyPojo("test", Ram.Kb(2));
			pojo.name = "test" + i;
        	payload = ser.serialize(pojo, pojo.getClass());
            record = new ORecordBytes(database, payload);
            record.save();
            CacheEntry entry = new CacheEntry();
            entry.identity = record.getIdentity();
            map.put("test" + i, entry);
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		long total = (finishedAt-startedAt);
        System.out.println("total time: " + total);
        
        database.commit();
        database.close();


	}
	@Test
	public void manyPutsManyReadsWithOffHeap() throws InterruptedException {		
		int howMany = 101000;
		System.out.println("Starting test " + howMany + " entries");
		SimpleOffHeapStore secondLevel = new SimpleOffHeapStore();
		secondLevel.queueSize = howMany / 1000;
		secondLevel.serializer = new ProtoStuffSerializer(Ram.Kb(4));
//		secondLevel.serializer = new StandardSerializer();
//		secondLevel.serializer = new DummyPojoSerializer();
		CacheManager2 cache = new CacheManager2(howMany / 100, secondLevel);
		System.out.println(cache.heap.toString());
		System.out.println(cache.heap.nextStore.toString());
		long startedAt = Calendar.getInstance().getTimeInMillis();
		for (int i = 0; i < howMany; i++) {
			DummyPojo pojo = new DummyPojo("test", Ram.Kb(2));
			pojo.name = "test" + i;
			cache.put(pojo.name, pojo);
		}
		long finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("put " + howMany + " entries in " + (finishedAt-startedAt) + " milliseconds");
		System.out.println(cache.heap.toString());
		System.out.println(cache.heap.nextStore.toString());

		startedAt = Calendar.getInstance().getTimeInMillis();
		for (int i = 0; i < howMany; i++) {
			DummyPojo pojo = (DummyPojo)cache.get("test" + i);
			assertNotNull("object not found: test" + i, pojo);
			assertEquals("test" + i, pojo.name);
		}
		
		System.out.println(cache.heap.toString());
		System.out.println(cache.heap.nextStore.toString());
		finishedAt = Calendar.getInstance().getTimeInMillis();
		System.out.println("got " + howMany + " entries in " + (finishedAt-startedAt) + " milliseconds");
		System.out.println("disposing");
		cache.heap.dispose();
		secondLevel.dispose();
		System.out.println(cache.heap.toString());
		System.out.println(cache.heap.nextStore.toString());
		System.out.println("done");
	}


	
}
