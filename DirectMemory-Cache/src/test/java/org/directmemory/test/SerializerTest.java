package org.directmemory.test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;
import org.directmemory.serialization.OldProtoStuffSerializer;
import org.directmemory.serialization.ProtoStuffSerializer;
import org.directmemory.serialization.Serializer;
import org.directmemory.serialization.StandardSerializer;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializerTest {
	private static Logger logger=LoggerFactory.getLogger(SerializerTest.class);
	private void testSerializer(String name, Serializer serializer, int size, int howMany) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		logger.info("begin " + serializer.getClass().toString());
        Stopwatch stopWatch = SimonManager.getStopwatch("serializer." + name + "." + size + "bytes");
        Split split = stopWatch.start();
		DummyPojo pojo = new DummyPojo("test", size);
		for (int i = 0; i < howMany; i++) {
			final byte[] array = serializer.serialize(pojo, pojo.getClass());
			DummyPojo check = (DummyPojo) serializer.deserialize(array, pojo.getClass());
			assertNotNull("object has not been serialized", check);
			assertEquals(pojo.name, check.name);
		}
		split.stop();
		logger.info("end " + serializer.getClass().toString() + "\r\n" + stopWatch.toString());
	}
	
	@Test
	public void ProtostuffTest() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		testSerializer("protostuff-old", new OldProtoStuffSerializer(), Ram.Kb(1), 20000);
		testSerializer("protostuff-old", new OldProtoStuffSerializer(), Ram.Kb(2), 20000);
		testSerializer("protostuff-old", new OldProtoStuffSerializer(), Ram.Kb(3), 20000);
		testSerializer("protostuff-old", new OldProtoStuffSerializer(), Ram.Kb(4), 20000);
	}
	@Test
	public void ProtostuffV2Test() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		testSerializer("protostuff-new", new ProtoStuffSerializer(), Ram.Kb(1), 20000);
		testSerializer("protostuff-new", new ProtoStuffSerializer(), Ram.Kb(2), 20000);
		testSerializer("protostuff-new", new ProtoStuffSerializer(), Ram.Kb(3), 20000);
		testSerializer("protostuff-new", new ProtoStuffSerializer(), Ram.Kb(4), 20000);
	}
	@Test
	public void StandardTest() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(1), 20000);
		testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(2), 20000);
		testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(3), 20000);
		testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(4), 20000);
	}
}
