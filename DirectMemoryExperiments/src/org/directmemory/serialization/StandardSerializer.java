package org.directmemory.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;

public class StandardSerializer implements Serializer {
	
	public Serializable deserialize(byte[] source, @SuppressWarnings("rawtypes") Class clazz) throws IOException, ClassNotFoundException {
        Stopwatch stopWatch = SimonManager.getStopwatch("java-deserialize");
		Split split = stopWatch.start();
		ByteArrayInputStream bis = new ByteArrayInputStream(source);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Serializable obj = (Serializable) ois.readObject();
		ois.close();
		split.stop();
		return obj;
	}
	
	@Override
	public byte[] serialize(Object obj, @SuppressWarnings("rawtypes") Class clazz) throws IOException {
        Stopwatch stopWatch = SimonManager.getStopwatch("java-serialize");
		Split split = stopWatch.start();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.flush();
		oos.close();
		split.stop();
		return baos.toByteArray();		
	}

}
