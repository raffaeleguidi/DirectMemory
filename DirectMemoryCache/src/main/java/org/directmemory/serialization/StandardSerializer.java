package org.directmemory.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class StandardSerializer implements Serializer {
	
	public byte[] serialize(Object obj, @SuppressWarnings({"rawtypes","unchecked"}) Class clazz) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.flush();
		oos.close();
		return baos.toByteArray();		
	}

	public Serializable deserialize(byte[] source, @SuppressWarnings({"rawtypes","unchecked"}) Class clazz) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(source);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Serializable obj = (Serializable) ois.readObject();
		ois.close();
		return obj;
	}}
