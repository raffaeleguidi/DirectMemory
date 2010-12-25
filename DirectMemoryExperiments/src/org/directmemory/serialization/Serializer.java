package org.directmemory.serialization;

import java.io.IOException;

public interface Serializer {

	public abstract byte[] serialize(Object obj,
			@SuppressWarnings("rawtypes") Class clazz) throws IOException;

	public abstract Object deserialize(byte[] source,
			@SuppressWarnings("rawtypes") Class clazz) throws IOException,
			ClassNotFoundException, InstantiationException, IllegalAccessException;

}