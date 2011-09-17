package org.directmemory.serialization;

import java.io.IOException;

import org.directmemory.measures.Ram;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

public final class ProtoStuffWithLinkedBufferSerializer implements Serializer {
	
	static int bufferSize = Ram.Kb(3);
	
	
	/*
	 * 
	 * 
	 * 
	LinkedBuffer buffer8k = ...;
	try
	{
	    ProtostuffIOUtil.writeTo(new ByteBufferOutputStream() { // paging logic }, message, schema, buffer8k)
	}
	finally
	{
	    buffer8k.clear();
	}
	
	ProtostuffIOUtil.mergeFrom(new ByteArrayInputStream() { // paging logic}, message, schema, buffer8k);

*/

	private static final ThreadLocal<LinkedBuffer> localBuffer = new ThreadLocal<LinkedBuffer>() {
		protected LinkedBuffer initialValue() {
			return LinkedBuffer.allocate(bufferSize);
		}
	};
	
	public ProtoStuffWithLinkedBufferSerializer() {
		
	}
	
	
	public ProtoStuffWithLinkedBufferSerializer(int bufferSize) {
		ProtoStuffWithLinkedBufferSerializer.bufferSize =bufferSize; 
	}
	
	
	
	/* (non-Javadoc)
	 * @see org.directmemory.utils.Serializer#serialize(java.lang.Object, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	public byte[] serialize(Object obj, @SuppressWarnings("rawtypes") Class clazz) throws IOException {
		@SuppressWarnings("rawtypes")
		Schema schema = RuntimeSchema.getSchema(clazz);
		final LinkedBuffer buffer = localBuffer.get();
		byte[] protostuff = null;

		try {
			protostuff = ProtostuffIOUtil.toByteArray(obj, schema, buffer);
		} finally {
			buffer.clear();
		}		
		return protostuff;
	}

	/* (non-Javadoc)
	 * @see org.directmemory.utils.Serializer#deserialize(byte[], java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	public Object deserialize(byte[] source, @SuppressWarnings("rawtypes") Class clazz) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		Object object = clazz.newInstance();
		@SuppressWarnings("rawtypes")
		Schema schema = RuntimeSchema.getSchema(clazz);
		ProtostuffIOUtil.mergeFrom(source, object, schema);
		return object;
	}	
}
