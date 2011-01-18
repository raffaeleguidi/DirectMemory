package org.directmemory.serialization;

import java.io.EOFException;
import java.io.IOException;

import org.directmemory.measures.Ram;
import org.directmemory.misc.DummyPojo;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

public final class DummyPojoSerializer implements Serializer
	{
	    
    	final DummyPojo pojo = new DummyPojo("test", Ram.Kb(2));
	    final byte[] data;
	    
	    public DummyPojoSerializer()
	    {
	        data = ProtostuffIOUtil.toByteArray(pojo, RuntimeSchema.getSchema(DummyPojo.class), LinkedBuffer.allocate(2048));
	    }

        @Override
        public Object deserialize(byte[] source, @SuppressWarnings({"rawtypes","unchecked"}) Class clazz) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
                EOFException
        {
            // testing puts only
            return pojo;
        }

        @Override
        public byte[] serialize(Object obj,  @SuppressWarnings({"rawtypes","unchecked"}) Class clazz) throws IOException
        {
//            byte[] ser = new byte[data.length];
//            System.arraycopy(data, 0, ser, 0, data.length);
            return data;
        }
	    
	}