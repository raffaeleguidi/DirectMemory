package org.directcache.test;

import java.io.Serializable;
import java.lang.instrument.ClassDefinition;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.jar.JarFile;

public class TestClass implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2391812518800537726L;
	private String name;
	public TestClass(String name) {
		this.name = name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getName() {
		return name;
	}

	public Object[] obj;
} 
