package com.coin;

//package opennlp.tools.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import opennlp.tools.util.InputStreamFactory;

public class MockInputStreamFactory implements InputStreamFactory {
	private final File inputSourceFile;
	private final String inputSourceStr;
	private final Charset charset;

	public MockInputStreamFactory(File file) {
		this.inputSourceFile = file;
		this.inputSourceStr = null;
		this.charset = null;
	}

	public MockInputStreamFactory(String str) {
		this(str, StandardCharsets.UTF_8);
	}

	public MockInputStreamFactory(String str, Charset charset) {
		this.inputSourceFile = null;
		this.inputSourceStr = str;
		this.charset = charset;
	}

	@Override
	public InputStream createInputStream() throws IOException {
		if (inputSourceFile != null) {
			//System.out.println(MockInputStreamFactory.class.getResourceAsStream());
			//System.out.println(inputSourceFile.getPath());
			//return this.getClass().getResourceAsStream("en-crypto-sentiment.train");
			return this.getClass().getClassLoader().getResourceAsStream(inputSourceFile.getPath());
		} else {
			return new ByteArrayInputStream(inputSourceStr.getBytes(charset));
		}
	}
}
