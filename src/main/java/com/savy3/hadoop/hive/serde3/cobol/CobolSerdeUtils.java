package com.savy3.hadoop.hive.serde3.cobol;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FixedLengthInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utilities useful only to the CobolSerde itself. Not mean to be used by
 * end-users but public for interop to the ql package.
 */
public class CobolSerdeUtils {
	private static final Log LOG = LogFactory.getLog(CobolSerdeUtils.class);

	/**
	 * Enum container for all avro table properties. If introducing a new
	 * avro-specific table property, add it here. Putting them in an enum rather
	 * than separate strings allows them to be programmatically grouped and
	 * referenced together.
	 */
	public static enum CobolTableProperties {
		LAYOUT_LITERAL("cobol.layout.literal"), LAYOUT_URL("cobol.layout.url"), LAYOUT_NAMESPACE(
				"cobol.layout.namespace"), LAYOUT_NAME("cobol.layout.name"), LAYOUT_DOC(
				"cobol.layout.doc"), COBOL_SERDE_LAYOUT("cobol.serde.layout"), LAYOUT_RETRIEVER(
				"cobol.layout.retriever"), LAYOUT_TEST("cobol.layout.test"), FB_LENGTH("fb.length"),
				LAYOUT_GEN("cobol.layout.generated"), COBOL_HIVE("cobol.hive.mapping");

		private final String propName;

		CobolTableProperties(String propName) {
			this.propName = propName;
		}

		public String getPropName() {
			return this.propName;
		}
	}

	public static final String LAYOUT_NONE = "none";
	public static final String EXCEPTION_MESSAGE = "Neither "
			+ CobolTableProperties.LAYOUT_LITERAL.getPropName() + " nor "
			+ CobolTableProperties.LAYOUT_URL.getPropName()
			+ " specified, can't determine table layout";

	/**
	 * Determine the layout to that's been provided for cobol serde work.
	 * 
	 * @param properties
	 *            containing a key pointing to the layout, one way or another
	 * @return layout to use while serdeing the avro file
	 * @throws IOException
	 *             if error while trying to read the layout from another
	 *             location
	 * @throws CobolSerdeException
	 *             if unable to find a layout or pointer to it in the properties
	 */
	public static String determineLayoutOrThrowException(Configuration conf,
			Properties properties) throws IOException, CobolSerdeException {
		
		//For fixed length record get length of the file 
		String fixedRecordLength = properties
				.getProperty(CobolTableProperties.FB_LENGTH.getPropName());
		if (fixedRecordLength != null){
			conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, Integer.parseInt(fixedRecordLength));
		}
		
		String layoutString = properties
				.getProperty(CobolTableProperties.LAYOUT_LITERAL.getPropName());
		if (layoutString != null && !layoutString.equals(LAYOUT_NONE))
			return CobolSerdeUtils.getLayoutFor(layoutString);
		
		//For testing purpose
		layoutString = properties.getProperty(CobolTableProperties.LAYOUT_TEST
				.getPropName());
		if (layoutString !=null){
			return readFile(layoutString,Charset.defaultCharset());
		}
		
		// Try pulling directly from URL
		layoutString = properties.getProperty(CobolTableProperties.LAYOUT_URL
				.getPropName());
		if (layoutString == null || layoutString.equals(LAYOUT_NONE))
			throw new CobolSerdeException(EXCEPTION_MESSAGE);

		try {
			String s = getLayoutFromFS(layoutString, conf);
			if (s == null) {
				// in case layout is not a file system
				
				return CobolSerdeUtils.getLayoutFor(new URL(layoutString)
						.openStream());
			}
			return s;
		} catch (IOException ioe) {
			throw new CobolSerdeException(
					"Unable to read layout from given path: " + layoutString,
					ioe);
		} catch (URISyntaxException urie) {
			throw new CobolSerdeException(
					"Unable to read layout from given path: " + layoutString,
					urie);
		}
	}

	// Protected for testing and so we can pass in a conf for testing.
	protected static String getLayoutFromFS(String layoutFSUrl,
			Configuration conf) throws IOException, URISyntaxException {
		FSDataInputStream in = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(layoutFSUrl), conf);
		} catch (IOException ioe) {
			// return null only if the file system in layout is not recognized
			String msg = "Failed to open file system for uri " + layoutFSUrl
					+ " assuming it is not a FileSystem url";
			LOG.debug(msg, ioe);
			return null;
		}
		try {
			in = fs.open(new Path(layoutFSUrl));
			String s = CobolSerdeUtils.getLayoutFor(in);
			return s;
		} finally {
			if (in != null)
				in.close();
		}
	}

	public static String getLayoutFor(String str) {
		str = str.replaceAll("[\\t\\n\\r]", " ");
		str = str.replaceAll("( )+", " ");
		return str;
	}

	// public static String getLayoutFor(File file) {
	// Layout.Parser parser = new Layout.Parser();
	// Layout layout;
	// try {
	// layout = parser.parse(file);
	// } catch (IOException e) {
	// throw new RuntimeException("Failed to parse Cobol layout from " +
	// file.getName(), e);
	// }
	// return layout;
	// }

	public static String getLayoutFor(InputStream stream) {
		StringBuilder out = new StringBuilder();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					stream));

			String line;
			while ((line = reader.readLine()) != null) {
				out.append(line);
			}
			reader.close();
		} catch (IOException e) {
			throw new RuntimeException("Failed to parse Cobol layout", e);
		}
		return getLayoutFor(out.toString());
	}
	//just for testing
	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}
}
