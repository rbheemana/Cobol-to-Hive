package com.savy3.hadoop.hive.serde2.cobol;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * CobolSerde is under construction currently implemented are
 * 
 * @author Ram Manohar <ram.manohar2708@gmail.com>
 */
public final class CobolSerDe extends AbstractSerDe {

	private ObjectInspector inspector;
	private int numCols;
	private List<TypeInfo> columnTypes;
	private List<Map<String, Integer>> columnProperties;

	private CobolDeserializer cobolDeserializer = null;
	private CobolCopybook ccb;

	@Override
	public void initialize(final Configuration conf, final Properties tbl)
			throws SerDeException {

		// final int fixedRecordlLength =
		// Integer.parseInt(tbl.getProperty("fb.length"));
		// conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH,
		// fixedRecordlLength);
		try {
			this.ccb = new CobolCopybook(
					CobolSerdeUtils.determineLayoutOrThrowException(conf, tbl));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		numCols = ccb.getFieldNames().size();
		this.inspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(ccb.getFieldNames(),
						ccb.getFieldOIs());
		this.columnTypes = ccb.getFieldTypeInfos();
		this.columnProperties = ccb.getFieldProperties();

	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		// Serializing to cobol layout format is out-of-scope
		throw new SerDeException("Serializer not built");
		// return new Text("Out-of-scope");
	}

	@Override
	public Object deserialize(final Writable blob) throws SerDeException {
		return getDeserializer().deserialize(ccb.getFieldNames(),
				this.columnTypes, this.columnProperties, this.numCols, blob);
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return inspector;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	public SerDeStats getSerDeStats() {
		return null;
	}

	private CobolDeserializer getDeserializer() {
		if (cobolDeserializer == null) {
			cobolDeserializer = new CobolDeserializer();
		}

		return cobolDeserializer;
	}

}
