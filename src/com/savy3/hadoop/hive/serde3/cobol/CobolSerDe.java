package com.savy3.hadoop.hive.serde3.cobol;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FixedLengthInputFormat;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeUtils;
import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeUtils.CobolTableProperties;

/**
 * CobolSerde is under construction currently implemented are
 * 
 * @author Ram Manohar <ram.manohar2708@gmail.com>
 */
public final class CobolSerDe extends AbstractSerDe {

	private ObjectInspector inspector;
	private CobolToHive ccb;

	@Override
	public void initialize(final Configuration conf, final Properties tbl)
			throws SerDeException {

		try {
			CobolCopybookBuilder ccbb = new CobolCopybookBuilder();
			this.ccb = new CobolToHive(ccbb.getCobolCopybook(
					CobolSerdeUtils.determineLayoutOrThrowException(conf, tbl)));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		conf.set(CobolTableProperties.LAYOUT_GEN.getPropName(), this.ccb.getLayout());
		conf.set(CobolTableProperties.COBOL_HIVE.getPropName(), this.ccb.getCobolHiveMapping());
		this.inspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(ccb.getHiveNames(),
						ccb.getObjectInspectors(),ccb.getHiveComments());
		

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
		BytesWritable bw = (BytesWritable)(blob);
		return ccb.deserialize(bw.getBytes());
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

	public CobolToHive getCcb() {
		return ccb;
	}



}