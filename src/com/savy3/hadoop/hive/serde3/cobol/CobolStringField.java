package com.savy3.hadoop.hive.serde3.cobol;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


public class CobolStringField extends CobolField {

	// constructor
	public CobolStringField(String debugInfo, int levelNo, String name,
			int length) {
		super(debugInfo, levelNo, name, length);
	}

	public CobolStringField(String debugInfo, int levelNo, String name,
			String picClause) {
		super();
		super.debugInfo = debugInfo;
		super.levelNo = levelNo;
		super.type = CobolFieldType.STRING;
		super.name = name;
		String fieldType = "string";
		if (picClause.contains("(")) {
			String[] s = picClause.split("\\(|\\)|\\.");
			if (s.length == 2) {
				try {
					super.length = Integer.parseInt(s[1]);
				} catch (NumberFormatException e) {
					throw e;
				}
			} else {
				throw new RuntimeException(
						"Alphanumeric Picture clause has more brackets:"
								+ this.debugInfo);
			}
		} else {
			if (picClause.matches("x+|a+"))
				super.length = picClause.length();
			else {
				throw new RuntimeException(
						"Alphanumeric Picture clause incorrect '"
								+ this.debugInfo);

			}
		}
		if (super.length < 65355) {
			fieldType = "varchar(" + this.length + ")";
		}
		super.typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldType);
		this.oi = TypeInfoUtils
				.getStandardJavaObjectInspectorFromTypeInfo(this.typeInfo);
	}

	@Override
	public Object deserialize(byte[] rowBytes) {
		byte[] temp = super.transcodeField(super.getBytes(rowBytes));
		String s1 = new String(temp);
		System.out.println(name+"\t - "+s1+"\t:"+offset+"\t@"+length);
		switch (((PrimitiveTypeInfo) this.typeInfo).getPrimitiveCategory()) {
		case STRING:
			return s1;
		case VARCHAR:
			return new HiveVarchar(s1, this.length);
			//return s1;
		}
		return null;
	}

	public String toString() {
		return ("CobolField :[ Name : " + name + ", type : " + typeInfo
				+ ", offset :" + offset + " ]");
	}

}