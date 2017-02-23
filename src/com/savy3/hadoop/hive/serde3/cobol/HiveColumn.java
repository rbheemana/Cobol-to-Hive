package com.savy3.hadoop.hive.serde3.cobol;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public interface HiveColumn {
	public String getName();
	public TypeInfo getTypeInfo();
	public int getOffset();
	public int getLength();

}
