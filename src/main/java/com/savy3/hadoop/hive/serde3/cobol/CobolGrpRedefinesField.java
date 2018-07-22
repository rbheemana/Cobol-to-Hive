package com.savy3.hadoop.hive.serde3.cobol;

public class CobolGrpRedefinesField extends CobolGroupField{
	@Override
	public String toString() {
		return "CobolGrpRedefinesField [redefinesField=" + redefinesField
				+ ", occurs=" + occurs + ", name=" + name + ", debugInfo="
				+ debugInfo + ", typeInfo=" + typeInfo + ", oi=" + oi
				+ ", compType=" + compType + ", decimalLocation="
				+ decimalLocation + ", length=" + length + ", type=" + type
				+ ", levelNo=" + levelNo + ", offset=" + offset + "]";
	}
	private CobolField redefinesField;

	public CobolGrpRedefinesField(String debugInfo,int levelNo, String name, CobolField redefinesField) {
		super(debugInfo,levelNo,name,1,0 );
		super.type = CobolFieldType.REDEFINES;
		this.redefinesField = redefinesField;
	}
	public CobolField getRedefinesField() {
		return redefinesField;
	}

}
