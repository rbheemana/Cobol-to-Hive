package com.savy3.hadoop.hive.serde3.cobol;

public class CobolGrpDependField extends CobolGroupField{
	private CobolField dependField;
	public CobolGrpDependField(String debugInfo, int levelNo, String name, int occurs, CobolField dependField) {
		super(debugInfo, levelNo,name,occurs,0 );
		super.type = CobolFieldType.DEPEND;
		this.dependField = dependField;
	}
	public CobolField getDependField() {
		return dependField;
	}
}
