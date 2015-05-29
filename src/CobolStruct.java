package com.savy3.cobolserde;

public class CobolStruct {
	
	String fieldName;
	String fieldType;
	int levelNoProcessing;
	String prevGroupType;
	CobolStruct(){
		fieldName = "";
		fieldType = "";
		//fieldProperty.add(cfd.getFieldProperties());
		levelNoProcessing = 0;
	}
	public void addElement(CobolFieldDecl cfd){
		if(!fieldType.contains("struct")){
			if (fieldType == "")
				fieldType=cfd.getFieldType();
			else if (cfd.getLevelNo() == levelNoProcessing){
				fieldType+=","+cfd.getFieldType();
			}
		}else {if (cfd.getLevelNo() > levelNoProcessing){
			fieldType+="<";
		}else if (cfd.getLevelNo() == levelNoProcessing){
			fieldType+=",";
		}else{
			if (prevGroupType == "array<struct")
			fieldType+=">>,";
			else
				fieldType+=">,";
		}
		if (prevGroupType == "array"){
		fieldType+=cfd.getFieldType();
		prevGroupType="";
		}
		else
		fieldType+=cfd.fieldName+":"+cfd.getFieldType();
		}
		levelNoProcessing = cfd.getLevelNo();
		
		
	}
	public void addStruct(CobolFieldDecl cfd){
		if (cfd.getLevelNo() > levelNoProcessing){
			fieldType+="<";
		}else if (cfd.getLevelNo() == levelNoProcessing){
			fieldType+=",";
		}else{
			if (prevGroupType == "array<struct")
			fieldType+=">>,";
			else
				fieldType+=">,";
		}
		if(fieldName==""){
			fieldName = cfd.getFieldName();
			fieldType = cfd.getFieldType();
		}
		else{
			if (prevGroupType == "array"){
				fieldType+=cfd.getFieldType();
				prevGroupType="";
			}
			else
				fieldType+=cfd.fieldName+":"+cfd.getFieldType();
		}
		
		levelNoProcessing = cfd.getLevelNo();
		prevGroupType = cfd.getFieldType();
			
		
	}
	
	public String getFieldName() {
		return fieldName;
	}
	public void closeStruct(){
//		if (fieldType.contains("array<") & fieldType.split(",").length == 1){
//			fieldType = "array<"+fieldType.split(":")[1];
//		}
//		if (fieldType.contains("struct<") & fieldType.split(",").length == 1){
//			fieldType = fieldType.split(":")[1];
//		}
		for (int i = 0; i < fieldType.split("<").length - fieldType.split(">").length;i++){
			fieldType+=">";
		}
	}

	public String getFieldType() {
		
		return fieldType;
	}

}
