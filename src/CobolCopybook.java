package com.savy3.cobolserde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

public class CobolCopybook {
	
	List<String> fieldLines;
	List<String> fieldNames, innerFieldNames;
 	List<String> fieldTypes;
 	List<List<Map<String, Integer>>> fieldProperties;
 	int columnNos;
 	public List<List<Map<String, Integer>>> getFieldProperties() {
		return fieldProperties;
	}
	List<TypeInfo> fieldTypeInfos;
 	public List<TypeInfo> getFieldTypeInfos() {
		return fieldTypeInfos;
	}
	List<ObjectInspector> fieldOIs;

	public CobolCopybook(String layout){
		//System.out.println(layout);
		this.fieldLines = Arrays.asList(layout.split("\\.\\s"));
		this.fieldNames = new LinkedList<String>();
		this.innerFieldNames = new LinkedList<String>();
		this.fieldTypes = new LinkedList<String>();
		this.fieldTypeInfos = new LinkedList<TypeInfo>();
		this.fieldOIs = new LinkedList<ObjectInspector>();
		
		this.fieldProperties = new ArrayList<List<Map<String,Integer>>>();
		new LinkedList<CobolFieldDecl>();
		//getLayout();
		getLayoutNew();
		
	}
	
	private void getLayoutNew() {
		CobolStruct cs = null;
		LinkedList<Integer> groupLevelNos = new LinkedList<Integer>();
		
		for (String fieldLine : fieldLines) {
			CobolFieldDecl cfd = new CobolFieldDecl(fieldLine);
			
			if (cs == null) {
				cs = new CobolStruct();
				LinkedList<Map<String,Integer>> prop = new LinkedList<Map<String,Integer>>();
				fieldProperties.add(prop);
			}else if(cfd.getLevelNo() == 1){
				cs.closeStruct();
				setFieldVars(cs.getFieldName(), cs.getFieldType());
				cs = new CobolStruct();
				LinkedList<Map<String,Integer>> prop = new LinkedList<Map<String,Integer>>();
				fieldProperties.add(prop);
			}
			if (cfd.isIgnoreField) {
				cs.addStruct(cfd);
			} else {
				cs.addElement(cfd);
			}
			if (cfd.getDepending() != null) {
				int index = innerFieldNames.indexOf(cfd.getDepending());
				cfd.fieldProperties.put("dcol", index);
			}

			if (cfd.isIgnoreField) {
				groupLevelNos.add(cfd.getLevelNo());
			}
			fieldProperties.get(columnNos).add(cfd.fieldProperties);
			innerFieldNames.add(cfd.getFieldName());
		}
		cs.closeStruct();
		
		setFieldVars(cs.getFieldName(), cs.getFieldType());
		
	}

	
	private void setFieldVars(String fieldName, String fieldType){
		innerFieldNames.clear();
		fieldNames.add(fieldName);
		System.out.println("field type:"+fieldType);
		fieldTypes.add(fieldType);
		fieldTypeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(fieldType));
		fieldOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoUtils.getTypeInfoFromTypeString(fieldType)));
		columnNos++;
		optimize();
	}
	private void optimize(){
		
//		TypeInfo columnType = fieldTypeInfos.getLast();
//		switch(columnType.getCategory()) {
//		
//		case STRUCT:
//			if(((StructTypeInfo) columnType).getAllStructFieldTypeInfos().size() ==1){
//				fieldProperties.getLast().removeFirst();
//				fieldNames.removeLast();
//				fieldTypes.removeLast();
//				fieldTypeInfos.removeLast();
//				fieldOIs.removeLast();
//				fieldNames.add(((StructTypeInfo) columnType).getAllStructFieldNames().get(0));
//				fieldTypes.add(((StructTypeInfo) columnType).g)
//			}
//		case UNION:
//			return deserializeUnion((UnionTypeInfo) columnType);
//		case LIST:
//		return deserializeList((ListTypeInfo) columnType);
//		case MAP:
//		throw new RuntimeException("map typr is not possible for cobol layout" + columnType.getCategory());
//		case PRIMITIVE:
//		return deserializePrimitive((PrimitiveTypeInfo) columnType);
//		default:
//		throw new RuntimeException("Unknown TypeInfo: " + columnType.getCategory());
//	}
	}
	public List<ObjectInspector> getFieldOIs() {
		return fieldOIs;
	}
	public List<String> getFieldNames() {
		return fieldNames;
	}
	public List<String> getFieldTypes() {
		return fieldTypes;
	}
	
}
