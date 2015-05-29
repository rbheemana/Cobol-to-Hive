package com.savy3.cobolserde;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CobolDeserializer {
	private List<Object> row;
	private int offset,fieldNo, columnNo;
	
	String rowString;
	private List<List<Map<String, Integer>>> propertiesList;
	private List<String> rowElements;
	
	Object deserialize(List<TypeInfo> columnTypes, List<List<Map<String, Integer>>> columnProperties,int numCols, Writable writable) {
		
		if(row == null || row.size() != numCols) {
			row = new ArrayList<Object>(numCols);
		} else {
			row.clear();
		}
		offset=0;
		
		Text rowText = (Text) writable;
		rowString = rowText.toString();
		propertiesList = columnProperties;
		rowElements = new ArrayList<String>();
	    workerBase1(row,numCols,columnTypes);
	    return row;
	}
	
	// The actual deserialization may involve nested records, which require recursion.
	private List<Object> workerBase1(List<Object> objectRow, int numCols, 
			List<TypeInfo> columnTypes){
			for(int i = 0; i < numCols; i++) {
				columnNo = i;
				fieldNo=0;
				rowElements.clear();
				System.out.println("working "+i+"prop"+propertiesList.get(i).toString());
				objectRow.add(worker(columnTypes.get(i)));
				
			}
		
			return objectRow;
	}
	private List<Object> workerBase(List<Object> objectRow, int numCols, 
		List<TypeInfo> columnTypes){
		for(int i = 0; i < numCols; i++) {
			objectRow.add(worker(columnTypes.get(i)));
		}
	
		return objectRow;
	}
	
	private Object worker(TypeInfo columnType){
		
		switch(columnType.getCategory()) {
			
			case STRUCT:
				return deserializeStruct((StructTypeInfo) columnType);
			case UNION:
				return deserializeUnion((UnionTypeInfo) columnType);
			case LIST:
			return deserializeList((ListTypeInfo) columnType);
			case MAP:
			throw new RuntimeException("map typr is not possible for cobol layout" + columnType.getCategory());
			case PRIMITIVE:
			return deserializePrimitive((PrimitiveTypeInfo) columnType);
			default:
			throw new RuntimeException("Unknown TypeInfo: " + columnType.getCategory());
		}
	}
	
	//deserilaize primitive types
	private Object deserializePrimitive(PrimitiveTypeInfo columnType){
		
		Map<String,Integer> columnProperty = propertiesList.get(columnNo).get(fieldNo);
		String s1;
		if (offset + columnProperty.get("length") < rowString.length()) {
			s1 = rowString.substring(offset, offset + columnProperty.get("length"));
			if (columnProperty.containsKey("decimal"))
				s1 = rowString.substring(offset, offset + columnProperty.get("decimal"))
						+ "."
						+ rowString.substring(offset + columnProperty.get("decimal"),offset+columnProperty.get("length"));
		}
		else{
			s1 = rowString.substring(offset);
			if (columnProperty.containsKey("decimal"))
				s1 = rowString.substring(offset, offset + columnProperty.get("decimal"))
						+ "."
						+ rowString.substring(offset + columnProperty.get("decimal"));
		}
		
		rowElements.add(s1.trim());
		fieldNo++;
		offset+=columnProperty.get("length");
		switch (columnType.getPrimitiveCategory()){
			case STRING:
				return s1;
			case LONG:
				return Long.parseLong(s1.trim());
			case SHORT:
				return Short.parseShort(s1.trim());
			case INT:
				return Integer.parseInt(s1.trim());
			case BYTE:
				return Byte.parseByte(s1.trim());
			case FLOAT:
				return Float.parseFloat(s1.trim());
			case DOUBLE:
				return Double.parseDouble(s1.trim());
//           to do list
//			case BINARY:

//			case DECIMAL:

//			case CHAR:

//			case VARCHAR:

//			case DATE:

//			case TIMESTAMP:

			default:
			return s1;
		}
	}


	private Object deserializeUnion(UnionTypeInfo columnType) throws RuntimeException {
		return null;
	}
	private Object deserializeStruct(StructTypeInfo columnType) {
		// No equivalent Java type for the backing structure, need to recurse
		// and build a list
		ArrayList<TypeInfo> innerFieldTypes = columnType
				.getAllStructFieldTypeInfos();
		List<Object> innerObjectRow = new ArrayList<Object>(
				innerFieldTypes.size());
		rowElements.add("");
		fieldNo++;
		return workerBase(innerObjectRow, innerFieldTypes.size(),
				innerFieldTypes);

	}
//	private Object deserializeUnion(UnionTypeInfo columnType) throws RuntimeException {
//	}
	private Object deserializeList(ListTypeInfo columnType)
			throws RuntimeException {
		System.out.println("column no"+columnNo);
		int size = Integer.parseInt(rowElements.get(propertiesList.get(columnNo).get(fieldNo).get("dcol")));
		
		List<Object> listContents = new ArrayList<Object>();
		TypeInfo ti = columnType.getListElementTypeInfo();
		rowElements.add("");
		int tempfieldNo = fieldNo, fieldNoList=fieldNo; 
		for (int j = 0; j < size; j++) {
				listContents.add(worker(ti));
				fieldNoList = fieldNo;
				fieldNo = tempfieldNo;
		}
		fieldNo = fieldNoList;
		return listContents;

	}


}
