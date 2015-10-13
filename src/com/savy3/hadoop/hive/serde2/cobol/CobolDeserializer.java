package com.savy3.hadoop.hive.serde2.cobol;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class CobolDeserializer {
	private List<Object> row;
	private int offset,fieldNo, columnNo;
	
	String rowString;
	byte[] rowBytes;
	private List<Map<String, Integer>> propertiesList;
	private Map<Integer,Integer> columnEndOffset;
	private List<String> rowElements;
	BytesWritable bw;
	Object deserialize(List<String> columnNames, List<TypeInfo> columnTypes, List<Map<String, Integer>> columnProperties,int numCols, Writable writable) {
		if(row == null || row.size() != numCols) {
			row = new ArrayList<Object>(numCols);
		} else {
			row.clear();
		}
		offset=0;
		bw = (BytesWritable)(writable);
		rowString=new String(bw.getBytes());
		rowBytes = bw.getBytes();

		propertiesList = columnProperties;
		rowElements = new ArrayList<String>();
		columnEndOffset = new HashMap<Integer,Integer>();
	    workerBase1(row,numCols,columnNames,columnTypes);
	    return row;
	}
	
	// The actual deserialization may involve nested records, which require recursion.
	private List<Object> workerBase1(List<Object> objectRow, int numCols, 
			List<String> columnNames,List<TypeInfo> columnTypes){
			for(int i = 0; i < numCols; i++) {
				columnNo = i;
				fieldNo=0;
				objectRow.add(worker(columnNames.get(i),columnTypes.get(i)));
				
			}
		
			return objectRow;
	}
	private List<Object> workerBase(List<Object> objectRow, int numCols, 
		List<String> columnNames, List<TypeInfo> columnTypes){
		for(int i = 0; i < numCols; i++) {
			objectRow.add(worker(columnNames.get(i),columnTypes.get(i)));
		}
	
		return objectRow;
	}
	
	private Object worker(String columnName, TypeInfo columnType){
		
		switch(columnType.getCategory()) {
			
			case STRUCT:
				return deserializeStruct(columnName, (StructTypeInfo) columnType);
			case UNION:
				return deserializeUnion(columnName,(UnionTypeInfo) columnType);
			case LIST:
			return deserializeList(columnName, (ListTypeInfo) columnType);
			case MAP:
			throw new RuntimeException("map type is not possible for cobol layout" + columnType.getCategory());
			case PRIMITIVE:
			return deserializePrimitive(columnName, (PrimitiveTypeInfo) columnType);
			default:
			throw new RuntimeException("Unknown TypeInfo: " + columnType.getCategory());
		}
	}
	
	//deserilaize primitive types
	private Object deserializePrimitive(String columnName, PrimitiveTypeInfo columnType){
		
		Map<String,Integer> columnProperty = propertiesList.get(columnNo);
		String s1 = null;
		
		if (columnProperty.get("depend.id") > 0) {
			Integer occurance = new Integer(columnProperty.get("occurance") - 1);
			if (!columnProperty.get("depend.id").equals("-2")) {

				if (rowElements.get(columnProperty.get("depend.id") - 1)
						.compareTo(occurance.toString()) < 0) {
					columnNo++;
					columnEndOffset.put(columnNo, offset);
					return null;
				}
			}
		}
		
		if (columnProperty.get("prev.col") == 0){
			offset =0;
		}else{
			offset=columnEndOffset.get(columnProperty.get("prev.col"));
		}
		
		byte [] fieldBytes = null;
		if (offset + columnProperty.get("length") < rowBytes.length) {
			fieldBytes = Arrays.copyOfRange(rowBytes, offset, offset + columnProperty.get("length"));
		}
		else{
			if (offset > rowBytes.length)
				System.out.println("Corrupted cobol layout; Offset position:"+offset+" greater than record length :"+rowBytes.length);
			fieldBytes= Arrays.copyOfRange(rowBytes, offset, rowBytes.length);
		}

		if (columnProperty.get("comp")!=null){
			s1 = new String (fieldBytes);
			if (columnProperty.get("comp")==3){ 
				int decimalLocation = 0;
				if (columnProperty.containsKey("decimal"))
					decimalLocation = columnProperty.get("decimal");
			
				s1=unpackData(fieldBytes,decimalLocation);
			}
		}else{
			byte[] temp = transcodeField(fieldBytes, Charset.forName("ebcdic-cp-us"),Charset.forName("ascii"),columnName);
			s1 = new String(temp);
		}
		
		rowElements.add(s1);
		columnNo++;
		offset+=columnProperty.get("length");
		columnEndOffset.put(columnNo, offset);
		s1=s1.trim();
		try {

			switch (columnType.getPrimitiveCategory()) {
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

			case DECIMAL:
				BigDecimal bd = new BigDecimal(s1);
				HiveDecimal dec = HiveDecimal.create(bd);
				JavaHiveDecimalObjectInspector oi = (JavaHiveDecimalObjectInspector) PrimitiveObjectInspectorFactory
						.getPrimitiveJavaObjectInspector((DecimalTypeInfo) columnType);
				return oi.set(null, dec);
			case VARCHAR:
				return new HiveVarchar(s1, columnProperty.get("length"));

				// to do list
				// case BINARY:
				// case DATE:
				// case CHAR:

				// case TIMESTAMP:

			default:
				return s1;
			}
		} catch (Exception e) {
//			System.out.println("Exception");
			return null;
		}
		
		
	}
	public String unpackData(byte[] packedData, int decimalPointLocation) {
		String unpackedData = "";
	    final int negativeSign = 13;
	    for (int currentByteIndex = 0; currentByteIndex < packedData.length; currentByteIndex++) {

	        int firstDigit = ((packedData[currentByteIndex] >> 4) & 0x0f);
	        int secondDigit = (packedData[currentByteIndex] & 0x0F);
	        unpackedData += String.valueOf(firstDigit);
	        if (currentByteIndex == (packedData.length - 1)) {
	            if (secondDigit == negativeSign) {
	                unpackedData = "-" + unpackedData;
	            }
	        } else {
	            unpackedData += String.valueOf(secondDigit);
	        }
	    }
	    if (decimalPointLocation > 0) {
	        unpackedData = unpackedData.substring(0, (decimalPointLocation - 1)) + 
	                        "." + 
	                        unpackedData.substring(decimalPointLocation);
	    }
	    return unpackedData;
	}

	public static byte[] transcodeField(byte[] source, Charset from, Charset to,String columnName) {
		  byte[] result = new String(source, from).getBytes(to);
//		  System.out.println("source length"+source.length+"result length"+result.length);
		  if (result.length != source.length) {
		    throw new AssertionError("EBCDIC TO ASCII conversion for column:"+columnName+"failed"+result.length + "!=" + source.length);
		  }
		  return result;
	}
	public String unpackData1(String packedData, int decimalPointLocation) {
	    String unpackedData = "";
	    char[] characters = packedData.toCharArray();
	    final int negativeSign = 13;
	    for (int currentCharIndex = 0; currentCharIndex < characters.length; currentCharIndex++) {
	        int firstDigit = (((byte) characters[currentCharIndex]) >>> 4);
	        int secondDigit = ((byte) characters[currentCharIndex]) & 0x0F;
	        unpackedData += String.valueOf(firstDigit);
	        if (currentCharIndex == (characters.length - 1)) {
	            if (secondDigit == negativeSign) {
	                unpackedData = "-" + unpackedData;
	            }
	        } else {
	            unpackedData += String.valueOf(secondDigit);
	        }
	    }
	    if (decimalPointLocation > 0) {
	        unpackedData = unpackedData.substring(0, (decimalPointLocation - 1)) + 
	                        "." + 
	                        unpackedData.substring(decimalPointLocation);
	    }
	    return unpackedData;
	}

	private Object deserializeUnion(String columnName,UnionTypeInfo columnType) throws RuntimeException {
		return null;
	}
	private Object deserializeStruct(String columnName, StructTypeInfo columnType) {
		// No equivalent Java type for the backing structure, need to recurse
		// and build a list
		ArrayList<TypeInfo> innerFieldTypes = (ArrayList<TypeInfo>) columnType
				.getAllStructFieldTypeInfos();
		List<Object> innerObjectRow = new ArrayList<Object>(
				innerFieldTypes.size());
		List<String> innerColumnNames = columnType.getAllStructFieldNames();
		rowElements.add("");
		fieldNo++;
		return workerBase(innerObjectRow, innerFieldTypes.size(),
				innerColumnNames, innerFieldTypes);

	}
//	private Object deserializeUnion(UnionTypeInfo columnType) throws RuntimeException {
//	}
	private Object deserializeList(String columnName, ListTypeInfo columnType)
			throws RuntimeException {
		int size = Integer.parseInt(rowElements.get(propertiesList.get(fieldNo).get("dcol")));
		
		List<Object> listContents = new ArrayList<Object>();
		TypeInfo ti = columnType.getListElementTypeInfo();
		String tn = columnType.getTypeName();
		rowElements.add("");
		int tempfieldNo = fieldNo, fieldNoList=fieldNo; 
		for (int j = 0; j < size; j++) {
				listContents.add(worker(tn,ti));
				fieldNoList = fieldNo;
				fieldNo = tempfieldNo;
		}
		fieldNo = fieldNoList;
		return listContents;

	}


}
