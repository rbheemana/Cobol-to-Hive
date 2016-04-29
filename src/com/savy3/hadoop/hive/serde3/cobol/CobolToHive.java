package com.savy3.hadoop.hive.serde3.cobol;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeException;


import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class CobolToHive {
	private static List<String> hiveNames = null; 
	private static String layout = null;
	private static String cobolHiveMapping = null;
	private static List<TypeInfo> hiveTypesInfos = null;
	private static List<ObjectInspector> objectInspectors = null;
	CobolGroupField cobolCopyBook;
	
	
	public CobolToHive(CobolGroupField cobolCopyBook) {
		super();
		this.cobolCopyBook = cobolCopyBook;
	}
	public int getSize() {
		return cobolCopyBook.getSize();
	}

	public List<Object> deserialize(byte[] rowBytes) throws CobolSerdeException {
		return cobolCopyBook.deserialize(rowBytes);
	}

	public List<String> getHiveNames() {
		if(hiveNames == null){
			hiveNames = cobolCopyBook.getHiveColumnNames();
		}
		return hiveNames;
	}

	public String getLayout() {
		if(layout == null){
			layout="";
			for(String s:cobolCopyBook.getLayout(0)){
				layout+=s+"\n";
			}
		}
		return layout;
	}
	public String getCobolHiveMapping() {
		if(cobolHiveMapping == null){
			cobolHiveMapping="";
			for(String s:cobolCopyBook.getCobolHiveMapping(0)){
				cobolHiveMapping+=s+"\n";
			}
		}
		return cobolHiveMapping;
	}
	public List<TypeInfo> getHiveTypes() {
		if(hiveTypesInfos ==null){
			hiveTypesInfos = cobolCopyBook.getHiveColumnTypes();
		}
		return hiveTypesInfos;
	}

	public List<ObjectInspector> getObjectInspectors() {
		if(objectInspectors==null){
			objectInspectors = cobolCopyBook.getObjectInspectors();
		}
		return objectInspectors;
	}

	@Override
	public String toString() {
		return cobolCopyBook.getDebugInfo();
	}
}