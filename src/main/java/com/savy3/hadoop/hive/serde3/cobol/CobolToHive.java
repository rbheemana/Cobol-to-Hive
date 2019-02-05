package com.savy3.hadoop.hive.serde3.cobol;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

public class CobolToHive {
	private List<String> hiveNames = null; 
	private List<String> hiveComments = null;
	private String layout = null;
	private String cobolHiveMapping = null;
	private List<TypeInfo> hiveTypesInfos = null;
	private List<ObjectInspector> objectInspectors = null;
	private String cobolFieldIgnorePattern = null;
	CobolGroupField cobolCopyBook;
	
	
	public CobolToHive(CobolGroupField cobolCopyBook) {
		super();
		this.cobolCopyBook = cobolCopyBook;
	}

	public CobolToHive(CobolGroupField cobolCopyBook, String cobolFieldIgnorePattern) {
		super();
		this.cobolCopyBook = cobolCopyBook;
		this.cobolFieldIgnorePattern = cobolFieldIgnorePattern;

	}
	public int getSize() {
		return cobolCopyBook.getSize();
	}

	public List<Object> deserialize(byte[] rowBytes) throws CobolSerdeException {
		if (cobolFieldIgnorePattern != null) {
			return cobolCopyBook.deserialize(rowBytes, cobolFieldIgnorePattern);
		} else {
			return cobolCopyBook.deserialize(rowBytes);
		}

	}

	protected List<?> getNamesbyIgnoring(List<?> list) {

		if (cobolFieldIgnorePattern != null) {
			List<Object> list1 = new ArrayList<Object>();
			for (int i = 0; i < cobolCopyBook.getHiveColumnNames().size(); i++) {
				if (cobolCopyBook.getHiveColumnNames().get(i).matches(cobolFieldIgnorePattern)) {
					continue;
				} else {
					list1.add(list.get(i));
				}
			}
			return list1;
		} else {
			return list;
		}

	}

	public List<String> getHiveNames() {
		if(hiveNames == null){

			hiveNames = (List<String>) getNamesbyIgnoring(cobolCopyBook.getHiveColumnNames());
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
	public String getCobolHiveMapping() throws CobolSerdeException {
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
			hiveTypesInfos = (List<TypeInfo>) getNamesbyIgnoring(cobolCopyBook.getHiveColumnTypes());
		}
		return hiveTypesInfos;
	}
	public List<String> getHiveComments() throws CobolSerdeException {
		if(hiveComments ==null){
			hiveComments = (List<String>) getNamesbyIgnoring(cobolCopyBook.getHiveColumnComments(0));
		}
		return hiveComments;
	}

	public List<ObjectInspector> getObjectInspectors() {
		if(objectInspectors==null){
			objectInspectors = (List<ObjectInspector>) getNamesbyIgnoring(cobolCopyBook.getObjectInspectors());
		}
		return objectInspectors;
	}

	@Override
	public String toString() {
		return cobolCopyBook.getDebugInfo();
	}
}
