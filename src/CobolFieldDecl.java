package com.savy3.cobolserde;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class CobolFieldDecl {
	int levelNo;
	String fieldName;
	String picClause;
	String value;
	List<String> fieldParts;
	String fieldType;
	Map<String,Integer> fieldProperties;
	private TypeInfo fieldTypeInfo;
	boolean isIgnoreField;
	boolean isArray;
	public CobolFieldDecl(String line) throws NumberFormatException{
		this.fieldParts = Arrays.asList(line.trim().toLowerCase().split("\\s"));
		this.fieldProperties= new HashMap<String,Integer>();
		this.fieldType="";
		setLevelNo();
		setFieldName();
		setPicClause();
		setValue();
		if (picClause != "") {
			setFieldTypeLength();
			setFieldTypeInfo();

		} else {
			isIgnoreField = true;

			if (getDepending() != null) {
				fieldType = "array<struct";
				fieldProperties.put("length", 0);
			} else {
				fieldType = "struct";
				fieldProperties.put("length", 0);
			}
		}
		
			
	}
	
	
	private void setFieldTypeInfo() {
		this.fieldTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(this.fieldType);
		
	}

	public TypeInfo getFieldTypeInfo() {
		return fieldTypeInfo;
	}

	void setLevelNo() throws NumberFormatException {
		try{
			this.levelNo = Integer.parseInt(this.fieldParts.get(0).trim());
		}
		catch (NumberFormatException e){
			throw e;
		}		
	}
	
	void setFieldName() {		
		//if(this.fieldParts.size() > 2)
			this.fieldName = this.fieldParts.get(1).trim().replace('-', '_');
		//else 
		//	this.fieldName = "";
	}
	String getDepending(){
		int dependIndex = this.fieldParts.indexOf("depending");
		if (dependIndex > -1){
			if(this.fieldParts.get(dependIndex+1).equalsIgnoreCase("on"))
				return  this.fieldParts.get(dependIndex+2).trim().replace('-', '_');
			else
				return this.fieldParts.get(dependIndex+1).trim().replace('-', '_');
		}
		else
			return null;
	}
	
	void setPicClause() {
		int picIndex = this.fieldParts.indexOf("pic");
		if (picIndex > -1)
			this.picClause = this.fieldParts.get(picIndex+1);
		else
			this.picClause ="";
	}

	void setValue() {
		int valIndex = this.fieldParts.indexOf("value");
		if (valIndex > -1){
			this.value = this.fieldParts.get(valIndex+1);		
		}
		else{
			this.value ="";
		}
	}
	
	void setFieldTypeLength() throws RuntimeException, NumberFormatException{
		
		switch(this.picClause.charAt(0)){
			case 'x':	
			case 'a':	fieldType ="string";validateStringFormat();break;
			case '9':
			case 'z':
			case '+':
			case '-':
			case '$':
			case 's': fieldType =  "integer";validateNumberFormat();break;
			default: fieldType = "";
			//may need to throw exception
		}
		//System.out.println("picClause -"+picClause+fieldType);
		if(fieldType == "string"){
						
		}

	}
	
	void validateStringFormat(){
		if(this.picClause.contains("(")){
			
			String[] s = this.picClause.split("\\(|\\)|\\.");
			//System.out.println("picClause contains ("+s.length);
			if(s.length == 2){
				try{
					fieldProperties.put("length", Integer.parseInt(s[1]));
				}
				catch (NumberFormatException e){
					throw e;
				}
			}
			else{
				throw new RuntimeException("Alphanumeric Picture clause has more brackets");
			}
		}
		else{
			if (this.picClause.matches("x+[.]|a+[.]"))
				fieldProperties.put("length",  this.picClause.length());
			else{
				this.fieldType = "";
				throw new RuntimeException("Alphanumeric Picture clause incorrect");
				
			}
		}
//		if (this.picClause.length() == 1){
//			fieldType="char";
//		}
	}
	
	void validateNumberFormat(){
		String[] s = this.picClause.split("\\(|\\)|\\.");
		try{
			switch(s.length){
			 case 1:	fieldProperties.put("length", s[0].length());break;
			 case 2: 	fieldProperties.put("length", Integer.parseInt(s[1]));break;
			 case 3:	fieldProperties.put("length", Integer.parseInt(s[1])+s[2].length()-1);
			 			fieldProperties.put("decimal", Integer.parseInt(s[1]));
			 			break;
			 case 4:	fieldProperties.put("length", Integer.parseInt(s[1])+ Integer.parseInt(s[3]));
			 			fieldProperties.put("decimal",Integer.parseInt(s[1]));break;
			 default:
				 throw new RuntimeException("Alphanumeric Picture clause has more brackets");
			}
			if (s.length < 3){
				if (fieldProperties.get("length") < 3)
					this.fieldType = "tinyint";
				else if (fieldProperties.get("length") <5)
					this.fieldType = "smallint";
				else if (fieldProperties.get("length") < 10)
					this.fieldType = "int";
				else if (fieldProperties.get("length") < 19)
					this.fieldType = "bigint";
				else
					this.fieldType = "string";
				
			}
			else{
				if (fieldProperties.get("length")< 10)
					this.fieldType = "float";
				else if (fieldProperties.get("length") < 19)
					this.fieldType = "double";
				else
					this.fieldType = "string";
			}
		}
		catch (NumberFormatException e){
				throw e;
		}
		
		
	}


	public String getFieldType() {
		//System.out.println("LevelNo:"+levelNo+fieldType);
		return fieldType;
	}



	public Map<String, Integer> getFieldProperties() {
		return fieldProperties;
	}

	public List<String> getFieldParts() {
		return fieldParts;
	}

	public int getLevelNo() {
		return levelNo;
	}

	public String getFieldName() {
		return fieldName;
	}

	public String getPicClause() {
		return picClause;
	}

	public String getValue() {
		return value;
	}
	
	
	

}
