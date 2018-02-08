package com.savy3.hadoop.hive.serde2.cobol;

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
	Map<String, Integer> fieldProperties;
	private TypeInfo fieldTypeInfo;
	boolean isIgnoreField, isDepending, isRedefines;
	boolean isArray;

	public CobolFieldDecl(String line) throws NumberFormatException {
		line = line.replaceAll("[\\t\\n\\r]", " ");
		line = line.replaceAll("( )+", " ").trim();
		this.fieldParts = Arrays.asList(line.trim().toLowerCase().split("\\s"));
		if (fieldParts.size() <= 1)
			return;
		this.fieldProperties = new HashMap<String, Integer>();
		this.fieldType = "";

		setLevelNo();
		setFieldName();
		setPicClause();
		setValue();
		if (getRedefines() != null)
			isRedefines = true;
		if (getDepending() != null)
			isDepending = true;
		if (picClause != "") {
			setFieldTypeLength();
			if (this.picClause != "")
				setFieldTypeInfo();

		} else {
			isIgnoreField = true;
			if (getDepending() != null) {
				isDepending = true;
				fieldType = "array<struct";
				fieldProperties.put("length", 0);
			} else {
				if (getRedefines() != null)
					isRedefines = true;

				fieldType = "struct";
				fieldProperties.put("length", 0);
			}
		}
	}

	@Override
	public String toString() {
		return "CobolFieldDecl [levelNo=" + levelNo + ", fieldName="
				+ fieldName + ", picClause=" + picClause + ", value=" + value
				+ ", fieldParts=" + fieldParts + ", fieldType=" + fieldType
				+ ", fieldProperties=" + fieldProperties + ", fieldTypeInfo="
				+ fieldTypeInfo + ", isIgnoreField=" + isIgnoreField
				+ ", isDepending=" + isDepending + ", isRedefines="
				+ isRedefines + ", isArray=" + isArray + "]";
	}

	private void setFieldTypeInfo() {
		this.fieldTypeInfo = TypeInfoUtils
				.getTypeInfoFromTypeString(this.fieldType);
	}

	public TypeInfo getFieldTypeInfo() {
		return fieldTypeInfo;
	}

	void setLevelNo() throws NumberFormatException {
		try {
			this.levelNo = Integer.parseInt(this.fieldParts.get(0).trim());
		} catch (NumberFormatException e) {
			System.out.println("Exception with line " + fieldParts);
			throw e;
		}
	}

	void setFieldName() {
		this.fieldName = this.fieldParts.get(1).trim().replace('-', '_')
				.replace('.', ' ').trim();
	}

	String getRedefines() {

		int redefineIndex = this.fieldParts.indexOf("redefines");
		if (redefineIndex > -1)
			return this.fieldParts.get(redefineIndex + 1).trim()
					.replace('-', '_');
		else
			return null;
	}

	String getDepending() {
		int occurTimesIndex = this.fieldParts.indexOf("times");
		if (occurTimesIndex > -1) {
			fieldProperties.put("times",
					Integer.parseInt(this.fieldParts.get(occurTimesIndex - 1)));
		} else
			return null;
		int occurToIndex = this.fieldParts.indexOf("to");
		if (occurToIndex > -1) {
			fieldProperties.put(
					"times",
					Integer.parseInt(this.fieldParts.get(occurTimesIndex - 1))
							- Integer.parseInt(this.fieldParts
									.get(occurToIndex - 1)) + 1);
		}

		int dependIndex = this.fieldParts.indexOf("depending");
		if (dependIndex > -1) {
			if (this.fieldParts.get(dependIndex + 1).equalsIgnoreCase("on"))
				return this.fieldParts.get(dependIndex + 2).trim()
						.replace('-', '_');
			else
				return this.fieldParts.get(dependIndex + 1).trim()
						.replace('-', '_');

		} else
			return "OCCURS";
	}

	void setPicClause() {
		int picIndex = this.fieldParts.indexOf("pic");

		if (picIndex > -1)
			this.picClause = this.fieldParts.get(picIndex + 1);
		else
			this.picClause = "";
		// System.out.println("picIndex"+picClause);
	}

	void setValue() {
		int valIndex = this.fieldParts.indexOf("value");
		if (valIndex > -1) {
			this.value = this.fieldParts.get(valIndex + 1);
		} else {
			this.value = "";
		}
	}

	void setFieldTypeLength() throws RuntimeException, NumberFormatException {
		switch (this.picClause.charAt(0)) {
		case 'x':
		case 'a':
			fieldType = "string";
			validateStringFormat();
			break;
		case '9':
		case 'z':
		case '+':
		case '-':
		case '$':
		case 's':
			fieldType = "integer";
			validateNumberFormat();
			break;
		default:
			fieldType = "";
			// may need to throw exception
		}
		if (fieldType == "string") {
		}
	}

	void validateStringFormat() {
		if (this.picClause.contains("(")) {
			String[] s = this.picClause.split("\\(|\\)|\\.");
			if (s.length == 2) {
				try {
					fieldProperties.put("length", Integer.parseInt(s[1]));
				} catch (NumberFormatException e) {
					throw e;
				}
			} else {
				throw new RuntimeException(
						"Alphanumeric Picture clause has more brackets");
			}
		} else {
			if (this.picClause.matches("x+|a+"))
				fieldProperties.put("length", this.picClause.length());
			else {
				this.fieldType = "";
				throw new RuntimeException(
						"Alphanumeric Picture clause incorrect '"
								+ this.picClause + "' for field"
								+ this.fieldName + " level No:" + this.levelNo);

			}
		}
		if (fieldProperties.get("length") < 65355) {
			this.fieldType = "varchar(" + fieldProperties.get("length") + ")";
		}
	}

	void validateNumberFormat() {
		String[] s = this.picClause.split("\\(|\\)|\\.");
		int divideFactor = 1;
		try {

			if (this.fieldParts.indexOf("comp-3") > -1) {
				fieldProperties.put("comp", 3);
				divideFactor = 2;
			}

			switch (s.length) {
			case 1:
				fieldProperties.put(
						"length",
						new Integer((int) Math.ceil((double) s[0].length()
								/ divideFactor)));
				break;
			case 2:
				fieldProperties.put(
						"length",
						new Integer((int) Math.ceil(Double.parseDouble(s[1])
								/ divideFactor)));
				break;
			case 3:
				fieldProperties.put(
						"length",
						new Integer((int) Math.ceil((Double.parseDouble(s[1])
								+ s[2].length() - 1)
								/ divideFactor)));
				if (this.picClause.charAt(0) == '9') {
					fieldProperties.put("decimal", Integer.parseInt(s[1]));
				} else {
					fieldProperties.put("decimal", Integer.parseInt(s[1]) - 1);
				}
				break;
			case 4:
				fieldProperties
						.put("length",
								new Integer((int) Math.ceil((Double
										.parseDouble(s[1]) + Integer
										.parseInt(s[3]))
										/ divideFactor)));
				if (this.picClause.charAt(0) == '9') {
					fieldProperties.put("decimal", Integer.parseInt(s[1]));
				} else {
					fieldProperties.put("decimal", Integer.parseInt(s[1]) - 1);
				}
				break;
			default:
				throw new RuntimeException(
						"Alphanumeric Picture clause has more brackets");
			}
			// System.out.println("CFD: length"+s.length+"-"+Math.ceil(Double.parseDouble(s[1])/(double)divideFactor)+"-"+fieldProperties.get("length"));
			if (s.length < 3) {
				if (fieldProperties.get("length") * divideFactor < 3)
					this.fieldType = "tinyint";
				else if (fieldProperties.get("length") * divideFactor < 5)
					this.fieldType = "smallint";
				else if (fieldProperties.get("length") * divideFactor < 10)
					this.fieldType = "int";
				else if (fieldProperties.get("length") * divideFactor < 19)
					this.fieldType = "bigint";
				else
					this.fieldType = "string";

			} else {
				this.fieldType = "decimal(" + fieldProperties.get("length")
						* divideFactor + "," + (fieldProperties.get("length")
								* divideFactor - fieldProperties.get("decimal"))
						+ ")";
			}
		} catch (NumberFormatException e) {
			throw e;
		}
	}

	public String getFieldType() {
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