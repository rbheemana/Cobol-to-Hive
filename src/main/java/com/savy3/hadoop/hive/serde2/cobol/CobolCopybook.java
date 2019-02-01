package com.savy3.hadoop.hive.serde2.cobol;

import com.savy3.hadoop.hive.serde3.cobol.CobolSerdeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.*;

public class CobolCopybook {

	List<String> fieldLines;
	List<String> fieldNames;
	List<String> fieldTypes;
	List<Map<String, Integer>> fieldProperties;
	Map<String, Integer> uniqueNames;
	Map<String, Integer> namePrevCol;
	int columnNos, prevColumn;

	public List<Map<String, Integer>> getFieldProperties() {
		return fieldProperties;
	}

	List<TypeInfo> fieldTypeInfos;

	public List<TypeInfo> getFieldTypeInfos() {
		return fieldTypeInfos;
	}

	List<ObjectInspector> fieldOIs;

	public CobolCopybook(String layout) {
		/*
		 * Split the cobol layout seperate lines using ". " as delimiter, space
		 * is also included because dot can also occur in between Picture
		 * clause.
		 */
		layout = layout.replaceAll("[\\t\\n\\r]", " ").replaceAll("( )+", " ").trim();
		this.fieldLines = Arrays.asList(layout.split("\\.\\s"));
		this.fieldNames = new LinkedList<String>();
		this.fieldTypes = new LinkedList<String>();
		this.fieldTypeInfos = new LinkedList<TypeInfo>();
		this.fieldOIs = new LinkedList<ObjectInspector>();
		this.uniqueNames = new HashMap<String, Integer>();
		this.namePrevCol = new HashMap<String, Integer>();
		this.fieldProperties = new ArrayList<Map<String, Integer>>();
		new LinkedList<CobolFieldDecl>();

		try {
			buildLayout();
		} catch (CobolSerdeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/* Map and extract hive column names and types info from cobol layout */
	private void buildLayout() throws CobolSerdeException {
		int occurs = 1, dependId = -1, dependLevel = 99;
		List<CobolFieldDecl> groupedColumns = new ArrayList<CobolFieldDecl>();
		CobolFieldDecl cfd = null;
		for (String fieldLine : fieldLines) {
			fieldLine = fieldLine.replaceAll("[\\t\\n\\r]", " ").replaceAll("( )+", " ").trim();
			if (fieldLine.isEmpty())
				continue;
			cfd = new CobolFieldDecl(fieldLine);
			if (cfd.levelNo == 88)
				continue;
			if (cfd.isIgnoreField & !cfd.isRedefines & !cfd.isDepending) {
				namePrevCol.put(cfd.getFieldName(), prevColumn);
				continue;
			}
			if (cfd.isDepending) {
				occurs = cfd.fieldProperties.get("times");
				if (cfd.getDepending() == "OCCURS"){
					dependId = -2;
					dependLevel = cfd.levelNo;
					continue;
				}
				int i = 0;
				for (String s : fieldNames) {
					if (s.equals(cfd.getDepending())) {
						dependId = i + 1;
						dependLevel = cfd.levelNo;
						break;
					}
					i++;
				} //
			} else if (cfd.isRedefines) {
				if (!groupedColumns.isEmpty()) {
					for (int i = 0; i < occurs; i++) {
						for (CobolFieldDecl gcfd : groupedColumns) {
							setFieldVars(gcfd, i, dependId);
						}
					}
					groupedColumns.clear();
				}
				if (namePrevCol.get(cfd.getRedefines()) != null) {
					namePrevCol.put(cfd.getFieldName(),
							namePrevCol.get(cfd.getRedefines()));
					prevColumn = namePrevCol.get(cfd.getRedefines());
					dependLevel = cfd.levelNo;
				} else {
					System.out
							.println("Could find corresponding redefines field"
									+ cfd.getRedefines());
				}

				if (!cfd.isIgnoreField) {
					setFieldVars(cfd, 0, 0);
					dependLevel = 99;
				}
			} else if (dependLevel < cfd.getLevelNo()) {
				groupedColumns.add(cfd);
			} else {
				for (int i = 0; i < occurs; i++) {
					for (CobolFieldDecl gcfd : groupedColumns) {
						setFieldVars(gcfd, i, dependId);
					}
				}
				groupedColumns.clear();
				occurs=1;
				setFieldVars(cfd, 0, 0);
				dependLevel = 99;
			}
		}
		if (!groupedColumns.isEmpty()) {
			for (int i = 0; i < occurs; i++) {
				for (CobolFieldDecl gcfd : groupedColumns) {
					setFieldVars(gcfd, i, dependId);
				}
			}
			groupedColumns.clear();
			occurs=1;
		}
	}

	/* Add column names and types to list */
	private void setFieldVars(CobolFieldDecl cfd, int occurance, int dependId) {

		cfd.fieldProperties.put("prev.col", prevColumn);
		cfd.fieldProperties.put("occurance", occurance);
		
		// get unique fieldName
		String fieldName = "";
		if (uniqueNames.get(cfd.getFieldName()) == null) {
			fieldName = cfd.getFieldName();
		} else {
			uniqueNames.put(cfd.getFieldName(),
					uniqueNames.get(cfd.getFieldName()) + 1);
			fieldName = cfd.getFieldName() + "_"
					+ uniqueNames.get(cfd.getFieldName());

		}

		uniqueNames.put(fieldName, 0);
		namePrevCol.put(fieldName, prevColumn);
		
		String fieldType = cfd.getFieldType();
		fieldNames.add(fieldName);
		fieldTypes.add(fieldType);
		fieldTypeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(fieldType));
		fieldOIs.add(TypeInfoUtils
				.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoUtils
						.getTypeInfoFromTypeString(fieldType)));
		columnNos++;

		cfd.fieldProperties.put("id", columnNos);
		cfd.fieldProperties.put("depend.id", dependId);
		Map<String, Integer> prop = new HashMap<String, Integer>(
				cfd.fieldProperties);

		fieldProperties.add(prop);
		prevColumn = columnNos;

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