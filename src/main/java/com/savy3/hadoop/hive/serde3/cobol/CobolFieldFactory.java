package com.savy3.hadoop.hive.serde3.cobol;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class CobolFieldFactory {
	// use getCobolField method to get object of type CobolField
	private HashMap<String, CobolField> uniqueCobolField; // = new HashMap<String, CobolField>();
	private HashMap<String, Integer> uniqueNames; // = new HashMap<String, Integer>();

    public CobolFieldFactory() {
		uniqueCobolField = new HashMap<String, CobolField>();
		uniqueNames = new HashMap<String, Integer>();
	}
	public CobolField getCobolField(String line) throws CobolSerdeException {
		
		line = line.replaceAll("[\\t\\n\\r]", " ").replaceAll("( )+", " ")
				.trim();
		List<String> fieldParts = Arrays.asList(line.trim().toLowerCase()
				.split("\\s"));
		int levelNo = 0;
		if (fieldParts.size() <= 1)
			return null;
		try {
			if (Integer.parseInt(fieldParts.get(0).trim()) == 88) {
				return null;
			} else {
				levelNo = Integer.parseInt(fieldParts.get(0).trim());
			}
		} catch (NumberFormatException e) {
			System.out.println("Exception with line :" + line);
			throw e;
		}
		CobolField cf = null;
		String cobolName = fieldParts.get(1).trim().replace('-', '_')
				.replace('.', ' ').trim();

		// System.out.println(name);
		
		int picIndex = fieldParts.indexOf("pic");
		if (picIndex == -1)
			picIndex = fieldParts.indexOf("picture");
		if (picIndex > -1) {
			String name = getUniqueColumnName(cobolName);
			String picClause = fieldParts.get(picIndex + 1);
			switch (picClause.charAt(0)) {
			case 'x':
			case 'a':
				cf = new CobolStringField(line, levelNo, name, picClause);
				uniqueCobolField.put(name, cf);
				break;
			// return cf;
			case '9':
			case 'z':
			case '+':
			case '-':
			case '$':
			case 's':
				int compType = 0;
				if ((fieldParts.indexOf("comp-3") > -1)||(fieldParts.indexOf("comp-3.") > -1)) {
					compType = 3;
				}
				if ((fieldParts.indexOf("comp-4") > -1)||(fieldParts.indexOf("comp-4.") > -1)) {
					compType = 4;
				}
				if ((fieldParts.indexOf("comp") > -1)||(fieldParts.indexOf("comp.") > -1)) {
					compType = 4;
				}
				cf = new CobolNumberField(line, levelNo, name, picClause,
						compType);
				uniqueCobolField.put(name, cf);
				break;
			// return cf;
			default:
				break;
			// may need to throw exception
			}
		} // else {
		CobolGroupField cgf;
		int redefineIndex = fieldParts.indexOf("redefines");
		if (redefineIndex > -1) {
			String name = getUniqueColumnName(cobolName);
			String redefinedField = fieldParts.get(redefineIndex + 1).trim()
					.replace('-', '_');
			if (uniqueCobolField.containsKey(redefinedField)) {
				CobolField rcf = uniqueCobolField.get(redefinedField);
				cgf = new CobolGrpRedefinesField(line, levelNo, name, rcf);
				uniqueCobolField.put(name, cgf);
				if (cf != null)
					cgf.add(cf);
				return cgf;
			} else {
				throw new CobolSerdeException("Redefined field is not found"
						+ line + "\n---" + uniqueCobolField.keySet().toString());
			}
		}
		int occursIndex = fieldParts.indexOf("occurs");
		if (occursIndex > -1) {
			int occurTimesIndex = fieldParts.indexOf("times");
			if (occurTimesIndex > -1) {
				String name = getUniqueColumnName(cobolName);
				int occurs = Integer.parseInt(fieldParts
						.get(occurTimesIndex - 1));
				int occurToIndex = fieldParts.indexOf("to");
				if (occurToIndex > -1) {
					occurs = Integer.parseInt(fieldParts
							.get(occurTimesIndex - 1));
				}

				int dependIndex = fieldParts.indexOf("depending");
				if (dependIndex > -1) {
					String lookupField;
					if (fieldParts.get(dependIndex + 1).equalsIgnoreCase("on")) {
						lookupField = new String(fieldParts
								.get(dependIndex + 2).trim().replace('-', '_'));
					} else {
						lookupField = new String(fieldParts
								.get(dependIndex + 1).trim().replace('-', '_'));
					}

					cgf = new CobolGrpDependField(line, levelNo, name, occurs,
							uniqueCobolField.get(lookupField));
					uniqueCobolField.put(name, cgf);
					if (cf != null)
						cgf.add(cf);
					return cgf;

				}
				cgf = new CobolGroupField(line, levelNo, name, occurs, 0);
				uniqueCobolField.put(name, cgf);
				if (cf != null)
					cgf.add(cf);
				return cgf;

			} else {
				String name = getUniqueColumnName(cobolName);
				int occurs = Integer.parseInt(fieldParts
						.get(occursIndex + 1));
				int occurToIndex = fieldParts.indexOf("to");
				if (occurToIndex > -1) {
					occurs = Integer.parseInt(fieldParts
							.get(occurTimesIndex - 1));
				}
				cgf = new CobolGroupField(line, levelNo, name, occurs, 0);
				uniqueCobolField.put(name, cgf);
				if (cf != null)
					cgf.add(cf);
				return cgf;

			}

		}
		if (cf == null) {
			String name = getUniqueColumnName(cobolName);
			cf = new CobolGroupField(line, levelNo, name, 1, 0);
			uniqueCobolField.put(name, cf);
			return cf;
		} else {
			return cf;
		}
	}
	String getUniqueColumnName(String cobolName){
		String name = "";
		if (uniqueNames.get(cobolName) == null) {
			uniqueNames.put(cobolName,0);
			name = cobolName;
		} else {
			uniqueNames.put(cobolName,
					uniqueNames.get(cobolName) + 1);
			name = cobolName + "_"
					+ uniqueNames.get(cobolName);
			name = getUniqueColumnName(name);
			
		}
		return name;
	}

}
