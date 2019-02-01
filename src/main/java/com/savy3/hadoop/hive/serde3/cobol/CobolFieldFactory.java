package com.savy3.hadoop.hive.serde3.cobol;

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
        String cobolName = getCobolName(fieldParts.get(1));
        // else {
        CobolGroupField cgf = getGroupType(cobolName, line, fieldParts, levelNo);
        if (cgf != null) {
            uniqueCobolField.put(cgf.name, cgf);
            CobolField cf = getElementaryType(cobolName, line, fieldParts, levelNo);
            if (cf != null) {
                uniqueCobolField.put(cf.name, cf);
                cgf.add(cf);
            }
            return cgf;
        } else {
            CobolField cf = getElementaryType(cobolName, line, fieldParts, levelNo);
            if (cf != null) {
                uniqueCobolField.put(cf.name, cf);
                return cf;
            } else {
                String name = getUniqueColumnName(cobolName);
                cf = new CobolGroupField(line, levelNo, name, 1, 0);
                uniqueCobolField.put(name, cf);
                return cf;
            }
		}
	}

    private String getCobolName(String name) {
        return name.trim()
                .replaceAll("[^a-zA-Z0-9\\-]", "")
                .replace('-', '_')
                .replace('.', ' ').trim();

    }

    private CobolGroupField getGroupType(String cobolName, String line, List<String> fieldParts, int levelNo) throws CobolSerdeException {
        int redefineIndex = fieldParts.indexOf("redefines");
        if (redefineIndex > -1) {
            String name = getUniqueColumnName(cobolName);
            String redefinedField = getCobolName(fieldParts.get(redefineIndex + 1));
            if (uniqueCobolField.containsKey(redefinedField)) {
                CobolField rcf = uniqueCobolField.get(redefinedField);
                return new CobolGrpRedefinesField(line, levelNo, name, rcf);

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

                    return new CobolGrpDependField(line, levelNo, name, occurs,
                            uniqueCobolField.get(lookupField));

                }
                return new CobolGroupField(line, levelNo, name, occurs, 0);

            } else {
                String name = getUniqueColumnName(cobolName);
                int occurs = Integer.parseInt(fieldParts
                        .get(occursIndex + 1));
                int occurToIndex = fieldParts.indexOf("to");
                if (occurToIndex > -1) {
                    occurs = Integer.parseInt(fieldParts
                            .get(occurTimesIndex - 1));
                }
                return new CobolGroupField(line, levelNo, name, occurs, 0);

            }
        }
        return null;
    }

    private CobolField getElementaryType(String cobolName, String line, List<String> fieldParts, int levelNo) throws CobolSerdeException {
        int picIndex = fieldParts.indexOf("pic");
        if (picIndex == -1)
            picIndex = fieldParts.indexOf("picture");
        if (picIndex > -1) {
            String name = getUniqueColumnName(cobolName);
            String picClause = fieldParts.get(picIndex + 1);
            switch (picClause.charAt(0)) {
                case 'x':
                case 'a':
                    return new CobolStringField(line, levelNo, name, picClause);
                // return cf;
                case 'v':
                case 'n':
                case '9':
                case 'z':
                case '+':
                case '-':
                case '$':
                case 's':
                    int compType = 0;
                    if ((fieldParts.indexOf("comp-3") > -1) || (fieldParts.indexOf("comp-3.") > -1)) {
                        compType = 3;
                    }
                    if ((fieldParts.indexOf("comp-4") > -1) || (fieldParts.indexOf("comp-4.") > -1)) {
                        compType = 4;
                    }
                    if ((fieldParts.indexOf("comp") > -1) || (fieldParts.indexOf("comp.") > -1)) {
                        compType = 4;
                    }
                    return new CobolNumberField(line, levelNo, name, picClause,
                            compType);
                // return cf;
                default:
                    break;
                // may need to throw exception
            }
        }
        return null;
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
