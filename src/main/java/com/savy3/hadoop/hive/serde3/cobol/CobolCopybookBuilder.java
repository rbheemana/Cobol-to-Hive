package com.savy3.hadoop.hive.serde3.cobol;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeException;

import java.util.Arrays;
import java.util.List;
import java.util.Stack;


public class CobolCopybookBuilder {
	private Stack<CobolGroupField> st = new Stack<CobolGroupField>();
	CobolGroupField level0Field;

	public CobolGroupField getCobolCopybook(String layout) throws CobolSerdeException {
		/*
		 * Split the cobol layout seperate lines using ". " as delimiter, space
		 * is also included because dot can also occur in between Picture
		 * clause.
		 */
		CobolFieldFactory cff = new CobolFieldFactory();
		level0Field = (CobolGroupField) cff.getCobolField(" 00 COPYBOOK. ");
		layout = layout.replaceAll("[\\t\\n\\r]", " ").replaceAll("( )+", " ")
				.trim();
		try {
			buildLayout(cff, Arrays.asList(layout.split("\\.\\s")));
			while (!st.isEmpty()) {
				CobolGroupField cgf = (CobolGroupField) st.pop();
				if (cgf.getLevelNo() == 1) {
					level0Field.add(cgf);
				}
			}
		} catch (CobolSerdeException e) {
			e.printStackTrace();
		}
		return level0Field;
	}

	/* Map and extract hive column names and types info from cobol layout */
	private void buildLayout(CobolFieldFactory cff, List<String> fieldLines)
			throws CobolSerdeException {
		CobolField pcf = null;
		CobolGroupField cgcf = null;
		for (String fieldLine : fieldLines) {
			CobolField cf = cff.getCobolField(fieldLine);
			// System.out.println(cf.getName());
			if (cf != null) {
				if (pcf == null) {// First element
					if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
						throw new CobolSerdeException(
								"First Field in the copybook is not Group field"
										+ cf.getDebugInfo());
					} else {
						if (cf.getLevelNo() != 1) {
							throw new CobolSerdeException(
									"First Field in the copybook is not 01 field"
											+ cf.getDebugInfo());
						} else {
							pcf = cf;
							cgcf = (CobolGroupField) cf;
							st.push(cgcf);
							continue;
						}
					}
				}
				// current element is elementary
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					cgcf = getGroupField(cf.getLevelNo());
					cgcf.add(cf);
					st.push(cgcf);
				} else {// current element is a group element
					cgcf = getGroupField(cf.getLevelNo());
					cgcf.add(cf);
					st.push(cgcf);
					cgcf = (CobolGroupField) cf;
					st.push(cgcf);
				}
				pcf = cf;
			}

		}

	}

	public CobolGroupField getGroupField(int levelNo) {
		CobolGroupField cgcf = st.pop();
		while (cgcf.getLevelNo() >= levelNo) {
			cgcf = (CobolGroupField) st.pop();
		}
		return cgcf;
	}


}
