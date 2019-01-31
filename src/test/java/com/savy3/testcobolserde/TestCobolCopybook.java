package com.savy3.testcobolserde;

import com.savy3.hadoop.hive.serde2.cobol.CobolCopybook;
import junit.framework.TestCase;

public class TestCobolCopybook extends TestCase {

	CobolCopybook ccb = new CobolCopybook(
			"01 WS-VAR. 05 WS-NAME PIC X(12). 01 ws-sudha. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");



	public void testGetFieldNames() {
		assertEquals("ws_name", ccb.getFieldNames().get(0));
		ccb = new CobolCopybook("01 WS-NAME PIC X(12). 01 ws-sudha. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");
		assertEquals("ws_marks_length", ccb.getFieldNames().get(1));
		assertEquals("ws_mark", ccb.getFieldNames().get(2));
		assertEquals("ws_mark_1", ccb.getFieldNames().get(3));

	}

	public void testGetFiledTypes() {
		assertEquals("varchar(12)", ccb.getFieldTypes().get(0));
		assertEquals("smallint",
				ccb.getFieldTypes().get(1));

	}
	public void testGetFiledProperties() {
		assertEquals("{length=12, id=1, depend.id=0, prev.col=0, occurance=0}",
				ccb.getFieldProperties().get(0).toString());
		assertEquals("{length=3, id=2, depend.id=0, prev.col=1, occurance=0}",
				ccb.getFieldProperties().get(1).toString());

	}

}
