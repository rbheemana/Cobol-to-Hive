package com.savy3.testcobolserde;

import com.savy3.hadoop.hive.serde2.cobol.CobolCopybook;
import junit.framework.TestCase;

public class TestCobolCopybook extends TestCase {

	CobolCopybook ccb = new CobolCopybook(
			"01 WS-VAR. 05 WS-NAME PIC X(12). 01 ws-sudha. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");



	public void testGetFieldNames() {
		assertEquals(ccb.getFieldNames().get(0), "ws_var");
		ccb = new CobolCopybook("01 WS-NAME PIC X(12). 01 ws-sudha. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");
		 assertEquals(ccb.getFieldNames().get(1),"ws_sudha");
		// assertEquals(ccb.getFieldNames().get(2),"ws_marks");
		// assertEquals(ccb.getFieldNames().get(3),"ws_nickname");

	}

	public void testGetFiledTypes() {
		assertEquals(
				ccb.getFieldTypes().get(0),
				"struct<ws_name:string>");
		assertEquals(
				ccb.getFieldTypes().get(1),
				"struct<ws_marks_length:smallint,ws_marks:array<struct<ws_mark:smallint>>,ws_nickname:string>");

	}
	public void testGetFiledProperties() {
		System.out.println(ccb.getFieldProperties().get(1).toString());
		assertEquals(
				ccb.getFieldProperties().get(0).toString(),
				"[{length=0}, {length=12}]");
		assertEquals(
				ccb.getFieldProperties().get(1).toString(),
				"[{length=0}, {length=3}, {length=0, dcol=1}, {length=3}, {length=6}]");

	}

}
