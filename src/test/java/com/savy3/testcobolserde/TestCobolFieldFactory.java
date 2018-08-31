package com.savy3.testcobolserde;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeException;
import com.savy3.hadoop.hive.serde3.cobol.CobolFieldFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
public class TestCobolFieldFactory {
	CobolFieldFactory cff = new CobolFieldFactory();


	@Test
	public void testGetCobolField() throws CobolSerdeException {
		assertEquals("Testing getting Cobol Field",cff.getCobolField(" 00 COPYBOOK. " ).toString(),"CobolField :[ Name : copybook, type : null, offset :0 ]");
		assertEquals("Testing duplicate Cobol Field name",cff.getCobolField(" 01 COPYBOOK PIC X(10). " ).toString(),"CobolField :[ Name : copybook_1, type : varchar(10), offset :0 ]");
		
		assertEquals("varchar picture clause: Test 1",cff.getCobolField(" 01 COPYBOOK PIC x. " ).toString(),"CobolField :[ Name : copybook_2, type : varchar(1), offset :0 ]");
		assertEquals("varchar picture clause: Test 2",cff.getCobolField(" 01 COPYBOOK PIC x . " ).toString(),"CobolField :[ Name : copybook_3, type : varchar(1), offset :0 ]");
		assertEquals("varchar picture clause: Test 3",cff.getCobolField(" 01 COPYBOOK PIC xxxx . " ).toString(),"CobolField :[ Name : copybook_4, type : varchar(4), offset :0 ]");
		assertEquals("varchar picture clause: Test 4",cff.getCobolField(" 01 COPYBOOK PIC x(10) . " ).toString(),"CobolField :[ Name : copybook_5, type : varchar(10), offset :0 ]");
		assertEquals("varchar picture clause: Test 5",cff.getCobolField(" 01 COPYBOOK PIC x(10). " ).toString(),"CobolField :[ Name : copybook_6, type : varchar(10), offset :0 ]");
		assertEquals("varchar picture clause: Test 6",cff.getCobolField(" 01 COPYBOOK PIC xxxx. " ).toString(),"CobolField :[ Name : copybook_7, type : varchar(4), offset :0 ]");
		assertEquals("varchar picture clause: Test 7",cff.getCobolField(" 01 COPYBOOK PIC x(65354). " ).toString(),"CobolField :[ Name : copybook_8, type : varchar(65354), offset :0 ]");
		
		assertEquals("string  picture clause: Test 1",cff.getCobolField(" 01 String-field PIC x(65355). " ).toString(),"CobolField :[ Name : string_field, type : string, offset :0 ]");
		assertEquals("string  picture clause: Test 2",cff.getCobolField(" 01 String-field PIC x(65356). " ).toString(),"CobolField :[ Name : string_field_1, type : string, offset :0 ]");
		assertEquals("string  picture clause: Test 3",cff.getCobolField(" 01 String-field PIC x(65356) . " ).toString(),"CobolField :[ Name : string_field_2, type : string, offset :0 ]");
		assertEquals("string  picture clause: Test 4",cff.getCobolField(" 01 String-field PICTURE x(65356). " ).toString(),"CobolField :[ Name : string_field_3, type : string, offset :0 ]");
		
		assertEquals("string  decimal clause: Test 1",cff.getCobolField(" 01 decimal-field PICTURE S9(7)v9(7). " ).toString(),"CobolNumberField [compType=0, decimalLocation=7, name=decimal_field, debugInfo=01 decimal-field PICTURE S9(7)v9(7)., length=14, type=decimal(14,7), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 2",cff.getCobolField(" 01 decimal-field PICTURE S9(7)v9(7) COMP-3 . " ).toString(),"CobolNumberField [compType=3, decimalLocation=7, name=decimal_field_1, debugInfo=01 decimal-field PICTURE S9(7)v9(7) COMP-3 ., length=8, type=decimal(16,7), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 3",cff.getCobolField(" 01 decimal-field PICTURE S9(7)v9(7) COMP-3. " ).toString(),"CobolNumberField [compType=3, decimalLocation=7, name=decimal_field_2, debugInfo=01 decimal-field PICTURE S9(7)v9(7) COMP-3., length=8, type=decimal(16,7), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 4",cff.getCobolField(" 01 decimal-field PICTURE S9(7)v9(6). " ).toString(),"CobolNumberField [compType=0, decimalLocation=6, name=decimal_field_3, debugInfo=01 decimal-field PICTURE S9(7)v9(6)., length=13, type=decimal(13,6), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 5",cff.getCobolField(" 01 decimal-field PICTURE S9(7)v9(6) COMP-3 . " ).toString(),"CobolNumberField [compType=3, decimalLocation=6, name=decimal_field_4, debugInfo=01 decimal-field PICTURE S9(7)v9(6) COMP-3 ., length=7, type=decimal(14,6), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 6",cff.getCobolField(" 01 decimal-field PICTURE S9(7)v9(6) COMP-3. " ).toString(),"CobolNumberField [compType=3, decimalLocation=6, name=decimal_field_5, debugInfo=01 decimal-field PICTURE S9(7)v9(6) COMP-3., length=7, type=decimal(14,6), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 7",cff.getCobolField(" 01 decimal-field PICTURE S9(6)v9(7). " ).toString(),"CobolNumberField [compType=0, decimalLocation=7, name=decimal_field_6, debugInfo=01 decimal-field PICTURE S9(6)v9(7)., length=13, type=decimal(13,7), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 8",cff.getCobolField(" 01 decimal-field PICTURE S9(6)v9(7) COMP-3 . " ).toString(),"CobolNumberField [compType=3, decimalLocation=7, name=decimal_field_7, debugInfo=01 decimal-field PICTURE S9(6)v9(7) COMP-3 ., length=7, type=decimal(14,7), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 9",cff.getCobolField(" 01 decimal-field PICTURE S9(6)v9(7) COMP-3. " ).toString(),"CobolNumberField [compType=3, decimalLocation=7, name=decimal_field_8, debugInfo=01 decimal-field PICTURE S9(6)v9(7) COMP-3., length=7, type=decimal(14,7), levelNo=1, offset=0]");
		assertEquals("string  decimal clause: Test 10",cff.getCobolField(" 01 decimal-field PICTURE S9(2) COMP-3. " ).toString(),"CobolNumberField [compType=3, decimalLocation=0, name=decimal_field_9, debugInfo=01 decimal-field PICTURE S9(2) COMP-3., length=2, type=smallint, levelNo=1, offset=0]");
		
		
	}
	
}
