package com.savy3.testcobolserde;
import com.savy3.hadoop.hive.serde2.cobol.CobolFieldDecl;

import junit.framework.TestCase;


public class testCobolFieldDecl extends TestCase {

	public void testCobolFieldDecl() {
		CobolFieldDecl cfd = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd.getFieldName(),"ws_names");
		CobolFieldDecl cfd1 = new CobolFieldDecl("   	01 WS-NAMES PIC .");
		assertEquals(cfd1.getFieldName(),"ws_names");
		try{
			CobolFieldDecl cfd2 = new CobolFieldDecl("Ws-01 WS-NAMES.");
			assertNotSame(cfd2.getFieldName(),"ws_names");
		}
		catch(NumberFormatException e){
			System.out.println("error in layout");
		}
		
		
	}
	
	public void testGetPicClause(){
		CobolFieldDecl cfd = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd.getPicClause(),"");
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getPicClause(),"");
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getPicClause(),"x(10).");
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10).");
		assertEquals(cfd3.getPicClause(),"x(10).");
	}
	public void testGetValue(){
		CobolFieldDecl cfd = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd.getValue(),"");
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getValue(),"");
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getValue(),"");
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10).");
		assertEquals(cfd3.getValue(),"");
		CobolFieldDecl cfd4 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10) VALUE 'ram'.");
		assertEquals(cfd4.getValue(),"'ram'.");
	}
	public void testGetFieldType(){
		System.out.println("testing GetFieldType..");
		
		CobolFieldDecl cfd = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd.getFieldType(),"struct");
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getFieldType(),"");
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getFieldType(),"string");
		
		try{
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10)V9(20).");
		assertEquals(cfd3.getFieldType(),"");
		}
		catch(RuntimeException e){
			System.out.println("Alphanumeric pic clause Error");
		}
		
		CobolFieldDecl cfd4 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10) VALUE 'ram'.");
		assertEquals(cfd4.getFieldType(),"string");
		CobolFieldDecl cfd5 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10) VALUE 'ram'.");
		assertEquals(cfd5.getFieldType(),"bigint");
		CobolFieldDecl cfd6 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(2) VALUE 'ram'.");
		assertEquals(cfd6.getFieldType(),"tinyint");
		CobolFieldDecl cfd7 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(4) VALUE 'ram'.");
		assertEquals(cfd7.getFieldType(),"smallint");
		CobolFieldDecl cfd8 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(9) VALUE 'ram'.");
		assertEquals(cfd8.getFieldType(),"int");
		CobolFieldDecl cfd9 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(19) VALUE 'ram'.");
		assertEquals(cfd9.getFieldType(),"string");
	}
	
	public void testGetFieldLength(){
		System.out.println("testing GetFieldLength..");
		
		CobolFieldDecl cfd = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd.getFieldProperties().get("length"),(Integer) 0);
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getFieldProperties().get("length"),(Integer) 0);
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getFieldProperties().get("length"),(Integer) 10);
		
		try{
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10)V9(20).");
		assertEquals(cfd3.getFieldProperties().get("length"),(Integer) 0);
		}
		catch(RuntimeException e){
			System.out.println("Alphanumeric pic clause Error");
		}
		
		CobolFieldDecl cfd4 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10) VALUE 'ram'.");
		assertEquals(cfd4.getFieldProperties().get("length"),(Integer) 10);
		CobolFieldDecl cfd5 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10) VALUE 'ram'.");
		assertEquals(cfd5.getFieldProperties().get("length"),(Integer)10);
		CobolFieldDecl cfd6 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10)V9(03) VALUE 'ram'.");
		assertEquals(cfd6.getFieldProperties().get("length"),(Integer)13);
		
		CobolFieldDecl cfd7 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10)V99 VALUE 'ram'.");
		assertEquals(cfd7.getFieldProperties().get("length"),(Integer)12);
		
		CobolFieldDecl cfd8 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 99999 VALUE 'ram'.");
		assertEquals(cfd8.getFieldProperties().get("length"),(Integer)5);
	}
	
	

}
