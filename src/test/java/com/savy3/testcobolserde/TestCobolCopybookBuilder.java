package com.savy3.testcobolserde;

import com.savy3.hadoop.hive.serde3.cobol.CobolCopybookBuilder;
import com.savy3.hadoop.hive.serde3.cobol.CobolSerdeException;
import com.savy3.hadoop.hive.serde3.cobol.CobolToHive;
import junit.framework.TestCase;

public class TestCobolCopybookBuilder extends TestCase {

    String copyBook1 = "01 WS-VAR. 05 WS-NAME PIC X(12). ";
    String copyBook2 = "01 ws-sudha. 05 WS-MARKS-LENGTH PIC 9(3). " +
            "05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. " +
            "10 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)";

    String copyBook3 = "01 :NTTFIGRQ:-FIGN-RESP-AREA.                             \n" +
            "   15 :NTTFIGRQ:-FEE-SECTION.                             \n" +
            "     20 :NTTFIGRQ:-FEE-AND-CHARGES  OCCURS 5 TIMES.       \n" +
            "        30 :NTTFIGRQ:-BILL-EVENT-TY-CD PIC X(04).         \n" +
            "        30 :NTTFIGRQ:-BILL-RQST-CD     PIC X(01).         \n" +
            "           88 :NTTFIGRQ:-EARMARK             VALUE '1'.   \n" +
            "           88 :NTTFIGRQ:-EARMARK-APPROVAL    VALUE '2'.   \n" +
            "           88 :NTTFIGRQ:-CANCEL              VALUE '3'.   \n" +
            "           88 :NTTFIGRQ:-EARMARK-CANCEL      VALUE '4'.   \n" +
            "           88 :NTTFIGRQ:-EARMARK-WAKEUP      VALUE '5'.   \n" +
            "           88 :NTTFIGRQ:-STRAIGHT-APPROVAL   VALUE '6'.   \n" +
            "        30 :NTTFIGRQ:-FEE-EXEMPT-ID     PIC X(01).        \n" +
            "           88 :NTTFIGRQ:-EXEMPT              VALUE 'Y'.   \n" +
            "           88 :NTTFIGRQ:-NOT-EXEMPT          VALUE 'N'.   \n" +
            "        30 :NTTFIGRQ:-FEE-EXEMPT-RSN-CD PIC X(04).        \n" +
            "           88 :NTTFIGRQ:-BUNDLE-EXEMPT       VALUE 'BDEX'.\n" +
            "           88 :NTTFIGRQ:-NETX-BUNDLE         VALUE 'NXBD'.\n" +
            "           88 :NTTFIGRQ:-REG-EXEMPT          VALUE 'XMPT'.\n" +
            "           88 :NTTFIGRQ:-EXCEPT              VALUE 'XCPT'.\n" +
            "        30 :NTTFIGRQ:-RULE-FEE-CD PIC X(04).                    \n" +
            "           88 :NTTFIGRQ:-PERSHING-FEE-XMPT   VALUE 'FIRM'.      \n" +
            "           88 :NTTFIGRQ:-IBD-MKUP-XMPT       VALUE 'MKUP'.      \n" +
            "           88 :NTTFIGRQ:-TOTAL-FEE-XMPT      VALUE 'BOTH'.      \n" +
            "        30 :NTTFIGRQ:-FEE-CALC-RCL-CD   PIC X(03).              \n" +
            "           88 :NTTFIGRQ:-PERS-DFLT           VALUE 'FI'.        \n" +
            "           88 :NTTFIGRQ:-IBD-RULE            VALUE 'SC'.        \n" +
            "           88 :NTTFIGRQ:-OFC-RULE            VALUE 'OF'.        \n" +
            "           88 :NTTFIGRQ:-IP-ALL-RULE         VALUE 'RS'.        \n" +
            "           88 :NTTFIGRQ:-IP-RULE             VALUE 'RO'.        \n" +
            "           88 :NTTFIGRQ:-ACT-RULE            VALUE 'AC'.        \n" +
            "           88 :NTTFIGRQ:-FFAMILY-RULE        VALUE 'FF'.        \n" +
            "           88 :NTTFIGRQ:-CUSIP-RULE          VALUE 'CU'.        \n" +
            "           88 :NTTFIGRQ:-RCDKPR-RULE         VALUE 'RK'.        \n" +
            "        30 :NTTFIGRQ:-TOTAL-FEE         PIC S9(15)V99 COMP-3.   \n" +
            "        30 :NTTFIGRQ:-TOTAL-INT           REDEFINES             \n" +
            "           :NTTFIGRQ:-TOTAL-FEE         PIC S9(12)V9(05) COMP-3.\n" +
            "        30 :NTTFIGRQ:-PERS-FEE          PIC S9(15)V99 COMP-3.   \n" +
            "        30 :NTTFIGRQ:-FUND-BASED-ACCT-CHG REDEFINES             \n" +
            "           :NTTFIGRQ:-PERS-FEE          PIC S9(15)V99 COMP-3.   \n" +
            "        30 :NTTFIGRQ:-IBD-BASED-ACCT-CHG  REDEFINES             \n" +
            "           :NTTFIGRQ:-PERS-FEE          PIC S9(15)V99 COMP-3.   \n" +
            "        30 :NTTFIGRQ:-PERS-INT           REDEFINES              \n" +
            "           :NTTFIGRQ:-PERS-FEE          PIC S9(12)V9(05) COMP-3.\n" +
            "        30 :NTTFIGRQ:-IBD-MARK-UP-FEE   PIC S9(15)V99 COMP-3.   \n" +
            "        30 :NTTFIGRQ:-IBD-MARK-UP-INT      REDEFINES            \n" +
            "           :NTTFIGRQ:-IBD-MARK-UP-FEE   PIC S9(12)V9(05) COMP-3.\n" +
            "        30 :NTTFIGRQ:-CONTRACT-SCHD-ID  PIC X(12).              \n" +
            "        30 :NTTFIGRQ:-EBS-REQ-ENT-ID    PIC X(12).              \n" +
            "        30 :NTTFIGRQ:-BILL-RULE-SET-ID  PIC X(12).              \n" +
            "        30 :NTTFIGRQ:-TRAN-BUNDLE-ID    PIC X(12).              \n" +
            "        30 :NTTFIGRQ:-FXCPT-RULE-SET-ID REDEFINES               \n" +
            "           :NTTFIGRQ:-TRAN-BUNDLE-ID PIC X(12).                 \n" +
            "        30 :NTTFIGRQ:-BUNDLE-IND        PIC X(01).";

    private CobolToHive ccb;

    public void testCopybookBuilder() {
        try {
            CobolCopybookBuilder ccbb = new CobolCopybookBuilder();
            ccb = new CobolToHive(ccbb.getCobolCopybook(copyBook3));
            System.out.println(ccb.getHiveNames());
        } catch (CobolSerdeException e) {
            e.printStackTrace();
        }
    }
//	public void testGetFieldNames() {
//		assertEquals("ws_name", ccb.getFieldNames().get(0));
//		ccb = new CobolCopybook("01 WS-NAME PIC X(12). 01 ws-sudha. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");
//		assertEquals("ws_marks_length", ccb.getFieldNames().get(1));
//		assertEquals("ws_mark", ccb.getFieldNames().get(2));
//		assertEquals("ws_mark_1", ccb.getFieldNames().get(3));
//
//	}
//
//	public void testGetFiledTypes() {
//		assertEquals("varchar(12)", ccb.getFieldTypes().get(0));
//		assertEquals("smallint",
//				ccb.getFieldTypes().get(1));
//
//	}
//	public void testGetFiledProperties() {
//		assertEquals("{length=12, id=1, depend.id=0, prev.col=0, occurance=0}",
//				ccb.getFieldProperties().get(0).toString());
//		assertEquals("{length=3, id=2, depend.id=0, prev.col=1, occurance=0}",
//				ccb.getFieldProperties().get(1).toString());
//
//	}
//	public void testIssue44(){
//		for (int i = 0; i < ccbIssue44.getFieldNames().size(); i++){
//			System.out.println("FieldName: "+ccbIssue44.getFieldNames().get(i)+" Property: "+ccbIssue44.getFieldProperties().get(i));
//		}
//
//	}

}
