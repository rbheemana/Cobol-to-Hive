package com.savy3.testcobolserde;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerDe;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
//import org.apache.hive.jdbc.HiveDriver;


public class TestCobolSerDe extends TestCase {

	Configuration conf = new Configuration();


	public static BytesWritable ConvertStringToEBCIDICBytes(String src) {
		Charset from = Charset.forName("ascii");
		Charset to = Charset.forName("ebcdic-cp-us");
		byte[] source = src.getBytes();
		byte[] result = new String(source, from).getBytes(to);
//		  System.out.println("source length"+source.length+"result length"+result.length);
		if (result.length != source.length) {
			throw new AssertionError("ASCII TO EBCIDIC conversion failed" + result.length + "!=" + source.length);
		}
		return new BytesWritable(result);
	}

	public void testDeserializeWritable() throws SerDeException {
		CobolSerDe csd = new CobolSerDe();
		Properties tbl = new Properties();
		tbl.setProperty("cobol.layout.literal", "01 ws-var. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999.");
		csd.initialize(conf, tbl);
		List<Object> res1 = new ArrayList<Object>();
		res1.add(2);
		res1.add(123);
		res1.add(123);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);
		res1.add(null);

		assertEquals(res1.toString(), csd.deserialize(ConvertStringToEBCIDICBytes("002123123")).toString());

		CobolSerDe csd2 = new CobolSerDe();
		Properties tbl2 = new Properties();
		tbl2.setProperty("cobol.layout.literal", "01 WS-VAR. 05 WS-NAME PIC X(12). 01 wS-pas. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 10 Ws-per pic 9. 05 WS-NICKNAME PIC X(6)");
		csd2.initialize(conf, tbl2);
		List<Object> result2 = new ArrayList<Object>();
		result2.add("rammanohar");
		result2.add(2);
		result2.add(123);
		result2.add(4);
		result2.add(123);
		result2.add(5);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add(null);
		result2.add("manu");

		List<Object> result3 = new ArrayList<Object>();
		result3.add("summanohar");
		result3.add(2);
		result3.add(123);
		result3.add(4);
		result3.add(123);
		result3.add(5);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add(null);
		result3.add("manu");
		assertEquals(result2.toString(), csd2.deserialize(ConvertStringToEBCIDICBytes("rammanohar  00212341235manu  \nsummanohar  00212341234manu  ")).toString());
		assertEquals(result3.toString(), csd2.deserialize(ConvertStringToEBCIDICBytes("summanohar  00212341235manu  ")).toString());


		CobolSerDe csd3 = new CobolSerDe();
		Properties tbl3 = new Properties();
		tbl3.setProperty("cobol.layout.literal", "01 WS-VAR. 02 WS-NAME PIC X(12). 02 W-Ram. 05 WS-MARKS-LENGTH PIC 9(3). 02 ws-kanu. 03 ws-lipi. 05 WS-marks PIC 9(3). 05 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");
		csd3.initialize(conf, tbl3);
		List<Object> result4 = new ArrayList<Object>();
		result4.add("rammanohar");
		result4.add(2);
		result4.add(123);
		result4.add(123);
		result4.add("manu");
		List<Object> result5 = new ArrayList<Object>();
		result5.add("summanohar");
		result5.add(2);
		result5.add(321);
		result5.add(123);
		result5.add("manu");
		assertEquals(result4.toString(), csd3.deserialize(ConvertStringToEBCIDICBytes("rammanohar  002123123manu  \nrummanohar  002123123manu  ")).toString());
		assertEquals(result5.toString(), csd3.deserialize(ConvertStringToEBCIDICBytes("summanohar  002321123manu  ")).toString());


	}

}
