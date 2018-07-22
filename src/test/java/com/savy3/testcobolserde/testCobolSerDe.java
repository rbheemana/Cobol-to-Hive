package com.savy3.testcobolserde;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerDe;

import junit.framework.TestCase;


public class testCobolSerDe extends TestCase {
	CobolSerDe csd = new CobolSerDe();
	Configuration conf = new Configuration();
	Properties tbl = new Properties();
	
	public void testInitializeConfigurationProperties() {
		fail("Not yet implemented");
	}

	public void testDeserializeWritable() throws SerDeException {
		tbl.setProperty("cobol.layout", "01 ws-var. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999.");
		csd.initialize(conf, tbl);
		System.out.println(csd.deserialize(new Text("002123123")).toString());
		
		tbl.setProperty("cobol.layout", "01 WS-VAR. 05 WS-NAME PIC X(12). 01 wS-pas. 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-marks OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 10 Ws-per pic 9. 05 WS-NICKNAME PIC X(6)");
		csd.initialize(conf, tbl);
		System.out.println(csd.deserialize(new Text("rammanohar  00212341235manu  \nsummanohar  00212341234manu  ")).toString());
		System.out.println(csd.deserialize(new Text("summanohar  00212341235manu  ")).toString());
		tbl.setProperty("cobol.layout", "01 WS-VAR. 02 WS-NAME PIC X(12). 02 W-Ram. 05 WS-MARKS-LENGTH PIC 9(3). 02 ws-kanu. 03 ws-lipi. 05 WS-marks PIC 9(3). 05 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");
		csd.initialize(conf, tbl);
		System.out.println(csd.deserialize(new Text("rammanohar  002123123manu  \nrummanohar  002123123manu  ")).toString());
		System.out.println(csd.deserialize(new Text("summanohar  002321123manu  ")).toString());
	
	
	}

}
