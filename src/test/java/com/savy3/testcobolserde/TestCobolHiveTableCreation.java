package com.savy3.testcobolserde;

import com.github.sakserv.minicluster.impl.HiveLocalMetaStore;
import com.github.sakserv.minicluster.impl.HiveLocalServer2;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.savy3.hadoop.hive.serde2.cobol.CobolSerDe;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.sql.*;
import java.util.Properties;
//import org.apache.hive.jdbc.HiveDriver;


public class TestCobolHiveTableCreation extends TestCase {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    CobolSerDe csd = new CobolSerDe();
    Configuration conf = new Configuration();
    Properties tbl = new Properties();
    HiveConf hiveConf = null;
    ZookeeperLocalCluster zookeeperLocalCluster = null;
    HiveLocalMetaStore hiveLocalMetaStore = null;
    HiveLocalServer2 hiveLocalServer2 = null;
    String copyBook3 = "01 :NTTFIGRQ:-FIGN-RESP-AREA.                             \n" +
            "   15 :NTTFIGRQ:-FEE-SECTION.                             \n" +
            "     20 :NTTFIGRQ:-FEE-AND-CHARGES  OCCURS 5 TIMES.       \n" +
            "        30 :NTTFIGRQ:-BILL-EVENT-TY-CD PIC X(04).         \n" +
            "        30 :NTTFIGRQ:-BILL-RQST-CD     PIC X(01).         \n" +
            "        30 :NTTFIGRQ:-FEE-EXEMPT-ID     PIC X(01).        \n" +
            "        30 :NTTFIGRQ:-FEE-EXEMPT-RSN-CD PIC X(04).        \n" +
            "        30 :NTTFIGRQ:-RULE-FEE-CD PIC X(04).                    \n" +
            "        30 :NTTFIGRQ:-FEE-CALC-RCL-CD   PIC X(03).              \n" +
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
    String issue19Layout = "01 DOC2.\n" +
            "20 DOC2-V-ID PIC X(17).\n" +
            "20 DOC2-N-DATA PIC S9(5) COMP-3.\n" +
            "20 DOC2-VAL-DATA PIC X(100).";
    String issue39Layout = "01 DETAILS.\n" +
            "02 RATE PIC V9(6).";
    String issue45Layout = "01 DETAILS.\n" +
            "02 PROM-FACTOR PIC S99V999 COMP3.";
    String issue41Layout = "01  OCHT-ACCUR-DTL-LVL.\n" +
            "               15  OCHT-ACCUR-DET OCCURS 18 TIMES.\n" +
            "                   20  BILLED-AMT     PIC S9(5)V99\n" +
            "                       SIGN IS LEADING, SEPARATE.\n" +
            "                   20  ROC-COD        PIC X(7).\n" +
            "                   20  OD1            PIC X(2).\n" +
            "                   20  OD2            PIC X(2).\n" +
            "                   20  OS             PIC X(2).\n" +
            "                   20  OS-CATEG-CE PIC X(2).\n" +
            "                   20  TOSP           PIC X(3).\n" +
            "                   20  OUNT           PIC 9(3).\n" +
            "                   20  ALIFIER-I      PIC 9(9).\n" +
            "                   20  XT-RUL         PIC X(5).\n" +
            "                   20  SRVC-FROM-DATE.\n" +
            "                       25  SRVC-FR-DT-CCYY  PIC X(4).\n" +
            "                       25  SRVC-FR-DT-MMDD  PIC X(4).\n" +
            "                   20  SRVC-TO-DATE.\n" +
            "                       25  SRVC-TO-DT-CCYY  PIC X(4).\n" +
            "                       25  SRVC-TO-DT-MMDD  PIC X(4)";
    String issue41LayoutFull =
            "01 OCHT-ACCUR-REC-IN.\n" +
                    "  05 OCHNT-ACCUR-INPUT.\n" +
                    "    10 OCHT-ACCUR-EMP.\n" +
                    "       15  EMP-ID                   PIC X(11).\n" +
                    "       15  EMP-TYPE-CO              PIC X(2).\n" +
                    "       15  CEIVED-DA                PIC 9(8).\n" +
                    "       15  ODUCT-CO                 PIC X(1).\n" +
                    "       15  PARMNT-TY                PIC X(3).\n" +
                    "       15  EMP-OFFICE-K             PIC 9(3).\n" +
                    "       15  BSBR-ST-CO               PIC X(2).\n" +
                    "       15  DICARE-PART-CO           PIC X.\n" +
                    "       15  EAN-UNCLEAN-CO           PIC X.\n" +
                    "       15  NTROL-N                  PIC 9(7).\n" +
                    "       15  TO-ADJ-I                 PIC X.\n" +
                    "       15  AR-I                     PIC X(2).\n" +
                    "       15  TAL-ILL-A                PIC 9(5)V99.\n" +
                    "       15  OTAL-AI-A                PIC 9(5)V99.\n" +
                    "       15  TDENT-RELATSH            PIC X.\n" +
                    "       15  OB-MEOD-CO               PIC X(3).\n" +
                    "       15  EV-PD-AMN                PIC S9(5)V99\n" +
                    "           SIGN IS LEADING, SEPARATE.\n" +
                    "       15  URCE-CO                  PIC X(2).\n" +
                    "       15  NDIN                     PIC X(2).\n" +
                    "       15  OB-TP                    PIC X.\n" +
                    "       15  OB-ADTM                  PIC S9(5)V99\n" +
                    "           SIGN IS LEADING, SEPARATE.\n" +
                    "       15  TTER-CO                  PIC X.\n" +
                    "       15  TTER-I                   PIC X(2).\n" +
                    "       15  ODUCT-TY                 PIC X(2).\n" +
                    "       15  UAL-REEW-                PIC X.\n" +
                    "       15  NTRACT-STA               PIC X(2).\n" +
                    "       15  OV-ST-CO                 PIC X(2).\n" +
                    "       15  AY                       PIC X.\n" +
                    "       15  IN                       PIC X(9).\n" +
                    "       15  UM-DTL-LIN               PIC 9(2).\n" +
                    "       15  BSCRIBER-I               PIC X(10).\n" +
                    "       15  VERAGE-CO                PIC X(2).\n" +
                    "       15  TWORK                    PIC X(5).\n" +
                    "       15  EMO-TEX                  PIC X(70).\n" +
                    "       15  SCHRGE-STATUS-C          PIC X(2).\n" +
                    "       15  IRST-EMP-I               PIC X(1).\n" +
                    "       15  UB-VIEW-IN               PIC X(1).\n" +
                    "       15  OB-CO                    PIC X(1).\n" +
                    "       15  OB-INFO-I                PIC X(1).\n" +
                    "       15  IMB-MTHD-C               PIC X(2).\n" +
                    "       15  EE-SCH-I                 PIC X(9).\n" +
                    "       15  MISSION-TY               PIC X(1).\n" +
                    "       15  EMP-DELTION-I            PIC X(2).\n" +
                    "       15  XP-VNDR-I                PIC X(6).\n" +
                    "       15  ONOMY-CD                 PIC X(10).\n" +
                    "       15  LL-PROV-N                PIC 9(10).\n" +
                    "       15  ROV-IPA-N                PIC 9(9).\n" +
                    "       15  ROV-PBG                  PIC 9(9).\n" +
                    "       15  ROV-PIN                  PIC X(12).\n" +
                    "       15  ERV-POV-NP               PIC 9(10).\n" +
                    "       15  NIQ-PHY-I                PIC X(6).\n" +
                    "       15  NW-CAORY-C               PIC X(8).\n" +
                    "       15  ROF-COMP-FAC-ST-C        PIC X(2).\n" +
                    "       15  RV-PROV-NW-TY            PIC X(3).\n" +
                    "       15  ERV-PROV-PRACT-RO        PIC X(4).\n" +
                    "       15  AP-NW-I                  PIC X(5).\n" +
                    "       15  RG-WEIG                  PIC S9(06)V9(04)\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "       15  UB-DRG-CE                PIC X(5).\n" +
                    "       15  RG-RTN-CE                PIC X(2).\n" +
                    "       15  RG-RTN-DG-C              PIC X(6).\n" +
                    "       15  RG-RTN-DAG-RSN-C         PIC X(2).\n" +
                    "       15  RG-RTN-POC-C             PIC X(7).\n" +
                    "       15  RG-RTN-POC-RSN-CD        PIC X(2).\n" +
                    "       15  CESS-INT-DA   OCCURS 3 TIMES.\n" +
                    "           20 CESS-INT-DAYS-C       PIC S9(4)\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "       15  CAL-TRANSBLANT-IN        PIC X(1).\n" +
                    "       15  ISA-IND                  PIC X(1).\n" +
                    "       15  CI-DATE                  PIC X(10).\n" +
                    "       15  OSP-ADMSION-DAT          PIC X(10).\n" +
                    "       15  EMP-CATORY-C             PIC X(2).\n" +
                    "       15  FUD-RVW-IN               PIC X(1).\n" +
                    "       15  ULANCE-ZIP-C             PIC X(5).\n" +
                    "       15  N-PROD-I                 PIC X(11).\n" +
                    "       15  M-COV-STS-C              PIC S9(4)\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "       15  BLAN-ID-I                PIC X(6).\n" +
                    "       15  BLAN-EF-DA               PIC X(10).\n" +
                    "       15  BLAN-STOP-DA             PIC X(10).\n" +
                    "       15  BLAN-TE-CAL-OR-BE        PIC X(1).\n" +
                    "       15  BLAN-ACCO                PIC 9(5).\n" +
                    "       15  BLAN-SUF                 PIC 9(3).\n" +
                    "       15  BLAN-CST-SUB             PIC X(6).\n" +
                    "       15  EMP-USER-FI              PIC X(10).\n" +
                    "       15  EMP-USER-FIE             PIC X(10).\n" +
                    "       15  EMP-USER-FIEL            PIC X(10).\n" +
                    "       15  SCHGE-ST-TY              PIC X(3).\n" +
                    "       15  SER-ACPT-I               PIC X(10).\n" +
                    "       15  ER-RESP-ID               PIC X(10).\n" +
                    "       15  ER-QLTY-REV              PIC X(10).\n" +
                    "       15  ER-PYRLSE-I              PIC X(10).\n" +
                    "       15  EAUTH-DAT                PIC X(10).\n" +
                    "       15  FERRAL-I                 PIC X(9).\n" +
                    "       15  OV-REF-RSN-C             PIC X(2).\n" +
                    "       15  M-SRVC-PREAUTH-DA        PIC S9(3)\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "       15  EF-EXTS-IN               PIC X(1).\n" +
                    "       15  EF-PROV-N                PIC X(12).\n" +
                    "       15  EMP-REF-N                PIC X(19).\n" +
                    "       15  UMB-ID-SEQ-NUM           PIC X(06).\n" +
                    "       15  FILL                     PIC X(223).\n" +
                    "    10 OCHT-ACCUR-DTL-LVL.\n" +
                    "       15  OCHT-ACCUR-DET OCCURS 18 TIMES.\n" +
                    "           20  BILLED-AMT     PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  ROC-COD        PIC X(7).\n" +
                    "           20  OD1            PIC X(2).\n" +
                    "           20  OD2            PIC X(2).\n" +
                    "           20  OS             PIC X(2).\n" +
                    "           20  OS-CATEG-CE PIC X(2).\n" +
                    "           20  TOSP           PIC X(3).\n" +
                    "           20  OUNT           PIC 9(3).\n" +
                    "           20  ALIFIER-I      PIC 9(9).\n" +
                    "           20  XT-RUL         PIC X(5).\n" +
                    "           20  SRVC-FROM-DATE.\n" +
                    "               25  SRVC-FR-DT-CCYY  PIC X(4).\n" +
                    "               25  SRVC-FR-DT-MMDD  PIC X(4).\n" +
                    "           20  SRVC-TO-DATE.\n" +
                    "               25  SRVC-TO-DT-CCYY  PIC X(4).\n" +
                    "               25  SRVC-TO-DT-MMDD  PIC X(4).\n" +
                    "           20  INE-ITE        PIC 9(9).\n" +
                    "           20  EV-COD         PIC X(4).\n" +
                    "           20  RE-CERT-I      PIC X(9).\n" +
                    "           20  MT-ALLO        PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  MT-SU          PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  MT-COI         PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  MT-COP         PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  MT-DE          PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  AY-MTH         PIC X(6).\n" +
                    "           20  LTH-QAL-C      PIC X(3).\n" +
                    "           20  C-RATE-AM      PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  C-RATE-RS-C    PIC X(1).\n" +
                    "           20  RIING-CA       PIC X(3).\n" +
                    "           20  AP-RATE-AT     PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  DCR-ASSN-C     PIC X(1).\n" +
                    "           20  PC-C           PIC X(6).\n" +
                    "           20  PC-ATUS-C      PIC X(1).\n" +
                    "           20  CE-PRF-C       PIC X(1).\n" +
                    "           20  P-ELIG-CD      PIC X(1).\n" +
                    "           20  ANCE-CM-ID     PIC X(1).\n" +
                    "           20  ING-SS-CD      PIC X(6).\n" +
                    "           20  FS-SEQ-NO      PIC 9(4).\n" +
                    "           20  J-NDC-UT       PIC S9(7)V9(3)\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  DC-DG-C        PIC X(14).\n" +
                    "           20  FR-DIS-AW-AT   PIC S9(5)V99\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  R-EX-CD        PIC X(3).\n" +
                    "           20  R-FLAG-CD      PIC X(8).\n" +
                    "           20  ALT-PAY-PT     PIC S9(03)V999\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  ONT-PCT        PIC S9(03)V999\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  NOT-COVED-AC OCCURS 30 TIMES.\n" +
                    "              25 NC-ACT-C     PIC X(3).\n" +
                    "           20  EF-NO          PIC X(9).\n" +
                    "           20  EF-AUTH-LO     PIC S9(3)\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  NIT-COUN       PIC S9(3)\n" +
                    "               SIGN IS LEADING, SEPARATE.\n" +
                    "           20  FILE           PIC X(64).";
    String getIssue39Layout1 = "01  D2WCLIE-DETALHE.\n" +
            "            02 NOME                               PIC X(10).\n" +
            "            02 SOBRENOME                          PIC X(11).\n" +
            "            02 IDADE                              PIC 9(3).\n" +
            "            02 SALDO                              PIC S9(11)V99.\n" +
            "            02 SALDO-COMP                         PIC S9(11)V99\n" +
            "                                                      USAGE COMP-3.";

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
//        hiveLocalServer2.stop(true);
//        hiveLocalMetaStore.stop(true);
//        zookeeperLocalCluster.stop();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LogManager.getRootLogger().setLevel(Level.ERROR);
        hiveConf = new HiveConf();
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, false);
        String cwd = System.getProperty("user.dir");
        System.out.println("Current working directory : " + cwd);
        hiveConf.setAuxJars("target/Cobol-to-Hive-1.1.0.jar");
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(12345)
                .setTempDir("embedded_zookeeper")
                .setZookeeperConnectionString("localhost:12345")
                .setMaxClientCnxns(60)
                .setElectionPort(20001)
                .setQuorumPort(20002)
                .setDeleteDataDirectoryOnClose(false)
                .setServerId(1)
                .setTickTime(2000)
                .build();

        zookeeperLocalCluster.start();
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname("localhost")
                .setHiveMetastorePort(12347)
                .setHiveMetastoreDerbyDbDir("metastore_db")
                .setHiveScratchDir("hive_scratch_dir")
                .setHiveWarehouseDir("warehouse_dir")
                .setHiveConf(hiveConf)
                .build();

        hiveLocalMetaStore.start();
        hiveLocalServer2 = new HiveLocalServer2.Builder()
                .setHiveServer2Hostname("localhost")
                .setHiveServer2Port(12348)
                .setHiveMetastoreHostname("localhost")
                .setHiveMetastorePort(12347)
                .setHiveMetastoreDerbyDbDir("metastore_db")
                .setHiveScratchDir("hive_scratch_dir")
                .setHiveWarehouseDir("warehouse_dir")
                .setHiveConf(hiveConf)
                .setZookeeperConnectionString("localhost:12345")
                .build();

        hiveLocalServer2.start();
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
    }


    public void testHiveCreateTable() throws Exception {

//
        //replace "hive" here with the name of the user the queries should run as
        String username = System.getProperty("user.name");

        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:12348/default", username, "");
        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " TBLPROPERTIES ('cobol.layout.literal'='01 WS-VAR. 05 WS-NAME PIC X(12). 05 WS-SUB PIC N(10). 05 WS-SUB PIC 9(10). ','fb.length'='22')");
        // show tables
        // String sql = "show tables '" + tableName + "'";
        String sql = ("describe " + tableName);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("Printing metadata");
        printResultSet(res);

        String tableName2 = "CobolArq1";
        stmt.execute("drop table if exists " + tableName2);
        stmt.execute("create table " + tableName2 +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " TBLPROPERTIES ('cobol.layout.literal'='01 D2WCLI. 05 NOME PIC X(10). 05 SOBERNOME PIC X(11). 05 IDADE PIC 9(3). 05 AMOUNT PIC S9(11)V99. 05 AMOUNT-COMP PIC S9(11)V99 COMP-3. ','fb.length'='44')");

        String sql2 = ("describe " + tableName2);
        ResultSet res2 = stmt.executeQuery(sql2);
        System.out.println("Printing metadata");
        printResultSet(res2);

        String tableName3 = "Issue51";
        stmt.execute("drop table if exists " + tableName3);
        stmt.execute("create table " + tableName3 +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " TBLPROPERTIES ('cobol.layout.literal'='" + copyBook3 + "','fb.length'='44')");

        String sql3 = ("describe " + tableName3);
        ResultSet res3 = stmt.executeQuery(sql3);
        System.out.println("Printing metadata");
        printResultSet(res3);

        String tableName4 = "Issue19";
        stmt.execute("drop table if exists " + tableName4);
        stmt.execute("create table " + tableName4 +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " TBLPROPERTIES ('cobol.layout.literal'='" + issue19Layout + "','fb.length'='44')");

        String sql4 = ("describe " + tableName4);
        ResultSet res4 = stmt.executeQuery(sql4);
        System.out.println("Printing metadata");
        printResultSet(res4);

        String tableName5 = "Issue39";
        stmt.execute("drop table if exists " + tableName5);
        stmt.execute("create table " + tableName5 +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " TBLPROPERTIES ('cobol.layout.literal'='" + issue39Layout + "','fb.length'='44')");

        String sql5 = ("describe " + tableName5);
        ResultSet res5 = stmt.executeQuery(sql5);
        System.out.println("Printing metadata");
        printResultSet(res5);

        String tableName6 = "Issue45";
        stmt.execute("drop table if exists " + tableName6);
        stmt.execute("create table " + tableName6 +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " TBLPROPERTIES ('cobol.layout.literal'='" + issue45Layout + "','fb.length'='44')");

        String sql6 = ("describe " + tableName6);
        ResultSet res6 = stmt.executeQuery(sql6);
        System.out.println("Printing metadata");
        printResultSet(res6);
        String tableName7 = "Issue41";
        stmt.execute("drop table if exists " + tableName7);
        stmt.execute("create table " + tableName7 +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " TBLPROPERTIES ('cobol.layout.literal'='" + issue41Layout + "','fb.length'='44')");

        String sql7 = ("describe " + tableName7);
        ResultSet res7 = stmt.executeQuery(sql7);
        System.out.println("Printing metadata");
        printResultSet(res7);

        String tableName8 = "issue39_arq";
        String fileSeperator = System.getProperty("file.separator");
        String table8Location = System.getProperty("user.dir") + fileSeperator + "resources" + fileSeperator + "test_data" + fileSeperator + "issue39_arq";
        stmt.execute("drop table if exists " + tableName8);
        stmt.execute("create external table " + tableName8 +
                " ROW FORMAT SERDE 'com.savy3.hadoop.hive.serde3.cobol.CobolSerDe'" +
                " STORED AS " +
                " INPUTFORMAT 'org.apache.hadoop.mapred.FixedLengthInputFormat'" +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
                " LOCATION '" + table8Location + "'" +
                " TBLPROPERTIES ('cobol.layout.literal'='" + getIssue39Layout1 + "','fb.length'='44','cobol.field.ignorePattern'='nome')" +
                "");

        String sql8 = ("describe " + tableName8);
        ResultSet res8 = stmt.executeQuery(sql8);
        System.out.println("Printing metadata");
        printResultSet(res8);
        String sql9 = ("select * from " + tableName8);
        ResultSet res9 = stmt.executeQuery(sql9);
        System.out.println("Printing results");
        printResultSet(res9);
    }

    public void printResultSet(ResultSet resultSet) {
        ResultSetMetaData rsmd = null;
        try {
            rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print("#");
                    String columnValue = resultSet.getString(i);
                    System.out.print("|" + rsmd.getColumnName(i) + "|" + columnValue + "|");
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
