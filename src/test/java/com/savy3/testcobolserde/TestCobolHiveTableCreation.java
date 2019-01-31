package com.savy3.testcobolserde;

import com.github.sakserv.minicluster.impl.HiveLocalMetaStore;
import com.github.sakserv.minicluster.impl.HiveLocalServer2;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.savy3.hadoop.hive.serde2.cobol.CobolSerDe;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

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

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        hiveLocalServer2.stop(true);
        hiveLocalMetaStore.stop(true);
        zookeeperLocalCluster.stop();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
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
