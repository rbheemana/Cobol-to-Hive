package com.savy3.cobolserde;



import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;




/**
 * CobolSerde is under construction currently implemented are
 * 	1. basic alphanumeric
 * 	2. basic numeric
 * @author Ram Manohar <ram.manohar2708@gmail.com>
 */
public final class CobolSerde extends AbstractSerDe {
  
  private ObjectInspector inspector;
//  private String[] outputFields;
  private int numCols;
  private ArrayList<Object> row;
  private List<TypeInfo> columnTypes;
  private List<List<Map<String, Integer>>> columnProperties;
  
  private CobolDeserializer cobolDeserializer = null;
  private CobolSerializer cobolSerializer = null;
  private CobolCopybook ccb;
  
    
  @Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
//	 System.out.println(tbl.getProperty(Constants.LIST_COLUMN_TYPES).toString());
	  
	this.ccb = new CobolCopybook(tbl.getProperty("cobol.layout"));
	numCols = ccb.getFieldNames().size();
	//System.out.println(""+numCols);
	this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(ccb.getFieldNames(), ccb.getFieldOIs());
	this.columnTypes = ccb.getFieldTypeInfos();
	this.columnProperties = ccb.getFieldProperties();
	//System.out.println(ccb.getFieldTypes().toString());
	row = new ArrayList<Object>(numCols);
	for (int i=0; i< numCols; i++) 
	  row.add(null);
  }
 
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {  
	  return new Text("Ram");
  }  

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
	  return getDeserializer().deserialize(this.columnTypes,this.columnProperties,this.numCols, blob);
  }
     
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }
  
  public SerDeStats getSerDeStats() {
    return null;
  }
  private CobolDeserializer getDeserializer() {
	  if(cobolDeserializer == null) {
		  cobolDeserializer = new CobolDeserializer();
	  }
	  return cobolDeserializer;
  }
  
  private CobolSerializer getSerializer() {
	  if(cobolSerializer == null) {
		  cobolSerializer = new CobolSerializer();
	  }
	  return cobolSerializer;
  }
}

