package com.savy3.hadoop.hive.serde3.cobol;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.commons.codec.binary.Hex;


import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeException;

public class CobolField implements HiveColumn{
	protected String name;
	protected String debugInfo;
	protected TypeInfo typeInfo;
	protected ObjectInspector oi;
	protected int compType = 0;;
	protected int decimalLocation = 0;
	protected int length = 0;
	protected CobolFieldType type;
	protected int levelNo;
	protected int offset;

	// constructor

	
	public CobolField(String debugInfo,int levelNo,String name, int length) {
        this.debugInfo = debugInfo;
		this.name = name;
//        this.typeInfo = typeInfo;
        this.length = length;
        this.levelNo = levelNo;
	}

	public ObjectInspector getOi() {
		return oi;
	}


	public CobolFieldType getType() {
		return type;
	}


	public void setOffset(int offset) {
		this.offset = offset;
	}


	public int getLevelNo() {
		return levelNo;
	}

	public String getDebugInfo() {
		return debugInfo;
	}


	public CobolField() {
	}

	public String toString() {
		return ("CobolField :[ Name : " + name + ", type : " + typeInfo
				+ ", offset :" + offset + " ]");
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public TypeInfo getTypeInfo() {
		return this.typeInfo;
	}

	@Override
	public int getOffset() {
		return this.offset;
	}

	@Override
	public int getLength() {
		return this.length;
	}


	public Object deserialize(byte[] rowBytes) throws CobolSerdeException{
		if (this.type == CobolFieldType.STRING)
			return ((CobolStringField)this).deserialize(rowBytes);
		if (this.type == CobolFieldType.NUMBER) {
			return ((CobolNumberField)this).deserialize(rowBytes);
		}
		return null;
		
	}

	//getBytes modified to accomodate VB multi-record formats instead of throwing an exception.  
	protected byte[] getBytes(byte[] rowBytes){
			if (offset + length < rowBytes.length) {
				return Arrays.copyOfRange(rowBytes, offset, offset + length);
			}
			else if ((offset + length == rowBytes.length) && (offset < rowBytes.length))  {
				return Arrays.copyOfRange(rowBytes, offset, rowBytes.length);
			}
			else if ((offset + length > rowBytes.length) && (offset < rowBytes.length))  {
				
				byte [] originalBytes = Arrays.copyOfRange(rowBytes, offset, rowBytes.length);
				
				String zeroesStr = new String(new char[length-(rowBytes.length-offset)]).replace("\0", "0");
				byte[] zeroes =  zeroesStr.getBytes(Charset.forName("ebcdic-cp-us"));


				byte[] paddedBytes = new byte[originalBytes.length + zeroes.length];
				System.arraycopy(originalBytes, 0, paddedBytes, 0, originalBytes.length);
				System.arraycopy(zeroes, 0, paddedBytes, originalBytes.length, zeroes.length);
				
				return paddedBytes;
			}
			else {
				String zeroesStr = new String(new char[length]).replace("\0", "0");
				byte[] zeroes =  zeroesStr.getBytes(Charset.forName("ebcdic-cp-us"));
				return zeroes;
			}
	}

	public byte[] transcodeField(byte[] source) {
		  byte[] result = new String(source, Charset.forName("ebcdic-cp-us")).getBytes(Charset.forName("ascii"));
		  if (result.length != source.length) {
		    throw new AssertionError("EBCDIC TO ASCII conversion for column:"+this.name+"failed"+result.length + "!=" + source.length);
		  }
		  return result;
	}

	public int getSize() {
		// TODO Auto-generated method stub
		return this.length;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + levelNo;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CobolField other = (CobolField) obj;
		if (levelNo != other.levelNo)
			return false;
		return true;
	}

	public String getCobolHiveMapping() {
		return this.getDebugInfo().trim()+"|"+this.name+"\t"+this.typeInfo.getTypeName()+"\t|"+this.offset+"|"+this.length+"|";
	}
}