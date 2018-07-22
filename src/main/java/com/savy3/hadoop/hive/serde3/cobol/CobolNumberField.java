package com.savy3.hadoop.hive.serde3.cobol;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeException;

public class CobolNumberField extends CobolField {
	private int compType = 0;
	private int divideFactor = 0;
	private int decimalLocation = 0;

	// constructor
	public CobolNumberField(String debugInfo, int levelNo, String name,
			String picClause, int compType) throws CobolSerdeException {
		this.name = name;
		this.compType = compType;
		this.levelNo = levelNo;
		super.debugInfo = debugInfo;
		super.type = CobolFieldType.NUMBER;
		String fieldType = "integer";
		String[] s = picClause.replaceAll("v", "\\.").split("\\.");
		this.divideFactor = 1;
		try {
			if (this.compType == 3) {
				this.divideFactor = 2;
			}

			String[] ct = s[0].split("\\(|\\)");
			switch (ct.length) {
			case 1:
				this.length = ct[0].length();
				break;
			case 2:
				this.length = Integer.parseInt(ct[1]);
				break;
			}
			if (s.length == 2) {
				String[] mt = s[1].split("\\(|\\)");
				switch (mt.length) {
				case 1:
					this.decimalLocation = mt[0].length();
					this.length += this.decimalLocation;
					break;
				case 2:
					this.decimalLocation = Integer.parseInt(mt[1]);
					this.length += this.decimalLocation;
					break;
				}
			} else if(s.length >2) {
				throw new RuntimeException(
						"Alphanumeric Picture clause is not valid"
								+ this.debugInfo);
			}
			if (this.compType == 3) {
				this.length = (int) Math.ceil((double) (this.length +1) / divideFactor);
			}else{
				this.length = (int) Math.ceil((double) this.length / divideFactor);
			}
			if (decimalLocation ==0) {
				if (this.length * divideFactor < 3)
					fieldType = "tinyint";
				else if (this.length * divideFactor < 5)
					fieldType = "smallint";
				else if (this.length * divideFactor < 10)
					fieldType = "int";
				else if (this.length * divideFactor < 19)
					fieldType = "bigint";
				else
					fieldType = "string";

			} else {
				fieldType = "decimal(" + this.length * divideFactor + ","
						+ (this.decimalLocation) + ")";
			}
			if (this.compType == 4) {
				if(this.length < 5)
					this.length = 2;
				else if(this.length < 10)
					this.length = 4;
				else if(this.length < 19)
					this.length = 8;
				else
					throw new RuntimeException(
							"PIC-4 length is greater than 18"
									+ this.debugInfo);				
			}
		} catch (NumberFormatException e) {
			throw e;
		}
		try{
		this.typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldType);
		this.oi = TypeInfoUtils
				.getStandardJavaObjectInspectorFromTypeInfo(this.typeInfo);
		}
		catch(Exception e){
			throw new CobolSerdeException(e+this.debugInfo);
		}
	}

	@Override
	public String toString() {
		return "CobolNumberField [compType=" + compType + ", decimalLocation="
				+ decimalLocation + ", name=" + name + ", debugInfo="
				+ debugInfo + ", length=" + length + ", type=" + typeInfo
				+ ", levelNo=" + levelNo + ", offset=" + offset + "]";
	}

	@Override
	public Object deserialize(byte[] rowBytes) throws CobolSerdeException {
		byte[] temp = transcodeField(super.getBytes(rowBytes));
		String s1 = new String(temp);

		if (this.compType > 0) {
			if (this.compType == 3) {
				s1 = unpackData(super.getBytes(rowBytes), this.decimalLocation);
			}else if(this.compType == 4){
				s1 = getBinary(super.getBytes(rowBytes), this.decimalLocation);
			}
		} else if (this.decimalLocation > 0) {
			s1 = s1.substring(0, this.length * this.divideFactor
					- this.decimalLocation)
					+ "."
					+ s1.substring(this.length * this.divideFactor
							- this.decimalLocation);
		}
//		System.out.println(name + "\t - " + s1 + "\t:" + offset + "\t@"
//				+ length);
		try {
			switch (((PrimitiveTypeInfo) this.typeInfo).getPrimitiveCategory()) {
			case LONG:
				return Long.parseLong(s1.trim());
			case SHORT:
				return Short.parseShort(s1.trim());
			case INT:
				return Integer.parseInt(s1.trim());
			case BYTE:
				return Byte.parseByte(s1.trim());
			case FLOAT:
				return Float.parseFloat(s1.trim());
			case DOUBLE:
				return Double.parseDouble(s1.trim());
			case DECIMAL:
				BigDecimal bd = new BigDecimal(s1);
				HiveDecimal dec = HiveDecimal.create(bd);
				JavaHiveDecimalObjectInspector oi = (JavaHiveDecimalObjectInspector) PrimitiveObjectInspectorFactory
						.getPrimitiveJavaObjectInspector((DecimalTypeInfo) this.typeInfo);
				return oi.set(null, dec);
			}
		} catch (Exception e) {
			return null; // if cannot be converted make it null
		}
		return null;

	}

	public String unpackData(byte[] packedData, int decimalPointLocation) {
		String unpackedData = "";
		final int negativeSign = 13;
		for (int currentByteIndex = 0; currentByteIndex < packedData.length; currentByteIndex++) {

			int firstDigit = ((packedData[currentByteIndex] >> 4) & 0x0f);
			int secondDigit = (packedData[currentByteIndex] & 0x0F);
			//System.out.println("unpack_"+firstDigit+"_"+secondDigit);
			unpackedData += String.valueOf(firstDigit);
			if (currentByteIndex == (packedData.length - 1)) {
				if (secondDigit == negativeSign) {
					unpackedData = "-" + unpackedData;
				}
			} else {
				unpackedData += String.valueOf(secondDigit);
			}
		}
		if (decimalPointLocation > 0) {
			unpackedData = unpackedData.substring(0,
					(unpackedData.length() - decimalPointLocation))
					+ "."
					+ unpackedData.substring(unpackedData.length()
							- decimalPointLocation);
		}
		return unpackedData;
	}
	public static String getBinary(byte[] b,int decimalPointLocation) {
		long val = 0;
		for (int i = 0; i < b.length; i++) {
			int low = b[i] & 0x0F;
			int high = (b[i] >> 4) & 0x0f;
			if (low < 0)
				low *= -1;
			if (high < 0)
				high *= -1;
			int num = high * 16 + low;
			val = 256 * val + num;
//			System.out.println("LOW:"+low+"high:"+high);
		}
		String s = ""+val;
		while(s.length()<b.length*2){
			s="0"+s;
		}
//		System.out.println(s);
		if (decimalPointLocation > 0) {
			s = s.substring(0,
					(s.length() - decimalPointLocation))
					+ "."
					+ s.substring(s.length()
							- decimalPointLocation);
		}
		return s;
	}

}
