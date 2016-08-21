package com.savy3.hadoop.hive.serde3.cobol;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.savy3.hadoop.hive.serde2.cobol.CobolSerdeException;

public class CobolGroupField extends CobolField{
	int occurs;
	public CobolGroupField() {
		super();
	}

	public CobolGroupField(String debugInfo,int levelNo, String name,int occurs, int length) {
		super(debugInfo, levelNo, name, length);
		this.occurs = occurs;
		if (super.type == null){
			if (occurs > 1){
				super.type = CobolFieldType.OCCURS;
			}else{
				super.type =CobolFieldType.ORDINARY;
			}
		}
		subfields = new ArrayList<CobolField>();
	}

	private List<CobolField> subfields;
	public void add(CobolField e) {
		subfields.add(e);
	}

	public void remove(CobolField e) {
		subfields.remove(e);
	}

	public List<CobolField> getsubfields() {
		return subfields;
	}

//	@Override
//	public String getDebugInfo() {
//		String debugInfo = super.getDebugInfo()+"\n";
//		
//		for(CobolField cf : subfields){
//			if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
//				debugInfo = debugInfo +cf.getDebugInfo()+"\n";
//			}else{
//				debugInfo = debugInfo +((CobolGroupField)cf).getDebugInfo()+"\n";
//			}
//		}
//		return debugInfo;
//		
//	}

	public List<String> getHiveColumnNames() {
		List<String> hiveColumnNames = new ArrayList<String>();
		int count = occurs;
		//System.out.println(subfields.size());
		while(count>0) {
			for (CobolField cf : subfields) {
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					if(occurs>1){
					hiveColumnNames.add(cf.getName()+"_oc"+(occurs-count+1));
					}else{
						hiveColumnNames.add(cf.getName());
					}
				} else {
					hiveColumnNames.addAll(((CobolGroupField) cf)
							.getHiveColumnNames());
				}
			}
			count--;
		}
		return hiveColumnNames;
	}
	//getCobolHiveMapping()
	public List<String> getHiveColumnComments(int i) throws CobolSerdeException {
		List<String> hiveColumnComments = new ArrayList<String>();
		int count = occurs;
		if(this.levelNo<2){
			this.offset = 0;
		}
		if(this.type == CobolFieldType.DEPEND){
			count = occurs;
		}
		int offset = this.offset+this.length;
		if(this.type == CobolFieldType.REDEFINES){
			try{
				this.offset = ((CobolGrpRedefinesField)this).getRedefinesField().getOffset();
				offset = this.offset;
			}catch (Exception e) {
				throw new CobolSerdeException("Error getting redefines field:"+((CobolGrpRedefinesField)this).toString());
			}
			
		}
		while(count>0) {
			for (CobolField cf : subfields) {
				cf.setOffset(offset);
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					hiveColumnComments.add(cf.getDebugInfo().trim()+"|"+cf.getOffset()+"|"+cf.getLength());
					offset+=cf.getLength();
				} else {
					hiveColumnComments.addAll(((CobolGroupField) cf)
							.getHiveColumnComments(i+1));
					offset = cf.getOffset() + cf.getSize();
				}
			}
			count--;
		}
		return hiveColumnComments;
	}
	public List<TypeInfo> getHiveColumnTypes() {
		List<TypeInfo> hiveColumnNames = new ArrayList<TypeInfo>();
		int count = occurs;
		while(count>0) {
			for (CobolField cf : subfields) {
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					hiveColumnNames.add(cf.getTypeInfo());
				} else {
					hiveColumnNames.addAll(((CobolGroupField) cf)
							.getHiveColumnTypes());
				}
			}
			count--;
		}
		return hiveColumnNames;
	}

	public List<ObjectInspector> getObjectInspectors() {
		List<ObjectInspector> hiveColumnNames = new ArrayList<ObjectInspector>();
		int count = occurs;
		while(count>0) {
			for (CobolField cf : subfields) {
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					hiveColumnNames.add(cf.getOi());
				} else {
					hiveColumnNames.addAll(((CobolGroupField) cf)
							.getObjectInspectors());
				}
			}
			count--;
		}
		return hiveColumnNames;
	}
	public List<CobolField> getElementaryFields() {
		List<CobolField> hiveColumnNames = new ArrayList<CobolField>();
		int count = occurs;
		while(count>0) {
			for (CobolField cf : subfields) {
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					hiveColumnNames.add(cf);
				} else {
					hiveColumnNames.addAll(((CobolGroupField) cf)
							.getElementaryFields());
				}
			}
			count--;
		}
		return hiveColumnNames;
	}
	public List<String> getLayout(int i) {
		List<String> cobolLayout = new ArrayList<String>();
		int count = occurs;
		String s="+"+i;
		for(int j=0;j<i;j++)
			s+="\t";
		while(count>0) {
			for (CobolField cf : subfields) {
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					//System.out.println("+++"+cf.getDebugInfo());
					
					cobolLayout.add(s+cf.getDebugInfo().trim());
				} else {
					cobolLayout.add(s+cf.getDebugInfo());
					cobolLayout.addAll(((CobolGroupField) cf)
							.getLayout(i+1));
				}
			}
			count--;
		}
		return cobolLayout;
	}
	public List<String> getCobolHiveMapping(int i) throws CobolSerdeException {
		List<String> cobolLayout = new ArrayList<String>();
		int count = occurs;
		String s1="|+"+(i-1)+"|";
		for(int j=0;j<i-1;j++)
			s1+="\t";
		cobolLayout.add(s1+this.getDebugInfo().trim()+"|"+this.name+"\t"+"|"+this.offset+"|0|");
		String s="|+"+i+"|";
		for(int j=0;j<i;j++)
			s+="\t";
		if(this.levelNo<2){
			this.offset = 0;
		}
		if(this.type == CobolFieldType.DEPEND){
			count = occurs;
		}
		int offset = this.offset+this.length;
		if(this.type == CobolFieldType.REDEFINES){
			try{
				this.offset = ((CobolGrpRedefinesField)this).getRedefinesField().getOffset();
				offset = this.offset;
			}catch (Exception e) {
				throw new CobolSerdeException("Error getting redefines field:"+((CobolGrpRedefinesField)this).toString());
			}
			
		}
		while(count>0) {
			for (CobolField cf : subfields) {
				cf.setOffset(offset);
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					cobolLayout.add(s+cf.getCobolHiveMapping());
					offset+=cf.getLength();
				} else {
//					cobolLayout.add(s+cf.getDebugInfo());
					cobolLayout.addAll(((CobolGroupField) cf)
							.getCobolHiveMapping(i+1));
					offset = cf.getOffset() + cf.getSize();
				}
			}
			count--;
		}
		return cobolLayout;
	}
	public int getSize() {
		int size=0;
		int count = occurs;
		while(count>0) {
			for (CobolField cf : subfields) {
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
					size+=cf.getLength();
				} else {
					if(cf.getType() != CobolFieldType.REDEFINES){
						size+=((CobolGroupField) cf)
							.getSize();
					}
				}
			}
			count--;
		}
		return size;
	}
	public List<Object> deserialize(byte[] rowBytes) throws CobolSerdeException{
		List<Object> hiveColumnNames = new ArrayList<Object>();
		int count = occurs;
		if(this.levelNo<2){
			this.offset = 0;
		}
		if(this.type == CobolFieldType.DEPEND){
			count = Integer.parseInt(((CobolGrpDependField)this).getDependField().deserialize(rowBytes).toString());
		}
		int offset = this.offset+this.length;
		if(this.type == CobolFieldType.REDEFINES){
			try{
				this.offset = ((CobolGrpRedefinesField)this).getRedefinesField().getOffset();
				offset = this.offset;
			}catch (Exception e) {
				throw new CobolSerdeException("Error getting redefines field:"+((CobolGrpRedefinesField)this).toString());
			}
			
		}
		//offset = this.offset+this.length;
		while(count>0) {
			for (CobolField cf : subfields) {
				cf.setOffset(offset);
				
				if (cf.getType().isInGroup(CobolFieldType.Group.ELEMENTARY)) {
						hiveColumnNames.add(cf.deserialize(rowBytes));
						offset+=cf.getLength();
				} else {
//					System.out.println(cf.getName());
					hiveColumnNames.addAll(((CobolGroupField) cf)
							.deserialize(rowBytes));
					if(cf.getType() == CobolFieldType.REDEFINES){
						offset=((CobolGrpRedefinesField)cf).getRedefinesField().getOffset()+
								((CobolGrpRedefinesField)cf).getRedefinesField().getSize();
//						System.out.println(((CobolGrpRedefinesField)cf).getRedefinesField().getName()+":"+offset);
					}else{
						offset = cf.getOffset() + cf.getSize();
//						System.out.println(cf.getName()+":"+offset);
						
					}
				}
			}
			count--;
		}
		return hiveColumnNames;
	}
}
