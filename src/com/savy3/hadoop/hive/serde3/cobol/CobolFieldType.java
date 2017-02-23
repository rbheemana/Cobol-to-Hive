package com.savy3.hadoop.hive.serde3.cobol;

public enum CobolFieldType {
	STRING(Group.ELEMENTARY),
	NUMBER(Group.ELEMENTARY),
	REDEFINES(Group.GROUP),
	OCCURS(Group.GROUP),
	DEPEND(Group.GROUP),
	ORDINARY(Group.GROUP);
	
    private Group group;

    CobolFieldType(Group group) {
        this.group = group;
    }

    public boolean isInGroup(Group group) {
        return this.group == group;
    }

    public enum Group {
    	ELEMENTARY,
        GROUP;
    }
	
}
