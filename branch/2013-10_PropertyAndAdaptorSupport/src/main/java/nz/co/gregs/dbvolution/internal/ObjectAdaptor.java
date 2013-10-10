package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

public class ObjectAdaptor {
	private final DBDefinition dbDefn;
	private ClassAdaptor clazz;
	private Object adaptee;
	
//	public ObjectAdaptor(DBDefinition dbDefn, Object obj) {
//		this.dbDefn = dbDefn;
//		this.adaptee = obj;
//		this.clazz = new ClassAdaptor(dbDefn, obj.getClass()); // TODO: lookup class cache for better performance
//	}

	ObjectAdaptor(DBDefinition dbDefn, ClassAdaptor clazz, Object obj) {
		this.dbDefn = dbDefn;
		this.adaptee = obj;
		this.clazz = clazz;
	}
	
	public Property getPropertyByColumn(String columnName) {
		return clazz.getPropertyByColumn(dbDefn, columnName);
	}
}
