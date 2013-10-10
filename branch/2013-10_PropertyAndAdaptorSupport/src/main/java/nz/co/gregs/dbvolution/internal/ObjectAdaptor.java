package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

public class ObjectAdaptor {
	private ClassAdaptor clazz;
	private Object adaptee;
	
	public ObjectAdaptor(DBDefinition dbDefn, Object obj) {
		this.adaptee = obj;
		this.clazz = new ClassAdaptor(dbDefn, obj.getClass()); // TODO: lookup class cache for better performance
	}

	ObjectAdaptor(ClassAdaptor clazz, Object obj) {
		this.adaptee = obj;
		this.clazz = clazz;
	}
	
	/**
	 * Checks all the annotations etc. and errors.
	 * @throws Exception if has any errors
	 */
	public void checkForErrors() throws Exception {
		clazz.checkForErrors();
	}
	
	public Property getPropertyByColumn(String columnName) {
		return clazz.getPropertyByColumn(columnName);
	}
}
