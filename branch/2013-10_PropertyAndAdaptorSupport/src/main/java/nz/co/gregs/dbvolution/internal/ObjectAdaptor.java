package nz.co.gregs.dbvolution.internal;

public class ObjectAdaptor {
	private ClassAdaptor clazz;
	private Object adaptee;
	
	public ObjectAdaptor(Object obj) {
		this.adaptee = obj;
		this.clazz = new ClassAdaptor(obj.getClass()); // TODO: lookup class cache for better performance
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
