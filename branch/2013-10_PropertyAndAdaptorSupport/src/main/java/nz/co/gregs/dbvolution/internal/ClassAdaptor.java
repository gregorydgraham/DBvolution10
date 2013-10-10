package nz.co.gregs.dbvolution.internal;

import java.util.List;

public class ClassAdaptor {
	private Class<?> adaptee;
	
	public ClassAdaptor(Class<?> clazz) {
		this.adaptee = clazz;
	}
	
	/**
	 * Checks all the annotations etc. and errors.
	 * @throws Exception if has any errors
	 */
	public void checkForErrors() throws Exception {
		// TODO: to be implemented
	}
	
	public Property getPropertyByColumn(Object adaptee, String columnName) {
		// TODO: scan properties by annotation, or look up cached value
		
		return null;
	}

	public Property getPropertyByName(Object adaptee, String propertyName) {
		// TODO: scan properties by field or bean property name, or look up cached value
		
		return null;
	}
	
	public List<Property> getProperties() {
		// TODO: scan for all DBv properties
		
		return null;
	}
	
}
