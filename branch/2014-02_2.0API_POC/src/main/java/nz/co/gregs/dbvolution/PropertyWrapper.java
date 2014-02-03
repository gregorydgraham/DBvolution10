package nz.co.gregs.dbvolution;

import java.lang.reflect.Field;

public class PropertyWrapper {
	private Class<?> rowType;
	private Field field;
	private QueryableDatatype qdt;
	
	public static PropertyWrapper getInstance(Class<?> rowType, String fieldName) {
		Field field;
		try {
			field = rowType.getField(fieldName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		PropertyWrapper wrapper = new PropertyWrapper();
		wrapper.rowType = rowType;
		wrapper.field = field;
		return wrapper;
	}
	
	// TODO: get the appropriate QDT type according to the field type
	public QueryableDatatype getQDT() {
		if (qdt != null) {
			//Class<?> fieldType = field.getType();
			qdt = new QueryableDatatype();
		}
		return qdt;
	}
}
