package nz.co.gregs.dbvolution.internal;

import java.lang.reflect.Field;

public class FieldPropertyAdaptor extends AbstractPropertyAdaptor {
	private Field field;
	
	public FieldPropertyAdaptor(Field field) {
		this.field = field;
	}
	
	
}
