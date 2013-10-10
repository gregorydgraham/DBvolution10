package nz.co.gregs.dbvolution.internal;

import java.lang.annotation.Annotation;

import nz.co.gregs.dbvolution.DBTypeAdaptor;
import nz.co.gregs.dbvolution.QueryableDataType;
import nz.co.gregs.dbvolution.annotations.DBColumn;

/**
 * Base implementation of {@link Property} interface, common to 
 * field and bean-properties.
 * @author Malcolm Lett
 */
abstract class AbstractPropertyAdaptor implements Property {
	private final DBTypeAdaptor typeAdaptor; // null if no annotation
	
	public AbstractPropertyAdaptor() {
	}
	
	protected void init() {
		//if ()
		//typeAdaptor = new
	}
	
	protected abstract <A extends Annotation> A getAnnotation(Class<A> annotationType);

	protected abstract <A extends Annotation> A getAnnotations(Class<A> annotationType);
	
	@Override
	public Object value() {
		return null;
	}

	@Override
	public void setValue(Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Class<? extends QueryableDataType> type() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String columnName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isForeignKey() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Class<?> foreignClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String foreignColumnName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBColumn getDBColumnAnnotation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isReadable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWritable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object getRawValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRawValue(Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Class<?> getRawType() {
		// TODO Auto-generated method stub
		return null;
	}

}
