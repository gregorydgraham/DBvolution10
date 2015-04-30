package nz.co.gregs.dbvolution.internal.properties;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import nz.co.gregs.dbvolution.annotations.DBColumn;

/**
 * Used internally to control filtering of properties when first retrieving them.
 * The same logic could be included directly within {@link JavaPropertyFilter},
 * but this way the logic is sitting in one place plus it gives the ability to 
 * re-use the {@link JavaPropertyFilter} if we ever extend the to process all fields/bean-properties.
 * @author Malcolm Lett
 */
interface JavaPropertyFilter {
	/** Accepts everything */
	public static final JavaPropertyFilter ANY_PROPERTY_FILTER = new AnyPropertyFilter();
	
	/** Accepts only properties with {@link DBColumn} annotation */
	public static final JavaPropertyFilter COLUMN_PROPERTY_FILTER = new ColumnPropertyFilter();
	
	/**
	 * Indicates whether the specified field is accepted by the filter.
	 
	 * @return
	 */
	public boolean acceptField(Field field);
	
	/**
	 * Indicates whether the specified getter/setter pair are accepted
	 * by the filter.
	 
	 
	 * @return
	 */
	public boolean acceptBeanProperty(Method getter, Method setter);

	/**
	 * Implementation that accepts everything.
	 */
	class AnyPropertyFilter implements JavaPropertyFilter {
		@Override
		public boolean acceptField(Field field) {
			return true;
		}

		@Override
		public boolean acceptBeanProperty(Method getter, Method setter) {
			return true;
		}
	}
	
}
