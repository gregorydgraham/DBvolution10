package nz.co.gregs.dbvolution.internal.properties;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import nz.co.gregs.dbvolution.annotations.DBColumn;

/**
 * Used internally to control filtering of properties when first retrieving
 * them. The same logic could be included directly within
 * {@link JavaPropertyFilter}, but this way the logic is sitting in one place
 * plus it gives the ability to re-use the {@link JavaPropertyFilter} if we ever
 * extend the to process all fields/bean-properties.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
interface JavaPropertyFilter {

	/**
	 * Accepts everything
	 */
	public static final JavaPropertyFilter ANY_PROPERTY_FILTER = new AnyPropertyFilter();

	/**
	 * Accepts only properties with {@link DBColumn} annotation
	 */
	public static final JavaPropertyFilter COLUMN_PROPERTY_FILTER = new ColumnPropertyFilter();

	/**
	 * Accepts only properties that are a field or have both getter and setter
	 */
	public static final JavaPropertyFilter COLUMN_OR_AUTOFILLABLE_PROPERTY_FILTER = new ColumnOrAutoFillPropertyFilter();

	/**
	 * Indicates whether the specified field is accepted by the filter.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the field is acceptable, otherwise FALSE.
	 */
	public boolean acceptField(Field field);

	/**
	 * Indicates whether the specified getter/setter pair are accepted by the
	 * filter.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the field is acceptable, otherwise FALSE.
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
