package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Translates between a target object type and the type used by DBvolution.
 * 
 * <p> This approach means that DBv has very little control.
 * Implementations of this class can return brand new instances of the DBv
 * data type every time {@link #toDBvValue(Object)} is called. 
 * Which breaks the dirty flag and means that the end-user has to look after conversions
 * to/from permissable/excluded values if they want that feature.
 * @param <T> the target object type
 * @param <Q> the DBvolution data type
 */
public interface DBTypeAdaptor<T, Q extends QueryableDatatype> {
	/**
	 * Null values must be handled correctly.
	 * @param dbvValue
	 * @return
	 */
	public T toObjectValue(Q dbvValue);

	/**
	 * Null values must be handled correctly.
	 * @param objectValue
	 * @return
	 */
	public Q toDBvValue(T objectValue);
}