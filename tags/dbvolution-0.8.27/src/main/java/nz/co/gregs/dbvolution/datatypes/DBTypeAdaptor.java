package nz.co.gregs.dbvolution.datatypes;

/**
 * Translates between a target object's property type
 * and the type used by DBvolution.
 * 
 * <p> On the DBvolution side, the following types are supported:
 * <ul>
 * <li> String
 * <li> Integer, Long
 * <li> Float, Double
 * <li> arbitrary object type (requires the use of DBJavaObject)
 * </ul>
 * 
 * <p> If the target object's property is a DBvolution type, then
 * the supported types are the same as above. Otherwise
 * any object type can be used that is assignable with the type of the
 * property this adaptor is used on. 
 * 
 * @param <T> the type of the property on the target object
 * @param <D> the database side type: the type of the property once translated for DBvolution use
 */
public interface DBTypeAdaptor<T, D> {
	/**
	 * Null values must be handled correctly.
	 * @param dbvValue
	 * @return
	 */
	public T fromDatabaseValue(D dbvValue);

	/**
	 * Null values must be handled correctly.
	 * @param objectValue
	 * @return
	 */
	public D toDatabaseValue(T objectValue);
}