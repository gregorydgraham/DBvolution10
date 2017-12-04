package nz.co.gregs.dbvolution.datatypes;

/**
 * Used to identify the database-centric value of an enumeration. Java
 * enumerations that are used with {@code DBEnumType} columns must implement
 * this interface.
 *
 * @param <V> type of database-centric code value
 */
public interface DBEnumValue<V> {

	/**
	 * Gets the enum's database-centric code value.
	 *
	 * <p>
	 * For example, if the database column uses integers (e.g.: 1,2,3) as values,
	 * then this method should return literal values of type {@code Integer}.
	 * Alternatively, if the database column uses strings (e.g.: "READY",
	 * "PROCESSING", "DONE") as values, then this method should return literal
	 * values of type {@code String}.
	 *
	 * <p>
	 * Generally this method should not return null. However it is tolerated in
	 * that a null literal value will translate to a null value in the database.
	 * Null database values are translated to null enum values. So this
	 * interpretation is not symmetric.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the non-null value in the appropriate type for the value in the
	 * database.
	 */
	public V getCode();
}
