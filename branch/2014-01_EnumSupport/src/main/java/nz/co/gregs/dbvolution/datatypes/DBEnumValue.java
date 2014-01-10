package nz.co.gregs.dbvolution.datatypes;

/**
 * Used to identify the database-centric code value for each
 * value of an enumeration.
 * Java enumerations that are used with {@code DBEnumType} columns
 * must implement this interface.
 */
public interface DBEnumValue {
	/**
	 * Gets the enum value's database-centric code value.
	 * 
	 * <p> For example, if the database column uses integer values (eg: 1,2,3) as code values,
	 * then this method should return values of type {@code Integer}.
	 * Alternatively, if the database column uses string values (eg: "READY", "PROCESSING", "DONE") as
	 * code values, then this method should return values of type {@code String}.
	 * @return the code value in the appropriate type for the value in the database
	 */
	public Object getCode();
}
