package nz.co.gregs.dbvolution.datatypes;

/**
 * Used to identify the database-centric value of an enumeration. Java
 * enumerations that are used with {@code DBEnumType} columns must implement
 * this interface.
 */
public interface DBEnumValue<O extends Object> {

    /**
     * Gets the enum's literal value.
     *
     * <p> For example, if the database column uses integers (e.g.: 1,2,3) as
     * values, then this method should return literal values of type
     * {@code Integer}. Alternatively, if the database column uses strings (e.g.:
     * "READY", "PROCESSING", "DONE") as values, then this method should return
     * literal values of type {@code String}.
     *
     * @return the literal value in the appropriate type for the value in the
     * database.
     */
    public abstract O getLiteralValue();
}
