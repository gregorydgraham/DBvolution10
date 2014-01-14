package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.DBDatabase;

/**
 * Experimental representation of an enum-aware QDT.
 *
 * @param <E>
 */
public abstract class DBEnum<E extends DBEnumValue<?>> extends QueryableDatatype {

    public DBEnum() {
    }

    public DBEnum(E value) {
        super(value);
    }

    @SuppressWarnings("unchecked")
    public DBEnumValue<?> getDBEnumValue() {
        Object value = super.getValue();
        if (value instanceof DBEnumValue<?>) {
            DBEnumValue<?> enumVal = (DBEnumValue<?>) value;
            return enumVal;
        } else {
            throw new IncompatibleClassChangeError("Set Value Is Not A DBEnumValue: getValue() needs to return a DBEnumValue but it returned a " + value.getClass().getSimpleName() + " instead.");
        }
    }

    /**
     * Gets the DBEnum's literal value.
     *
     * <p> For example, if the database column uses integers (e.g.: 1,2,3) as
     * values, then this method should return literal values of type
     * {@code Integer}. Alternatively, if the database column uses strings
     * (e.g.: "READY", "PROCESSING", "DONE") as values, then this method should
     * return literal values of type {@code String}.
     *
     * @return the literal value in the appropriate type for the value in the
     * database
     */
    public abstract Object getEnumLiteralValue();

    @Override
    protected String formatValueForSQLStatement(DBDatabase db) {
        final Object databaseValue = getEnumLiteralValue();
        if (databaseValue == null) {
            return db.getDefinition().getNull();
        } else {
            QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(databaseValue);
            return qdt.formatValueForSQLStatement(db);
        }
    }
}
