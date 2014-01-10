/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.generators.DataGenerator;
import nz.co.gregs.dbvolution.exceptions.UnableInstantiateQueryableDatatypeException;
import nz.co.gregs.dbvolution.exceptions.UnableToCopyQueryableDatatypeException;
import nz.co.gregs.dbvolution.operators.*;

/**
 *
 * @author gregory.graham
 */
public abstract class QueryableDatatype extends Object implements Serializable, DataGenerator {

    public static final long serialVersionUID = 1L;
    protected Object literalValue = null;
    protected boolean isDBNull = false;
    protected boolean includingNulls = false;
    protected DBOperator operator = null;
    protected boolean undefined = true;
    protected boolean changed = false;
    protected QueryableDatatype previousValueAsQDT = null;
    protected boolean isPrimaryKey;
    public final static Boolean SORT_ASCENDING = Boolean.TRUE;
    public final static Boolean SORT_DESCENDING = Boolean.FALSE;
    protected Boolean sort = SORT_ASCENDING;

    /**
     *
     */
    protected QueryableDatatype() {
    }

    /**
     *
     * @param trans
     * @param obj
     */
    protected QueryableDatatype(Object obj) {
        if (obj == null) {
            this.isDBNull = true;
        } else{
            this.literalValue = obj;
            this.operator = new DBEqualsOperator(this);
        }
    }

    public static <T extends QueryableDatatype> T getQueryableDatatypeInstance(Class<T> requiredQueryableDatatype) {
        try {
            return requiredQueryableDatatype.getConstructor().newInstance();
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (SecurityException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        }
    }

    static public QueryableDatatype getQueryableDatatypeForObject(Object o) {
        QueryableDatatype qdt;
        if (o instanceof DataGenerator) {
            qdt = new DBDataGenerator();
        } else if (o instanceof Integer) {
            qdt = new DBInteger();
        } else if (o instanceof Number) {
            qdt = new DBNumber();
        } else if (o instanceof String) {
            qdt = new DBString();
        } else if (o instanceof Date) {
            qdt = new DBDate();
        } else if (o instanceof Byte[]) {
            qdt = new DBByteArray();
        } else if (o instanceof Boolean) {
            qdt = new DBBoolean();
        } else {
            qdt = new DBJavaObject();
        }
        qdt.setValue(o);
        return qdt;
    }

    /**
     * Copies a QueryableDatatype and returns the copy.
     *
     * Used internally to provide immutability to DBOperator objects.
     *
     * The intention is that this method will provide a snapshot of the QDT at
     * this moment in time and copy or clone any internal objects that might
     * change.
     *
     * Subclasses should extend this method if they have fields that maintain
     * the state of the QDT.
     *
     * Always use the super.copy() method first when overriding this method.
     *
     * @return a complete copy of the QDT with all values set.
     */
    @Override
    public QueryableDatatype copy() {
        QueryableDatatype newQDT = this;
        try {
            newQDT = this.getClass().newInstance();

            newQDT.literalValue = this.literalValue;
            newQDT.isDBNull = this.isDBNull;
            newQDT.includingNulls = this.includingNulls;
            newQDT.operator = this.operator;
            newQDT.undefined = this.undefined;
            newQDT.changed = this.changed;
            if (this.previousValueAsQDT != null) {
                newQDT.previousValueAsQDT = this.previousValueAsQDT.copy();
            }
            newQDT.isPrimaryKey = this.isPrimaryKey;
            newQDT.sort = this.sort;
        } catch (InstantiationException ex) {
            throw new UnableInstantiateQueryableDatatypeException(this, ex);
        } catch (IllegalAccessException ex) {
            throw new UnableToCopyQueryableDatatypeException(this, ex);
        }

        return newQDT;
    }

    @Override
    public String toString() {
        return (literalValue == null ? "" : literalValue.toString());
    }

    /**
     * Returns the raw value as a String
     *
     * @return the literal value as a String
     */
    public String stringValue() {
        return (literalValue == null ? "" : literalValue.toString());
    }

    public Long longValue() {
        if (isDBNull || literalValue == null) {
            return null;
        } else if (literalValue instanceof Long) {
            return (Long) literalValue;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).longValue();
        } else {
            return Long.parseLong(literalValue.toString());
        }

    }

    public Integer intValue() {
        if (isDBNull || literalValue == null) {
            return null;
        } else if (literalValue instanceof Integer) {
            return (Integer) literalValue;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).intValue();
        } else {
            return Integer.parseInt(literalValue.toString());
        }

    }

    public Double doubleValue() {
        if (isDBNull || literalValue == null) {
            return null;
        } else if (literalValue instanceof Double) {
            return (Double) literalValue;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).doubleValue();
        } else {
            return Double.parseDouble(literalValue.toString());
        }
    }

    protected void blankQuery() {
        includingNulls = false;
        isDBNull = false;
        this.operator = null;
    }

    /**
     *
     * @param db
     * @param columnName
     * @return the section of the SQL query between WHERE and ORDER BY
     */
    public String getWhereClause(DBDatabase db, String columnName) {
        return getWhereClauseUsingOperators(db, columnName);
    }

    private String getWhereClauseUsingOperators(DBDatabase db, String columnName) {
        String whereClause = "";
        DBOperator op = this.getOperator();
        if (op != null) {
            whereClause = op.generateWhereLine(db, columnName);
        }
        return whereClause;
    }

    public void negateOperator() {
        if (getOperator() != null) {
            getOperator().invertOperator(true);
        } else {
            throw new RuntimeException("No Operator Has Been Defined Yet: please use the query methods before inverting the operation");
        }
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Object... permitted) {
        this.setOperator(new DBPermittedValuesOperator(permitted));
    }

    /**
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(String... permitted) {
        this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
    }

    public void permittedValuesIgnoreCase(List<String> permitted) {
        this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
    }

    public void permittedValuesIgnoreCase(Set<String> permitted) {
        this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
    }

    /**
     *
     * @param excluded
     */
    public void excludedValuesIgnoreCase(String... excluded) {
        setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
        negateOperator();

    }

    /**
     *
     * @param excluded
     */
    public void excludedValuesIgnoreCase(List<String> excluded) {
        setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
        negateOperator();
    }

    /**
     *
     * @param excluded
     */
    public void excludedValuesIgnoreCase(Set<String> excluded) {
        setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
        negateOperator();
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(Object... excluded) {
        this.setOperator(new DBPermittedValuesOperator(excluded));
        negateOperator();
    }

    public void permittedRange(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
    }

    public void permittedRangeInclusive(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
    }

    public void excludedRange(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeInclusive(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
        negateOperator();

    }

    public void permittedPattern(String pattern) {
        this.setOperator(new DBLikeOperator(this));
    }

    public void excludedPattern(String pattern) {
        this.setOperator(new DBLikeOperator(this));
        this.negateOperator();
    }

    /**
     * Gets the current literal value of this queryable data type, without any
     * formatting. The returned value <i>should/<i> be in the correct type as
     * appropriate for the type of queryable data type.
     *
     * <p>
     * The literal value is undefined (and {@code null}) if using an operator
     * other than {@code equals}.
     *
     * @return the literal value, if defined, which may be null
     */
    // FIXME sometimes strings are returned for DBNumber types
    public Object getValue() {
        return literalValue;
    }

    /**
     * Sets the literal value of this queryable data type. Replaces any assigned
     * operator with an {@code equals} operator on the given value.
     *
     * @param newLiteralValue the literalValue to set
     */
    public void setValue(Object newLiteralValue) {
        preventChangeOfPrimaryKey();
        if (newLiteralValue == null) {
            setToNull();
        } else {
            if (newLiteralValue instanceof DataGenerator) {
                setChanged((DataGenerator) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDataGenerator((DataGenerator) newLiteralValue)));
            } else if (newLiteralValue instanceof Date) {
                setChanged((Date) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDate((Date) newLiteralValue)));
            } else if (newLiteralValue instanceof Timestamp) {
                setChanged((Timestamp) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDate((Timestamp) newLiteralValue)));
            } else {
                setChanged(newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(this.copy()));
            }
        }
    }

    public void setUnchanged() {
        changed = false;
        previousValueAsQDT = null;
    }


    /**
     *
     * Sets the value of this column to DBNull Also changes the operator to
     * DBIsNullOperator for comparisons
     *
     * @return the DBOperator that will be used with this QDT
     */
    protected DBOperator setToNull() {
        this.literalValue = null;
        this.isDBNull = true;
        this.setOperator(new DBIsNullOperator());
        return getOperator();
    }

    public void includingNulls() {
        this.operator.includeNulls();
    }

    /**
     *
     * Provides the SQL datatype used by default for this type of object
     *
     * This should be overridden in each subclass
     *
     * Example return value: "VARCHAR(1000)"
     *
     * @return the standard SQL datatype that corresponds to this QDT as a String
     */
    public abstract String getSQLDatatype();

    /**
     *
     * @param db
     * @return the literal value as it would appear in an SQL statement i.e.
     * {yada} => 'yada' {} => NULL
     */
    public final String toSQLString(DBDatabase db) {
        DBDefinition def = db.getDefinition();
        if (this.isDBNull || literalValue == null) {
            return def.getNull();
        }
//        return transform.generate(formatValueForSQLStatement(db));
        return formatValueForSQLStatement(db);
    }

    /**
     *
     * Returns the value of the object formatted for the database
     *
     * This should be overridden in each subclass
     *
     * This method is called by toSQLString after checking for NULLs and should
     * return a string representation of the object formatted for use within a
     * SQL select, insert, update, or delete statement.
     *
     * For Example:
     *
     * DBString{yada} => 'yada'
     *
     * DBInteger{1234} => 123
     *
     * DBDate{1/March/2013} => TO_DATE('20130301', 'YYYYMMDD')
     *
     * @param db
     * @return the literal value translated to a String ready to insert into an SQL statement
     */
    protected abstract String formatValueForSQLStatement(DBDatabase db);

    /**
     * @return the operator
     */
    public DBOperator getOperator() {
        return operator;
    }

    /**
     * @param operator the operator to set
     */
    // FIXME I think this should set 'literalValue' to null
    public void setOperator(DBOperator operator) {
        blankQuery();
        this.operator = operator;
        if (undefined) {
            undefined = false;
        } else {
            changed = true;
        }
    }

    public boolean hasChanged() {
        return changed;
    }

    /**
     *
     * @param resultSet
     * @param resultSetColumnName
     * @throws SQLException
     */
    public void setFromResultSet(ResultSet resultSet, String resultSetColumnName) throws SQLException {
        if (resultSet == null || resultSetColumnName == null) {
            this.setToNull();
        } else {
            String dbValue;
            try {
                dbValue = resultSet.getString(resultSetColumnName);
                if (resultSet.wasNull()) {
                    dbValue = null;
                }
            } catch (SQLException ex) {
                // Probably means the column wasn't selected.
                dbValue = null;
            }
            if (dbValue == null) {
                this.setToNull();
            } else {
                this.setValue(dbValue);
            }
        }
    }

    private void preventChangeOfPrimaryKey() {
        if (this.isPrimaryKey && !this.undefined) {
            throw new RuntimeException("Accidental Change Of Primary Key Stopped: Use the changePrimaryKey() method to change the primary key's value.");
        }
    }

    private void setChanged(Object newLiteralValue) {
        if (this.isDBNull
                || (literalValue != null && !newLiteralValue.equals(literalValue))) {
            changed = true;
            QueryableDatatype newInstance = QueryableDatatype.getQueryableDatatypeInstance(this.getClass());
            if (this.isDBNull) {
                newInstance.setToNull();
            } else {
                newInstance.setValue(this.literalValue);
            }
            previousValueAsQDT = newInstance;
        }
    }

    public boolean isNull() {
        return isDBNull || literalValue == null;
    }

    public String getPreviousSQLValue(DBDatabase db) {
        return previousValueAsQDT.toSQLString(db);
    }

    /**
     * Used to switch the direction of the column's sort order
     * 
     * use setSortOrderAscending() and setSortOrderDescending() where possible
     *
     * use setSortOrderAscending() and setSortOrderDescending() where possible
     *
     * Use Boolean.TRUE for Ascending Use Boolean.FALSE for Descending
     *
     * @param order
     * @return this object
     */
    private QueryableDatatype setSortOrder(Boolean order) {
        sort = order;
        return this;
    }

    /**
     * Used to switch the direction of the column's sort order
     *
     * @return this object
     */
    public QueryableDatatype setSortOrderAscending() {
        return this.setSortOrder(true);
    }

    /**
     * Used to switch the direction of the column's sort order
     *
     * @return this object
     */
    public QueryableDatatype setSortOrderDescending() {
        return this.setSortOrder(false);
    }

    public Boolean getSortOrder() {
        return sort;
    }

    public void clear() {
        blankQuery();
    }

    public boolean equals(QueryableDatatype other) {
        if (other == null) {
            return false;
        } else if (this.operator == null && other.operator == null) {
            return true;
        } else if (this.operator != null && other.operator == null) {
            return false;
        } else if (this.operator == null && other.operator != null) {
            return false;
        } else {
            return this.getOperator().equals(other.getOperator());
        }
    }
}
