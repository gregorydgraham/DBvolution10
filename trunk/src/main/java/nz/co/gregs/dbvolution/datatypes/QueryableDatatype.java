/*
 * Copyright 2013 gregory.graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.UnableInstantiateQueryableDatatypeException;
import nz.co.gregs.dbvolution.exceptions.UnableToCopyQueryableDatatypeException;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.operators.DBEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBIsNullOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedPatternOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesIgnoreCaseOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 *
 * @author gregory.graham
 */
public abstract class QueryableDatatype extends Object implements Serializable, DBExpression {

    public static final long serialVersionUID = 1L;
    protected Object literalValue = null;
    protected boolean isDBNull = false;
    protected boolean includingNulls = false;
    protected DBOperator operator = null;
    private boolean undefined = true;
    protected boolean changed = false;
    protected QueryableDatatype previousValueAsQDT = null;
    protected boolean isPrimaryKey;
    public final static Boolean SORT_ASCENDING = Boolean.TRUE;
    public final static Boolean SORT_DESCENDING = Boolean.FALSE;
    protected Boolean sort = SORT_ASCENDING;
    transient protected PropertyWrapperDefinition propertyWrapper; // no guarantees whether this gets set

    /**
     *
     */
    protected QueryableDatatype() {
    }

    /**
     *
     * @param obj
     */
    protected QueryableDatatype(Object obj) {
        if (obj == null) {
            this.isDBNull = true;
        } else {
            this.literalValue = obj;
            this.operator = new DBEqualsOperator(this);
            undefined = false;
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
        if (o instanceof QueryableDatatype) {
            qdt = QueryableDatatype.getQueryableDatatypeInstance(((QueryableDatatype) o).getClass());
            qdt.setLiteralValue(((QueryableDatatype) o).literalValue);
        } else {
            if (o instanceof DBExpression) {
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
            qdt.setLiteralValue(o);
        }
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
     * <p>
     * A database NULL is treated as an empty string, use {@link #isNull() } to
     * handle NULLs separately.
     *
     * @return the literal value as a String
     */
    public String stringValue() {
        return (literalValue == null ? "" : literalValue.toString());
    }

    /**
     * Use {@link #getValue() } instead
     *
     * <p>
     * This method undermines the type safety of the QDT objects
     *
     * <p>
     * However it has been implemented within DBNumber so will still work with
     * your existing objects.
     *
     * @return a Long of the value of this QDT.
     * @deprecated
     */
    @Deprecated
    public Long longValue() throws NumberFormatException {
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

    /**
     * Use {@link #getValue() } instead
     *
     * <p>
     * This method undermines the type safety of the QDT objects.
     *
     * <p>
     * However it has been implemented within DBNumber so will still work with
     * your existing objects.
     *
     * @return a Integer of the value of this QDT.
     * @deprecated
     */
    @Deprecated
    public Integer intValue() throws NumberFormatException {
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

    /**
     * Use {@link #getValue() } instead
     *
     * <p>
     * This method undermines the type safety of the QDT objects.
     *
     * <p>
     * However it has been implemented within DBNumber so will still work with
     * your existing objects.
     *
     * @return a Double of the value of this QDT.
     * @deprecated
     */
    @Deprecated
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
            throw new RuntimeException("No Operator Has Been Defined Yet: please use the permitted/excluded methods before negating the operation");
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
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * Strings ignoring letter case.
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(String... permitted) {
        this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * Strings ignoring letter case.
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(StringExpression... permitted) {
        this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * Strings ignoring letter case.
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(List<String> permitted) {
        this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * Strings ignoring letter case.
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(Set<String> permitted) {
        this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
    }

    /**
     * Reduces the rows to excluding the object, Set, List, Array, or vararg of
     * Strings ignoring letter case.
     *
     * @param excluded
     */
    public void excludedValuesIgnoreCase(String... excluded) {
        setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
        negateOperator();
    }

    /**
     * Reduces the rows to excluding the object, Set, List, Array, or vararg of
     * Strings ignoring letter case.
     *
     * @param excluded
     */
    public void excludedValuesIgnoreCase(StringExpression... excluded) {
        setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
        negateOperator();
    }

    /**
     * Reduces the rows to excluding the object, Set, List, Array, or vararg of
     * Strings ignoring letter case.
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

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified the lower-bound will be included
     * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
     * return 1 and 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRange(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be included in the search. I.e permittedRangeInclusive(1,3) will
     * return 1, 2, and 3.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRangeInclusive(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
     * return 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRangeExclusive(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
    }

    public void excludedRange(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeInclusive(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeExclusive(Object lowerBound, Object upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    /**
     * Perform searches based on using database compatible pattern matching
     *
     * <p>
     * This facilitates the LIKE operator.
     *
     * <p>
     * Please use the pattern system appropriate to your database.
     *
     * <p>
     * Java0-style regular expressions are not yet supported.
     *
     * @param pattern
     */
    public void permittedPattern(String pattern) {
        this.setOperator(new DBPermittedPatternOperator(pattern));
    }

    public void excludedPattern(String pattern) {
        this.setOperator(new DBPermittedPatternOperator(pattern));
        this.negateOperator();
    }

    public void permittedPattern(StringExpression pattern) {
        this.setOperator(new DBPermittedPatternOperator(pattern));
    }

    public void excludedPattern(StringExpression pattern) {
        this.setOperator(new DBPermittedPatternOperator(pattern));
        this.negateOperator();
    }

    /**
     * Gets the current literal value of this queryable data type. The returned
     * value <i>should/<i> be in the correct type as appropriate for the type of
     * queryable data type.
     *
     * <p>
     * This method will return NULL if the QDT represents a database NULL OR the
     * field is undefined. Use {@link #isNull() } and {@link #isDefined() } to
     * differentiate the 2 states.
     *
     * <p>
     * Undefined QDTs represents a QDT that is not a field from the database.
     * Undefined QDTs are similar to {@link DBRow#isDefined undefined DBRows}
     *
     * @return the literal value, if defined, which may be null
     */
    public Object getValue() {
        if (undefined || isNull()) {
            return null;
        } else {
            return literalValue;
        }
    }
    
    public abstract void setValue(Object newLiteralValue);

    /**
     * Sets the literal value of this queryable data type. Replaces any assigned
     * operator with an {@code equals} operator on the given value.
     *
     * @param newLiteralValue the literalValue to set
     */
    protected void setLiteralValue(Object newLiteralValue) {
        preventChangeOfPrimaryKey();
        if (newLiteralValue == null) {
            setToNull();
        } else {
            if (newLiteralValue instanceof DBExpression) {
                setChanged((DBExpression) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDataGenerator((DBExpression) newLiteralValue)));
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
     * @return the standard SQL datatype that corresponds to this QDT as a
     * String
     */
    public abstract String getSQLDatatype();

    /**
     * Formats the literal value for use within an SQL statement.
     *
     * <p>
     * This is used internally to transform the Java object in to SQL format.
     * You won't need to use it.
     *
     * @param db
     * @return the literal value as it would appear in an SQL statement i.e.
     * {yada} => 'yada' {} => NULL
     */
    @Override
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
     * @return the literal value translated to a String ready to insert into an
     * SQL statement
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
                this.setLiteralValue(dbValue);
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
                newInstance.setLiteralValue(this.literalValue);
            }
            previousValueAsQDT = newInstance;
        }
    }

    /**
     * Indicates whether object is NULL within the database
     *
     * <p>
     * Databases and Java both use the term NULL but for slightly different
     * meanings.
     *
     * <p>
     * This method indicates whether the field represented by this object is
     * NULL in the database sense.
     *
     * @return TRUE if this object represents a NULL database value, otherwise
     * FALSE
     */
    public boolean isNull() {
        return isDBNull || literalValue == null;
    }

    /**
     * Returns the previous value of this field as an SQL formatted String.
     *
     * <p>
     * Used by {@link DBActionList} to generate
     * {@link DBActionList#getRevertActionList() revert action lists}.
     *
     * @param db
     * @return the previous value of this field as an SQL formatted String
     */
    public String getPreviousSQLValue(DBDatabase db) {
        return (previousValueAsQDT == null) ? null : previousValueAsQDT.toSQLString(db);
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

    /**
     * @return the undefined
     */
    protected boolean isDefined() {
        return !undefined;
    }

    /**
     * @param defined the undefined to set
     */
    protected void setDefined(boolean defined) {
        this.undefined = !defined;
    }

    /**
     * Sets the internal reference the property wrapper of the field or bean
     * property that references this QueryableDatatype. Supports QDT types that
     * need extra meta-information, such as the {@code DBEnum} type.
     *
     * <p>
     * Called by the property wrapper itself when it gets or sets the field, so
     * this QDT's reference to its owning field is populated 99% of the time.
     *
     * <p>
     * Can't be called directly, must be called via
     * {@link InternalQueryableDatatypeProxy}.
     *
     * <p>
     * <i>Thread-safety: relatively safe, as PropertyWrappers are thread-safe
     * and interchangeable.
     *
     * @param propertyWrapper
     */
    void setPropertyWrapper(PropertyWrapperDefinition propertyWrapper) {
        this.propertyWrapper = propertyWrapper;
    }
}
