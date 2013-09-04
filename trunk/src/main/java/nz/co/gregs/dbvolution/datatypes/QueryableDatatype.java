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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.operators.*;

/**
 *
 * @author gregory.graham
 */
public abstract class QueryableDatatype extends Object implements Serializable {

    public static final long serialVersionUID = 1L;
    protected transient DBDatabase database = null;
    protected Object literalValue = null;
    protected boolean isDBNull = false;
    protected boolean includingNulls = false;
    private boolean invertOperator;
    private DBOperator operator = null;
    private boolean undefined = true;
    private boolean changed = false;
    private QueryableDatatype previousValueAsQDT = null;
    private boolean isPrimaryKey;
    public final static Boolean SORT_ASCENDING = Boolean.TRUE;
    public final static Boolean SORT_DESCENDING = Boolean.FALSE;
    private Boolean sort = SORT_ASCENDING;

    QueryableDatatype() {
    }

    QueryableDatatype(String str) {
        if (str == null) {
            this.isDBNull = true;
        } else if (!str.isEmpty()) {
            this.literalValue = str;
            this.operator = new DBEqualsOperator(this);
        }
    }

    QueryableDatatype(Object str) {
        if (str == null) {
            this.isDBNull = true;
        } else if (!str.toString().isEmpty()) {
            this.literalValue = str;
            this.operator = new DBEqualsOperator(this);
        }
    }

    @Override
    public String toString() {
        return (literalValue == null ? "" : literalValue.toString());
    }

    public Long longValue() {
        return (literalValue == null ? null : Long.parseLong(literalValue.toString()));
    }

    public Integer intValue() {
        return (literalValue == null ? null : Integer.parseInt(literalValue.toString()));
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

    static QueryableDatatype getQueryableDatatypeForObject(Object o) {
        if (o instanceof Integer) {
            return new DBInteger();
        } else if (o instanceof Number) {
            return new DBNumber();
        } else if (o instanceof String) {
            return new DBString();
        } else if (o instanceof Date) {
            return new DBDate();
        } else if (o instanceof Byte[]) {
            return new DBByteArray();
        } else if (o instanceof Boolean) {
            return new DBBoolean();
        } else {
            return new DBObject();
        }
    }

    /**
     *
     * @param columnName
     * @return
     */
    public String getWhereClause(String columnName) {
        return getWhereClauseUsingOperators(columnName);
    }

    private String getWhereClauseUsingOperators(String columnName) {
        String whereClause = "";
        DBOperator op = this.getOperator();
        if (op != null) {
            whereClause = op.generateWhereLine(database, columnName);
        }
        return whereClause;
    }

    public void negateOperator() {
        invertOperator = true;
        if (getOperator() != null) {
            getOperator().invertOperator(invertOperator);
        } else {
            throw new RuntimeException("No Operator Has Been Defined Yet: please use the query methods before inverting the operation");
        }
    }

    public void permittedValues(Object... permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.length == 1) {
            useEqualsOperator(permitted[0]);
        } else {
            useInOperator(permitted);
        }
    }

    /**
     *
     * @param permitted
     */
    public void permittedValues(Collection<Object> permitted) {
        if (permitted == null) {
            useNullOperator();
//        } else if (permitted.size() == 1) {
//            useEqualsOperator(permitted.get(0));
        } else {
            useInOperator(permitted.toArray());
        }
    }

    /**
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(String... permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.length == 1) {
            useEqualsCaseInsensitiveOperator(permitted[0]);
        } else {
            useInCaseInsensitiveOperator(permitted);
        }
    }

    /**
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(Collection<String> permitted) {
        if (permitted == null) {
            useNullOperator();
//        } else if (permitted.size() == 1) {
//            useEqualsCaseInsensitiveOperator(permitted.get(0));
        } else {
            useInCaseInsensitiveOperator(permitted.toArray(new String[]{}));
        }
    }

    /**
     *
     * @param permitted
     */
    public void excludedValuesIgnoreCase(String... permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.length == 1) {
            useEqualsCaseInsensitiveOperator(permitted[0]).not();
        } else {
            useInCaseInsensitiveOperator(permitted).not();
        }
    }

    /**
     *
     * @param permitted
     */
    public void excludedValuesIgnoreCase(Collection<String> permitted) {
        if (permitted == null) {
            useNullOperator();
//        } else if (permitted.size() == 1) {
//            useEqualsCaseInsensitiveOperator(permitted.get(0)).not();
        } else {
            useInCaseInsensitiveOperator(permitted.toArray(new String[]{})).not();
        }
    }

    /**
     *
     * @param permitted
     */
    @Deprecated
    public void permittedValuesCaseInsensitive(String... permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.length == 1) {
            useEqualsCaseInsensitiveOperator(permitted[0]);
        } else {
            useInCaseInsensitiveOperator(permitted);
        }
    }

    /**
     *
     * @param permitted
     */
    public void excludedValues(Object... permitted) {
        if (permitted == null) {
            useNullOperator().not();
        } else if (permitted.length == 1) {
            useEqualsOperator(permitted[0]).not();
        } else {
            useInOperator(permitted).not();
        }
    }

    public void excludedValues(Collection<Object> permitted) {
        if (permitted == null) {
            useNullOperator().not();
//        } else if (permitted.size() == 1) {
//            useEqualsOperator(permitted.get(0)).not();
        } else {
            useInOperator(permitted.toArray()).not();
        }
    }

    public void permittedRange(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound);
        } else if (lowerBound == null && upperBound != null) {
            QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOperator(qdt);
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOperator(qdt);
        }
    }

    public void permittedRangeInclusive(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound);
        } else if (lowerBound == null && upperBound != null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOrEqualToOperator(qdt);
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOrEqualToOperator(qdt);
        }
    }

    public void excludedRange(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound).not();
        } else if (lowerBound == null && upperBound != null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOperator(qdt).not();
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOperator(qdt).not();
        }
    }

    public void excludedRangeInclusive(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound).not();
        } else if (lowerBound == null && upperBound != null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOrEqualToOperator(qdt).not();
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOrEqualToOperator(qdt).not();
        }
    }

    public void permittedPattern(String pattern) {
        useLikeOperator(pattern);
    }

    public void excludedPattern(String pattern) {
        useLikeOperator(pattern).not();
    }

    /**
     *
     * @param newLiteralValue the literalValue to set
     */
    public void setValue(Object newLiteralValue) {
        useEqualsOperator(newLiteralValue);
    }

    protected DBOperator useEqualsOperator(Object newLiteralValue) {
        preventChangeOfPrimaryKey();
        blankQuery();
        if (newLiteralValue == null) {
            return useNullOperator();
        } else {
            if (newLiteralValue instanceof Date) {
                setChanged((Date) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDate((Date) newLiteralValue)));
            } else if (newLiteralValue instanceof Timestamp) {
                setChanged((Timestamp) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDate((Timestamp) newLiteralValue)));
            } else {
                setChanged(newLiteralValue);
                this.literalValue = newLiteralValue.toString();
                this.setOperator(new DBEqualsOperator(this));
            }
        }
        return getOperator();
    }

    protected DBOperator useEqualsCaseInsensitiveOperator(String newLiteralValue) {
        preventChangeOfPrimaryKey();
        blankQuery();
        if (newLiteralValue == null) {
            return useNullOperator();
        } else {
            setChanged(newLiteralValue);
            this.literalValue = newLiteralValue.toString();
            this.setOperator(new DBEqualsCaseInsensitiveOperator(this));
        }
        return getOperator();
    }

    public void setUnchanged() {
        changed = false;
        previousValueAsQDT = null;
    }

    protected DBOperator useGreaterThanOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOperator(literalValue));
        }
        return getOperator();
    }

    protected DBOperator useGreaterThanOrEqualToOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOrEqualsOperator(literalValue));
        }
        return getOperator();
    }

    protected DBOperator useLessThanOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOperator(literalValue));
        }
        return getOperator();
    }

    protected DBOperator useLessThanOrEqualToOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOrEqualOperator(literalValue));
        }
        return getOperator();
    }

    /**
     *
     * Sets the value of this column to DBNull Also changes the operator to
     * DBIsNullOperator for comparisons
     *
     */
    protected final DBOperator useNullOperator() {
        blankQuery();
        this.literalValue = null;
        this.isDBNull = true;
        this.setOperator(new DBIsNullOperator());
        return getOperator();
    }

    protected DBOperator useLikeOperator(Object t) {
        blankQuery();
        this.literalValue = t;
        this.setOperator(new DBLikeCaseInsensitiveOperator(this));
        return getOperator();
    }

    /**
     * TODO
     */
    public void includingNulls() {
        this.includingNulls = true;
    }

    /**
     * Converts the objects to QueryableDatatypes and calls
     * isIn(QueryableDatatype... inValues) with them
     *
     * @param inValues the inValues to set
     */
    protected DBOperator useInOperator(Object... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> inVals = new ArrayList<QueryableDatatype>();
        for (Object obj : inValues) {
            QueryableDatatype qdt = getQueryableDatatypeForObject(obj);
            qdt.setValue(obj);
            inVals.add(qdt);
        }
        this.setOperator(new DBInOperator(inVals));
        return getOperator();
    }

    protected DBOperator useInCaseInsensitiveOperator(String... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> inVals = new ArrayList<QueryableDatatype>();
        for (Object obj : inValues) {
            QueryableDatatype qdt = getQueryableDatatypeForObject(obj);
            qdt.setValue(obj);
            inVals.add(qdt);
        }
        this.setOperator(new DBInCaseInsensitiveOperator(inVals));
        return getOperator();
    }

    /**
     *
     * @param inValues
     */
    protected DBOperator useInOperator(QueryableDatatype... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> arrayList = new ArrayList<QueryableDatatype>();
        arrayList.addAll(Arrays.asList(inValues));
        this.setOperator(new DBInOperator(arrayList));
        return getOperator();
    }

    /**
     * @param lowerBound the lower bound to set
     * @param upperBound the upper bound to set
     */
    protected DBOperator useBetweenOperator(QueryableDatatype lowerBound, QueryableDatatype upperBound) {
        blankQuery();
        this.setOperator(new DBBetweenOperator(lowerBound, upperBound));
        return getOperator();
    }

    protected DBOperator useBetweenOperator(Object lowerBound, Object upperBound) {
        blankQuery();
        QueryableDatatype lower = getQueryableDatatypeForObject(lowerBound);
        lower.setValue(lowerBound);
        QueryableDatatype upper = getQueryableDatatypeForObject(upperBound);
        upper.setValue(upperBound);
        useBetweenOperator(lower, upper);
        return getOperator();
    }

    /**
     *
     * @return the literal value as it would appear in an SQL statement i.e.
     * {yada} => 'yada'
     */
    public String toSQLString() {
        DBDefinition def = database.getDefinition();
        if (this.isDBNull || literalValue == null) {
            return def.getNull();
        }
        return def.beginStringValue() + this.toString().replace("'", "\'") + def.endStringValue();
    }

    /**
     * @return the database
     */
    protected DBDatabase getDatabase() {
        return database;
    }

    /**
     * @param database the database to set
     */
    public void setDatabase(DBDatabase database) {
        this.database = database;
    }

    /**
     *
     * Provides the SQL datatype used by default for this type of object
     *
     * This should be overridden in each subclass
     *
     * @return
     */
    public abstract String getSQLDatatype();
//    {
//        return "VARCHAR(1000)";
//    }

    /**
     *
     * Returns the value of the object formatted for the database
     *
     * This should be overridden in each subclass
     *
     * @return
     */
    public abstract String getSQLValue();
//    {
//        if (this.isDBNull) {
//            return database.getNull();
//        } else {
//            if (literalValue instanceof Date) {
//                return database.getDateFormattedForQuery((Date) literalValue);
//            } else {
//                String unsafeValue = literalValue.toString();
//                return database.beginStringValue() + database.safeString(unsafeValue) + database.endStringValue();
//            }
//        }
//    }

    /**
     * @return the operator
     */
    public DBOperator getOperator() {
        return operator;
    }

    /**
     * @param operator the operator to set
     */
    public void setOperator(DBOperator operator) {
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
     * @param fullColumnName
     * @throws SQLException
     */
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.useNullOperator();
        } else {
            String dbValue;
            try {
                dbValue = resultSet.getString(fullColumnName);
            } catch (SQLException ex) {
                dbValue = null;
            }
            if (dbValue == null) {
                this.useNullOperator();
            } else {
                this.useEqualsOperator(dbValue);
            }
        }
    }

    void setIsPrimaryKey(boolean b) {
        this.isPrimaryKey = b;
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
                newInstance.useNullOperator();
            } else {
                newInstance.useEqualsOperator(this.literalValue);
            }
            previousValueAsQDT = newInstance;
        }
    }

    public boolean isNull() {
        return isDBNull || literalValue == null;
    }

    public String getPreviousSQLValue() {
        previousValueAsQDT.setDatabase(database);
        return previousValueAsQDT.getSQLValue();
    }

    public QueryableDatatype setSortOrder(Boolean order) {
        sort = order;
        return this;
    }

    public Boolean getSortOrder() {
        return sort;
    }

    public void clear() {
        blankQuery();
    }
}
