/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.operators.*;

/**
 *
 * @author gregory.graham
 */
public abstract class QueryableDatatype extends Object implements Serializable {

    public static final long serialVersionUID = 1L;
    protected DBDatabase database = null;
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

    public Double doubleValue() {
        return (literalValue == null ? null : Double.parseDouble(literalValue.toString()));
    }

    protected void blankQuery() {
        includingNulls = false;
        isDBNull = false;
        this.operator = null;
    }

    static <T extends QueryableDatatype> T getQueryableDatatype(Class<T> requiredQueryableDatatype) {
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

    /**
     * @param newLiteralValue the literalValue to set
     */
    public DBOperator isLiterally(Object newLiteralValue) {
        preventChangeOfPrimaryKey();
        blankQuery();
        if (newLiteralValue == null) {
            return isNull();
        } else {
            setChanged(newLiteralValue);
            this.literalValue = newLiteralValue.toString();
            this.setOperator(new DBEqualsOperator(this));
        }
        return getOperator();
    }

    /**
     * @param newLiteralValue the literalValue to set
     */
    public DBOperator isLiterally(Date newLiteralValue) {
        preventChangeOfPrimaryKey();
        blankQuery();
        if (newLiteralValue == null) {
            isNull();
        } else {
            setChanged(newLiteralValue);
            this.literalValue = newLiteralValue;
            this.setOperator(new DBEqualsOperator(new DBDate(newLiteralValue)));
        }
        return getOperator();
    }

    /**
     * @param newLiteralValue the literalValue to set
     */
    public DBOperator isLiterally(Timestamp newLiteralValue) {
        preventChangeOfPrimaryKey();
        blankQuery();
        if (newLiteralValue == null) {
            isNull();
        } else {
            setChanged(newLiteralValue);
            this.literalValue = newLiteralValue;
            this.setOperator(new DBEqualsOperator(new DBDate(newLiteralValue)));
        }
        return getOperator();
    }

    public void setUnchanged() {
        changed = false;
        previousValueAsQDT = null;
    }

    public DBOperator isGreaterThan(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOperator(literalValue));
        }
        return getOperator();
    }

    public DBOperator isGreaterThanOrEqualTo(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOrEqualsOperator(literalValue));
        }
        return getOperator();
    }

    public DBOperator isLessThan(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOperator(literalValue));
        }
        return getOperator();
    }

    public DBOperator isLessThanOrEqualTo(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
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
    public final DBOperator isNull() {
        blankQuery();
        this.literalValue = null;
        this.isDBNull = true;
        this.setOperator(new DBIsNullOperator());
        return getOperator();
    }

    public DBOperator isLike(Object t) {
        blankQuery();
        this.literalValue = t;
        this.setOperator(new DBLikeOperator(this));
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
    public DBOperator isIn(Object... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> inVals = new ArrayList<QueryableDatatype>();
        for (Object obj : inValues) {
            QueryableDatatype qdt = getQueryableDatatypeForObject(obj);
            qdt.isLiterally(obj);
            inVals.add(qdt);
        }
        this.setOperator(new DBInOperator(inVals));
        return getOperator();
    }

    /**
     *
     * @param inValues
     */
    public DBOperator isIn(QueryableDatatype... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> arrayList = new ArrayList<QueryableDatatype>();
        boolean addAll = arrayList.addAll(Arrays.asList(inValues));
        this.setOperator(new DBInOperator(arrayList));
        return getOperator();
    }

    /**
     * @param lowerBound the lower bound to set
     * @param upperBound the upper bound to set
     */
    public DBOperator isBetween(QueryableDatatype lowerBound, QueryableDatatype upperBound) {
        blankQuery();
        this.setOperator(new DBBetweenOperator(lowerBound, upperBound));
        return getOperator();
    }

    public DBOperator isBetween(Object lowerBound, Object upperBound) {
        blankQuery();
        QueryableDatatype lower = getQueryableDatatypeForObject(lowerBound);
        QueryableDatatype upper = getQueryableDatatypeForObject(upperBound);
        isBetween(lower, upper);
        return getOperator();
    }

    /**
     *
     * @return the literal value as it would appear in an SQL statement i.e.
     * {yada} => 'yada'
     */
    protected String toSQLString() {
        if (this.isDBNull || literalValue == null) {
            return database.getNull();
        }
        return database.beginStringValue() + this.toString().replace("'", "\'") + database.endStringValue();
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

    boolean hasChanged() {
        return changed;
    }

    /**
     *
     * @param resultSet
     * @param fullColumnName
     * @throws SQLException
     */
    protected void setFromResultSet(ResultSet resultSet, String fullColumnName) throws SQLException {
        this.isLiterally(resultSet.getString(fullColumnName));
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
            QueryableDatatype newInstance = QueryableDatatype.getQueryableDatatype(this.getClass());
            if (this.isDBNull) {
                newInstance.isNull();
            } else {
                newInstance.isLiterally(this.literalValue);
            }
            previousValueAsQDT = newInstance;
        }
    }

    String getPreviousValueAsSQL() {
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
}
