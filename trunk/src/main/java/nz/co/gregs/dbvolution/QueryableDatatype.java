/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.operators.*;

/**
 *
 * @author gregory.graham
 */
public class QueryableDatatype extends Object implements Serializable {

    public static final long serialVersionUID = 1L;
    protected DBDatabase database = null;
    protected Object literalValue = null;
    protected boolean isDBNull = false;
    protected boolean includingNulls = false;
    private boolean invertOperator;
    private DBOperator operator = null;
    private boolean changed = false;

    QueryableDatatype() {
    }

    QueryableDatatype(String str) {
        if (str == null) {
            this.isDBNull = true;
        }
        this.literalValue = str;
    }

    QueryableDatatype(Object str) {
        if (str == null) {
            this.isDBNull = true;
        }
        this.literalValue = str;
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
        this.setOperator(null);
    }

    /**
     *
     * @param columnName
     * @return
     */
    public String getWhereClause(String columnName) {
        return getWhereClauseUsingOperators(columnName);
    }

    public String getWhereClauseUsingOperators(String columnName) {
        StringBuilder whereClause = new StringBuilder();
        DBOperator op = this.getOperator();
        if (op != null) {
            whereClause.append(op.generateWhereLine(database, columnName));
        }
        return whereClause.toString();
    }

    public void invertOperator() {
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
    public void isLiterally(Object newLiteralValue) {
        blankQuery();
        if (newLiteralValue == null) {
            isNull();
        } else {
            if (this.isDBNull
                    || (literalValue != null && !newLiteralValue.equals(literalValue))) {
                changed = true;
            }
            this.literalValue = newLiteralValue.toString();
            this.setOperator(new DBEqualsOperator(new QueryableDatatype(newLiteralValue.toString())));
        }
    }

    public void setUnchanged() {
        changed = false;
    }

//    @Deprecated
//    public void isLiterally(QueryableDatatype literalValue) {
//        blankQuery();
//        if (literalValue == null) {
//            isNull();
//        } else {
//            this.literalValue = literalValue.literalValue;
//            this.setOperator(new DBEqualsOperator(literalValue));
//        }
//    }
    public void isGreaterThan(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOperator(literalValue));
        }
    }

    public void isGreaterThanOrEqualTo(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOrEqualsOperator(literalValue));
        }
    }

    public void isLessThan(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOperator(literalValue));
        }
    }

    public void isLessThanOrEqualTo(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOrEqualOperator(literalValue));
        }
    }

    public final void isNull() {
        this.setOperator(new DBIsNullOperator());
    }

    public void isLike(Object t) {
        this.literalValue = t;
        this.setOperator(new DBLikeOperator(new QueryableDatatype(t.toString())));
    }

    /**
     * NEEDS TO BE IMPLEMENTED PROPERLY
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
    public void isIn(Object... inValues) {
        ArrayList<QueryableDatatype> inVals = new ArrayList<QueryableDatatype>();
        for (Object obj : inValues) {
            inVals.add(new QueryableDatatype(obj));
        }
        this.setOperator(new DBInOperator(inVals));
    }

    /**
     *
     * @param inValues
     */
    public void isIn(QueryableDatatype... inValues) {
        ArrayList<QueryableDatatype> arrayList = new ArrayList<QueryableDatatype>();
        boolean addAll = arrayList.addAll(Arrays.asList(inValues));
        this.setOperator(new DBInOperator(arrayList));
    }

    /**
     * @param lowerBound the lower bound to set
     * @param upperBound the upper bound to set
     */
    public void isBetween(QueryableDatatype lowerBound, QueryableDatatype upperBound) {
        this.setOperator(new DBBetweenOperator(lowerBound, upperBound));
    }

    public void isBetween(Object lowerBound, Object upperBound) {
        isBetween(new QueryableDatatype(lowerBound), new QueryableDatatype(upperBound));
    }

    /**
     *
     * @return the literal value as it would appear in an SQL statement i.e.
     * {yada} => 'yada'
     */
    protected String toSQLString() {
        if (this.isDBNull) {
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

    public String getSQLDatatype() {
        return "VARCHAR(1000)";
    }

    public String getSQLValue() {
        String unsafeValue = literalValue.toString();
        return database.beginStringValue() + database.safeString(unsafeValue) + database.endStringValue();
    }

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
    }

    boolean hasChanged() {
        return changed;
    }
}
