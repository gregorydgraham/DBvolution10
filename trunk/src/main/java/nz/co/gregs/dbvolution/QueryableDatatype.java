/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.io.Serializable;
import java.util.ArrayList;
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
//    protected boolean usingLiteralComparison = false;
//    protected boolean usingNullComparison = false;
//    protected boolean usingLikeComparison = false;
//    protected boolean usingInComparison = false;
//    protected boolean usingBetweenComparison = false;
    protected boolean includingNulls = false;
//    protected QueryableDatatype[] inValues = new QueryableDatatype[]{};
//    protected QueryableDatatype lowerBound = null;
//    protected QueryableDatatype upperBound = null;
    private boolean invertOperator;
    private DBOperator operator = null;
    
    QueryableDatatype() {
    }
    
    QueryableDatatype(String str) {
        if (str == null) {
            this.isDBNull = true;
        }
        this.literalValue = str;
//        this.usingLiteralComparison = true;
    }
    
    QueryableDatatype(Object str) {
        if (str == null) {
            this.isDBNull = true;
        }
        this.literalValue = str;
//        this.usingLiteralComparison = true;
    }
    
    @Override
    public String toString() {
        return (literalValue == null ? "" : literalValue.toString());
    }
    
    protected void blankQuery() {
//        usingLiteralComparison = false;
//        usingLikeComparison = false;
//        usingInComparison = false;
//        usingBetweenComparison = false;
        includingNulls = false;
//        this.inValues = new QueryableDatatype[]{};
//        this.lowerBound = null;
//        this.upperBound = null;
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
    
//    public String getWhereClauseUsingSwitch(String columnName) {
//        StringBuilder whereClause = new StringBuilder();
//        if (usingLiteralComparison) {
//            whereClause.append(" and ").append(columnName).append(invertOperator ? " <> " : " = ").append(this.getSQLValue()).append(" ");
//        } else if (usingNullComparison) {
//            whereClause.append(" and ").append(columnName).append(invertOperator ? " is not null " : " is null ");
//        } else if (usingLikeComparison) {
//            whereClause.append(" and ").append(columnName).append(invertOperator ? " not like " : " like ").append(this.getSQLValue()).append(" ");
//        } else if (usingBetweenComparison) {
//            whereClause.append(" and ").append(invertOperator ? " !(" : "(").append(columnName).append(" between ").append(this.getLowerBound().getSQLValue()).append(" and ").append(this.getUpperBound().getSQLValue()).append(") ");
//        } else if (usingInComparison) {
//            whereClause.append(" and ").append(columnName).append(invertOperator ? " not in " : " in (");
//            String sep = "";
//            for (QueryableDatatype qdt : inValues) {
//                whereClause.append(sep).append(" ").append(qdt.getSQLValue()).append(" ");
//                sep = ",";
//            }
//            whereClause.append(") ");
//        }
//        return whereClause.toString();
//    }
    
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
        }else{
            throw new RuntimeException("No Operator Has Been Defined Yet: please use the query methods before inverting the operation");
        }
    }

    /**
     * @param literalValue the literalValue to set
     */
    public void isLiterally(Object literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
//            usingLiteralComparison = true;
            this.literalValue = literalValue.toString();
            this.setOperator(new DBEquals(new QueryableDatatype(literalValue.toString())));
        }
    }
    
    public void isLiterally(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
//            usingLiteralComparison = true;
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBEquals(literalValue));
        }
    }
    
    public void isGreaterThan(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
//            usingLiteralComparison = true;
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOperator(literalValue));
        }
    }
    
    public void isGreaterThanOrEqualTo(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
//            usingLiteralComparison = true;
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOrEqualsOperator(literalValue));
        }
    }
    
    public void isLessThan(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
//            usingLiteralComparison = true;
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOperator(literalValue));
        }
    }
    
    public void isLessThanOrEqualTo(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
//            usingLiteralComparison = true;
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOrEqualOperator(literalValue));
        }
    }
    
    public final void isNull() {
//        this.usingNullComparison = true;
        this.setOperator(new DBIsNullOp());
    }
    
    public void isLike(Object t) {
//        this.usingLikeComparison = true;
        this.literalValue = t;
        this.setOperator(new DBLike(new QueryableDatatype(t.toString())));
    }

    /**
     * NEEDS TO BE IMPLEMENTED PROPERLY
     */
    public void includingNulls() {
        this.includingNulls = true;
    }

    /**
     * Converts the objects to QueryableDatatypes and calls
     * isIn(QueryableDatatype[] inValues) with them
     *
     * @param inValues the inValues to set
     */
    public void isIn(Object[] inValues) {
        ArrayList<QueryableDatatype> inVals = new ArrayList<QueryableDatatype>();
        for (Object obj : inValues) {
            inVals.add(new QueryableDatatype(obj));
        }
//        this.isIn(inVals.toArray(this.inValues));
        this.setOperator(new DBInOperator(inVals));
    }

    /**
     *
     * @param inValues
     */
    public void isIn(QueryableDatatype[] inValues) {
//        this.usingInComparison = true;
//        this.inValues = inValues;
        ArrayList<QueryableDatatype> arrayList = new ArrayList<QueryableDatatype>();
        for (QueryableDatatype qdt : inValues) {
            arrayList.add(qdt);
        }
        this.setOperator(new DBInOperator(arrayList));
    }

    /**
     * @param lowerBound the lower bound to set
     * @param upperBound the upper bound to set
     */
    public void isBetween(QueryableDatatype lowerBound, QueryableDatatype upperBound) {
//        this.usingBetweenComparison = true;
//        this.lowerBound = lowerBound;
//        this.upperBound = upperBound;
        this.setOperator(new DBBetweenOperator(lowerBound, upperBound));
    }
    
    public void isBetween(Object lowerBound, Object upperBound) {
//        this.usingBetweenComparison = true;
//        this.lowerBound = new QueryableDatatype(lowerBound);
//        this.upperBound = new QueryableDatatype(upperBound);
//        this.operator = new DBBetweenOperator(this.lowerBound, this.upperBound);
        isBetween(new QueryableDatatype(lowerBound), new QueryableDatatype(upperBound));
    }

    /**
     *
     * @return the literal value as it would appear in an SQL statement i.e.
     * {yada} => 'yada'
     */
    protected String toSQLString() {
        return "'" + this.toString().replace("'", "\'") + "'";
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
//        if (this.upperBound != null) {
//            this.upperBound.setDatabase(database);
//        }
//        if (this.lowerBound != null) {
//            this.lowerBound.setDatabase(database);
//        }
//        for (QueryableDatatype qdt : inValues) {
//            if (qdt != null) {
//                qdt.setDatabase(database);
//            }
//        }
    }
    
//    protected QueryableDatatype getUpperBound() {
//        return this.upperBound;
//    }
//    
//    protected QueryableDatatype getLowerBound() {
//        return this.lowerBound;
//    }
    
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
}
