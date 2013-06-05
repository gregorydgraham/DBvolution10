/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregory.graham
 */
public class QueryableDatatype extends Object implements Serializable {

    public static final long serialVersionUID = 1L;
    protected DBDatabase database = null;
    protected Object literalValue = null;
    protected boolean isDBNull = false;
    protected boolean usingLiteralComparison = false;
    protected boolean usingNullComparison = false;
    protected boolean usingLikeComparison = false;
    protected boolean usingInComparison = false;
    protected boolean usingBetweenComparison = false;
    protected boolean includingNulls = false;
    protected QueryableDatatype[] inValues = new QueryableDatatype[]{};
    protected QueryableDatatype lowerBound = null;
    protected QueryableDatatype upperBound = null;

    QueryableDatatype() {
    }

    QueryableDatatype(String str) {
        if (str == null) {
            this.isDBNull = true;
        }
        this.literalValue = str;
        this.usingLiteralComparison = true;
    }

    QueryableDatatype(Object str) {
        if (str == null) {
            this.isDBNull = true;
        }
        this.literalValue = str;
        this.usingLiteralComparison = true;
    }

    @Override
    public String toString() {
        return (literalValue == null ? "" : literalValue.toString());
    }

    protected void blankQuery() {
        usingLiteralComparison = false;
        usingLikeComparison = false;
        usingInComparison = false;
        usingBetweenComparison = false;
        includingNulls = false;
        this.inValues = new QueryableDatatype[]{};
        this.lowerBound = null;
        this.upperBound = null;
    }

    /**
     *
     * @param columnName
     * @return
     */
    public String getWhereClause(String columnName) {
        StringBuilder whereClause = new StringBuilder();
        if (usingLiteralComparison) {
            whereClause.append(" and ").append(columnName).append(" = ").append(this.getSQLValue()).append(" ");
        } else if (usingNullComparison) {
            whereClause.append(" and ").append(columnName).append(" is null ");
        } else if (usingLikeComparison) {
            whereClause.append(" and ").append(columnName).append(" like ").append(this.getSQLValue()).append(" ");
        } else if (usingBetweenComparison) {
            whereClause.append(" and ").append(columnName).append(" between ").append(this.getLowerBound().getSQLValue()).append(" and ").append(this.getUpperBound().getSQLValue()).append(" ");
        } else if (usingInComparison) {
            whereClause.append(" and ").append(columnName).append(" in (");
            String sep = "";
            for (QueryableDatatype qdt : inValues) {
                whereClause.append(sep).append(" ").append(qdt.getSQLValue()).append(" ");
                sep = ",";
            }
            whereClause.append(") ");
        }
        return whereClause.toString();
    }

    /**
     * @param literalValue the literalValue to set
     */
    public void isLiterally(Object literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            usingLiteralComparison = true;
            this.literalValue = literalValue.toString();
        }
    }

    public void isLiterally(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            isNull();
        } else {
            usingLiteralComparison = true;
            this.literalValue = literalValue.literalValue;
        }
    }

    public final void isNull() {
        this.usingNullComparison = true;
    }

    public void isLike(Object t) {
        this.usingLikeComparison = true;
        this.literalValue = t;
    }

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
        this.isIn(inVals.toArray(this.inValues));
    }

    /**
     *
     * @param inValues
     */
    public void isIn(QueryableDatatype[] inValues) {
        this.usingInComparison = true;
        this.inValues = inValues;
    }

    /**
     * @param lowerBound the lower bound to set
     * @param upperBound the upper bound to set 
     */
    public void isBetween(QueryableDatatype lowerBound, QueryableDatatype upperBound) {
        this.usingBetweenComparison = true;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public void isBetween(Object lowerBound, Object upperBound) {
        this.usingBetweenComparison = true;
        this.lowerBound = new QueryableDatatype(lowerBound);
        this.upperBound = new QueryableDatatype(upperBound);
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
    protected void setDatabase(DBDatabase database) {
        this.database = database;
        if (this.upperBound != null) {
            this.upperBound.setDatabase(database);
        }
        if (this.lowerBound != null) {
            this.lowerBound.setDatabase(database);
        }
        for (QueryableDatatype qdt : inValues) {
            if (qdt != null) {
                qdt.setDatabase(database);
            }
        }
    }

    protected QueryableDatatype getUpperBound() {
        return this.upperBound;
    }

    protected QueryableDatatype getLowerBound() {
        return this.lowerBound;
    }
    
    public String getSQLDatatype(){
        return "VARCHAR(1000)";
    }

    String getSQLValue() {
        return database.beginStringValue()+literalValue.toString()+database.endStringValue();
    }
}
