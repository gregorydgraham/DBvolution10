/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.io.Serializable;

/**
 *
 * @author gregory.graham
 */
public class QueryableDatatype extends Object implements Serializable {

    public static long serialVersionUID = 1L;
    protected Object literalValue = null;
    protected boolean usingLiteralComparison = false;
    protected boolean usingNullComparison = false;
    protected boolean usingLikeComparison = false;
    protected boolean usingInComparison = false;
    protected boolean usingBetweenComparison = false;
    protected boolean includingNulls = false;
    protected Object[] inValues = new Object[]{};
    protected Object lowerBound = null;
    protected Object upperBound = null;

    QueryableDatatype() {
    }

    QueryableDatatype(String str) {
        this.literalValue = str;
        this.usingLiteralComparison = true;
    }

    @Override
    public String toString() {
        return literalValue.toString();
    }

    protected void blankQuery() {
        usingLiteralComparison = false;
        usingLikeComparison = false;
        usingInComparison = false;
        usingBetweenComparison = false;
        includingNulls = false;
        this.inValues = new String[]{};
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
            whereClause.append(" and ").append(columnName).append(" = '").append(this.toString()).append("' ");
        } else if (usingNullComparison) {
            whereClause.append(" and ").append(columnName).append(" is null ");
        } else if (usingLikeComparison) {
            whereClause.append(" and ").append(columnName).append(" like '").append(this.toString()).append("' ");
        } else if (usingBetweenComparison) {
            whereClause.append(" and ").append(columnName).append(" between '").append(this.lowerBound).append("' and '").append(this.upperBound).append("' ");
        } else if (usingInComparison) {
            whereClause.append(" and ").append(columnName).append(" in (");
            String sep = "";
            for (Object str : inValues) {
                whereClause.append(" '").append(str.toString()).append("'").append(sep).append(" ");
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

    /**
     * @param usingNullComparison the usingNullComparison to set
     */
    public final void isNull() {
        this.usingNullComparison = true;
    }

    public void isLike(Object t) {
        this.usingLikeComparison = true;
        this.literalValue = t;
    }

    /**
     * @param includingNulls the includingNulls to set
     */
    public void includingNulls() {
        this.includingNulls = true;
    }

    /**
     * @param inValues the inValues to set
     */
    public void isIn(Object[] inValues) {
        this.usingInComparison = true;
        this.inValues = inValues;
    }

    /**
     * @param lowerBound the lowerBound to set
     */
    public void isBetween(Object lowerBound, Object upperBound) {
        this.usingBetweenComparison = true;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }
}
