/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.superconductor;

import java.util.ArrayList;

/**
 *
 * @author gregory.graham
 */
public class DBNumber extends QueryableDatatype {

    protected Number numberValue = null;
    protected Number lowerBoundNumber = null;
    protected Number upperBoundNumber = null;
    protected Number[] inValuesNumber = new Number[]{};

    DBNumber(String aLong) {
        this(Double.parseDouble(aLong));
    }

    DBNumber(Number aNumber) {
        if (aNumber == null) {
            numberValue = 0L;
            this.usingNullComparison = true;
        } else {
            super.isLiterally(aNumber.toString());
            numberValue = aNumber;
        }
    }

    public DBNumber() {
        super();
    }

    @Override
    public String toString() {
        return (numberValue == null ? null : numberValue.toString());
    }

    @Override
    public void blankQuery() {
        super.blankQuery();
        this.numberValue = null;
        this.lowerBoundNumber = null;
        this.upperBoundNumber = null;
        this.inValuesNumber = new Long[]{};
    }

    @Override
    public void isIn(Object[] literalOptions) {
        ArrayList<Integer> intOptions = new ArrayList<Integer>();
        for (Object str : literalOptions) {
            intOptions.add(Integer.parseInt(str.toString()));
        }
        isIn(intOptions.toArray());
    }

    @Override
    public String getWhereClause(String columnName) {
        StringBuilder whereClause = new StringBuilder();
        if (this.usingLiteralComparison) {
            whereClause.append(" and ").append(columnName).append(" = ").append(this.toString()).append(" ");
        } else if (this.usingLikeComparison) {
            throw new RuntimeException("NUMBER COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else if (this.usingBetweenComparison) {
            whereClause.append(" and ").append(columnName).append(" between ").append(this.lowerBoundNumber).append(" and ").append(this.upperBoundNumber).append(" ");
        } else if (this.usingInComparison) {
            whereClause.append(" and ").append(columnName).append(" in (");
            String sep = "";
            for (Number val : this.inValuesNumber) {
                whereClause.append(" and ").append(val).append(sep).append(" ");
                sep = ",";
            }
            whereClause.append(") ");
        } else {
            return super.getWhereClause(columnName);
        }
        return whereClause.toString();
    }

    public void isIn(Number[] inValues) {
        super.isIn(inValues);
        this.inValuesNumber = inValues;
    }

    @Override
    public void isBetween(Object lower, Object upper) {
        this.upperBoundNumber = Double.parseDouble(upper.toString());
        this.lowerBoundNumber = Double.parseDouble(lower.toString());
        super.isBetween(lower, upper);
    }
    
    public void isBetween(Number lower, Number upper) {
        super.isBetween(lower.toString(), upper.toString());
        this.upperBoundNumber = upper;
        this.lowerBoundNumber = lower;
    }
    
    @Override
    public void isLiterally(Object literal){
        this.isLiterally(Double.parseDouble(literal.toString()));
    }

    public void isLiterally(Number literal) {
        super.isLiterally(literal);
        this.numberValue = literal;
    }
    
    @Override
    public void isLike(Object obj){
        throw new RuntimeException("LIKE Comparison Cannot Be Used With Numeric Fields: "+obj);
    }
}
