/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.util.ArrayList;

/**
 *
 * @author gregory.graham
 */
public class DBNumber extends QueryableDatatype {

    protected Number numberValue = null;
    protected DBNumber lowerBoundNumber = null;
    protected DBNumber upperBoundNumber = null;
    protected DBNumber[] inValuesNumber = new DBNumber[]{};

    public DBNumber(Object aNumber) {
        this(Double.parseDouble(aNumber.toString()));
    }

    public DBNumber(String aNumber) {
        this(Double.parseDouble(aNumber));
    }

    public DBNumber(Number aNumber) {
        if (aNumber == null) {
            numberValue = 0L;
            this.usingNullComparison = true;
        } else {
            super.isLiterally(aNumber);
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
        this.inValuesNumber = new DBNumber[]{};
    }

    @Override
    public void isIn(Object[] literalOptions) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Object str : literalOptions) {
            intOptions.add(new DBNumber(str));
        }
        isIn(intOptions.toArray(this.inValuesNumber));
    }

    public void isIn(DBNumber[] inValues) {
        super.isIn(inValues);
        this.inValuesNumber = inValues;
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
            for (DBNumber val : this.inValuesNumber) {
                whereClause.append(" and ").append(val.toSQLString()).append(sep).append(" ");
                sep = ",";
            }
            whereClause.append(") ");
        } else {
            return super.getWhereClause(columnName);
        }
        return whereClause.toString();
    }

    @Override
    public void isBetween(Object lower, Object upper) {
        this.upperBoundNumber = new DBNumber(upper);
        this.lowerBoundNumber = new DBNumber(lower);
        super.isBetween(lowerBoundNumber, upperBoundNumber);
    }

    public void isBetween(Number lower, Number upper) {
        this.upperBoundNumber = new DBNumber(upper);
        this.lowerBoundNumber = new DBNumber(lower);
        super.isBetween(lowerBoundNumber, upperBoundNumber);
    }

    @Override
    public void isLiterally(Object literal) {
        this.isLiterally(Double.parseDouble(literal.toString()));
    }

    @Override
    public void isLiterally(QueryableDatatype literalValue) {
        if (literalValue instanceof DBNumber) {
            this.isLiterally(((DBNumber)literalValue).numberValue);
        } else {
            super.isLiterally(literalValue);
        }
    }

    public void isLiterally(Number literal) {
        super.isLiterally(literal);
        this.numberValue = literal;
    }

    @Override
    public void isLike(Object obj) {
        throw new RuntimeException("LIKE Comparison Cannot Be Used With Numeric Fields: " + obj);
    }

    /**
     *
     * @return
     */
    @Override
    public String getSQLDatatype() {
        return "NUMERIC(15,5)";
    }

    /**
     *
     * @return the literal value as it would appear in an SQL statement i.e.
     * {123} => 123
     *
     */
    @Override
    protected String toSQLString() {
        return this.numberValue == null ? "NULL" : this.numberValue.toString();
    }

    @Override
    String getSQLValue() {
        return database.beginNumberValue() + numberValue.toString() + database.endNumberValue();
    }
    
    public Double doubleValue(){
        return numberValue.doubleValue();
    }
    
    public Long longValue(){
        return numberValue.longValue();
    }
    
    public Integer intValue(){
        return numberValue.intValue();
    }
}
