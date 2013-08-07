/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.operators.DBLikeOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
public class DBNumber extends QueryableDatatype {

    protected Number numberValue = null;
    protected DBNumber lowerBoundNumber = null;
    protected DBNumber upperBoundNumber = null;
    protected DBNumber[] inValuesNumber = new DBNumber[]{};

    public DBNumber() {
        super();
    }

    /**
     *
     * @param aNumber
     */
    public DBNumber(Number aNumber) {
        super(aNumber);
        if (aNumber == null) {
            numberValue = 0L;
//            this.isDBNull = true;
//            this.usingNullComparison = true;
        } else {
//            super.isLiterally(aNumber);
            numberValue = aNumber;
        }
    }

    /**
     *
     * @param aNumber
     */
    public DBNumber(Object aNumber) {
        this(aNumber.toString());
    }

    /**
     *
     * @param aNumber
     */
    public DBNumber(String aNumber) {
        this(Double.parseDouble(aNumber));
    }

    @Override
    public String toString() {
        return (numberValue == null ? null : numberValue.toString());
    }

    /**
     *
     */
    @Override
    public void blankQuery() {
        super.blankQuery();
        this.numberValue = null;
        this.lowerBoundNumber = null;
        this.upperBoundNumber = null;
        this.inValuesNumber = new DBNumber[]{};
    }

    @Override
    public DBOperator isIn(Object... literalOptions) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Object str : literalOptions) {
            intOptions.add(new DBNumber(str));
        }
        return isIn(intOptions.toArray(this.inValuesNumber));
    }

    /**
     *
     * @param inValues
     */
    public void isIn(Number... inValues) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Number num : inValues) {
            intOptions.add(new DBNumber(num));
        }
        isIn(intOptions.toArray(this.inValuesNumber));
    }

    /**
     *
     * @param inValues
     */
    public void isIn(List<Number> inValues) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Number num : inValues) {
            intOptions.add(new DBNumber(num));
        }
        isIn(intOptions.toArray(this.inValuesNumber));
    }

    /**
     *
     * @param inValues
     */
    public DBOperator isIn(DBNumber... inValues) {
        this.inValuesNumber = inValues;
        return super.isIn(inValues);
    }

    @Override
    public String getWhereClause(String columnName) {
        if (this.getOperator() instanceof DBLikeOperator) {
            throw new RuntimeException("NUMBER COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else {
            return super.getWhereClause(columnName);
        }
    }

    /**
     *
     * @param lower
     * @param upper
     */
    @Override
    public DBOperator isBetween(Object lower, Object upper) {
        this.upperBoundNumber = new DBNumber(upper);
        this.lowerBoundNumber = new DBNumber(lower);
        return super.isBetween(lowerBoundNumber, upperBoundNumber);
    }

    /**
     *
     * @param lower
     * @param upper
     */
    public void isBetween(Number lower, Number upper) {
        this.upperBoundNumber = new DBNumber(upper);
        this.lowerBoundNumber = new DBNumber(lower);
        super.isBetween(lowerBoundNumber, upperBoundNumber);
    }

    @Override
    public DBOperator isLiterally(Object literal) {
        if (literal == null||literal.toString().isEmpty()) {
            super.isLiterally(null);
            this.numberValue = null;
        } else {
            this.isLiterally(Double.parseDouble(literal.toString()));
        }
        return getOperator();
    }

//    @Deprecated
//    @Override
//    public void isLiterally(QueryableDatatype literalValue) {
//        if (literalValue instanceof DBNumber) {
//            this.isLiterally(((DBNumber) literalValue).numberValue);
//        } else {
//            super.isLiterally(literalValue);
//        }
//    }
    /**
     *
     * @param literal
     */
    public void isLiterally(Number literal) {
        super.isLiterally(literal);
        this.numberValue = literal;
    }

    /**
     *
     * @param obj
     */
    @Override
    public DBOperator isLike(Object obj) {
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
        if (this.isDBNull || this.numberValue == null) {
            return database.getNull();
        }
        return this.numberValue.toString();
    }

    /**
     *
     * @return
     */
    @Override
    public String getSQLValue() {
        return database.beginNumberValue() + numberValue.toString() + database.endNumberValue();
    }

    /**
     *
     * @return
     */
    @Override
    public Double doubleValue() {
        return numberValue == null ? null : numberValue.doubleValue();
    }

    /**
     *
     * @return
     */
    @Override
    public Long longValue() {
        return numberValue == null ? null : numberValue.longValue();
    }

    /**
     *
     * @return
     */
    public Integer intValue() {
        return numberValue == null ? null : numberValue.intValue();
    }

    /**
     *
     * @param resultSet
     * @param fullColumnName
     * @throws SQLException
     */
    @Override
    protected void setFromResultSet(ResultSet resultSet, String fullColumnName) throws SQLException {
        this.isLiterally(resultSet.getBigDecimal(fullColumnName));
    }
}
