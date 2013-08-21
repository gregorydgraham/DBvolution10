/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
public class DBNumber extends QueryableDatatype {

    public static final long serialVersionUID = 1;
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
        super(aNumber);
        initDBNumber(aNumber);
    }

    /**
     *
     * @param aNumber
     */
//    public DBNumber(String aNumber) {
//        this(Double.parseDouble(aNumber));
//    }
    private void initDBNumber(Object aNumber) {
        if (aNumber == null) {
            numberValue = null;
        } else {
            if (aNumber instanceof Number){
                numberValue = (Number)aNumber;
            }else{
                numberValue = Double.parseDouble(aNumber.toString());
            }            
        }
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
    public DBOperator useInOperator(Object... literalOptions) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Object str : literalOptions) {
            intOptions.add(new DBNumber(str));
        }
        return useInOperator(intOptions.toArray(this.inValuesNumber));
    }

    /**
     *
     * @param inValues
     */
    public void useInOperator(Number... inValues) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Number num : inValues) {
            intOptions.add(new DBNumber(num));
        }
        useInOperator(intOptions.toArray(this.inValuesNumber));
    }

    /**
     *
     * @param inValues
     */
    public void useInOperator(List<Number> inValues) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Number num : inValues) {
            intOptions.add(new DBNumber(num));
        }
        useInOperator(intOptions.toArray(this.inValuesNumber));
    }

    /**
     *
     * @param inValues
     */
    public DBOperator useInOperator(DBNumber... inValues) {
        this.inValuesNumber = inValues;
        return super.useInOperator(inValues);
    }

    public DBOperator useGreaterThanOperator(Number literalValue) {
        return this.useGreaterThanOperator(new DBNumber(literalValue));
    }

    @Override
    public String getWhereClause(String columnName) {
        if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
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
    public DBOperator useBetweenOperator(Object lower, Object upper) {
        this.upperBoundNumber = new DBNumber(upper);
        this.lowerBoundNumber = new DBNumber(lower);
        return super.useBetweenOperator(lowerBoundNumber, upperBoundNumber);
    }

    /**
     *
     * @param lower
     * @param upper
     */
    public void useBetweenOperator(Number lower, Number upper) {
        this.upperBoundNumber = new DBNumber(upper);
        this.lowerBoundNumber = new DBNumber(lower);
        super.useBetweenOperator(lowerBoundNumber, upperBoundNumber);
    }

    @Override
    public DBOperator useEqualsOperator(Object literal) {
        if (literal == null || literal.toString().isEmpty()) {
            super.useEqualsOperator(null);
            this.numberValue = null;
        } else {
            this.useEqualsOperator(Double.parseDouble(literal.toString()));
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
    public void useEqualsOperator(Number literal) {
        super.useEqualsOperator(literal);
        this.numberValue = literal;
    }

    /**
     *
     * @param obj
     */
    @Override
    public DBOperator useLikeOperator(Object obj) {
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
    protected void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.useNullOperator();
        } else {
            BigDecimal dbValue;
            try {
                dbValue = resultSet.getBigDecimal(fullColumnName);
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
}
