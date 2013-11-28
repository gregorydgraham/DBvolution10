/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
// FIXME (comment from Malcolm): I think the use of numberValue, lowerBoundNumber, upperBoundNumber, inValuesNumber
// in this class will cause bugs because they aren't settable via QueryableDatatype.
// eg: calls to QueryableDatatype.permittedValues(null) currently won't change any of the local fields
// because useNullOperator() isn't overridden. 
public class DBNumber extends QueryableDatatype {

    public static final long serialVersionUID = 1;
//    protected Number numberValue = null;
//    protected DBNumber lowerBoundNumber = null;
//    protected DBNumber upperBoundNumber = null;
//    protected DBNumber[] inValuesNumber = new DBNumber[]{};

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
//            numberValue = 0L;
//            this.isDBNull = true;
//            this.usingNullComparison = true;
        } else {
//            super.isLiterally(aNumber);
//            numberValue = aNumber;
        }
    }

    /**
     *
     * @param aNumber
     */
    public DBNumber(Object aNumber) {
        super(aNumber);
        if (!(aNumber instanceof Number)) {
            initDBNumber(aNumber);
        }
    }

    @Override
    public void setValue(Object newLiteralValue) {
        initDBNumber(newLiteralValue);
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
            super.setValue(null);
        } else {
            if (aNumber instanceof Number) {
                super.setValue((Number) aNumber);
            } else {
                super.setValue(Double.parseDouble(aNumber.toString()));
            }
        }
    }

//    @Override
//    public String toString() {
//        return (doubleValue() == null ? "" : doubleValue().toString());
//    }

    /**
     *
     */
    @Override
    public void blankQuery() {
        super.blankQuery();
//        this.numberValue = null;
//        this.lowerBoundNumber = null;
//        this.upperBoundNumber = null;
//        this.inValuesNumber = new DBNumber[]{};
    }

    @Override
    public DBOperator useInOperator(Object... literalOptions) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Object str : literalOptions) {
            intOptions.add(new DBNumber(str));
        }
        return useInOperator(intOptions.toArray(new DBNumber[]{}));
    }

    /**
     *
     * @param inValues
     */
    public DBOperator useInOperator(Number... inValues) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Number num : inValues) {
            intOptions.add(new DBNumber(num));
        }
        return useInOperator(intOptions.toArray(new DBNumber[]{}));
    }

    /**
     *
     * @param inValues
     */
    public DBOperator useInOperator(List<Number> inValues) {
        ArrayList<DBNumber> intOptions = new ArrayList<DBNumber>();
        for (Number num : inValues) {
            intOptions.add(new DBNumber(num));
        }
        return useInOperator(intOptions.toArray(new DBNumber[]{}));
    }

    /**
     *
     * @param inValues
     */
    public DBOperator useInOperator(DBNumber... inValues) {
//        this.inValuesNumber = inValues;
        return super.useInOperator(inValues);
    }

    public DBOperator useGreaterThanOperator(Number literalValue) {
        return this.useGreaterThanOperator(new DBNumber(literalValue));
    }

    @Override
    public String getWhereClause(DBDatabase db, String columnName) {
        if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
            throw new RuntimeException("NUMBER COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else {
            return super.getWhereClause(db, columnName);
        }
    }

    /**
     *
     * @param lower
     * @param upper
     */
    @Override
    public DBOperator useBetweenOperator(Object lower, Object upper) {
        DBNumber upperBoundNumber = new DBNumber(upper);
        DBNumber lowerBoundNumber = new DBNumber(lower);
        return super.useBetweenOperator(lowerBoundNumber, upperBoundNumber);
    }

    /**
     *
     * @param lower
     * @param upper
     */
    public DBOperator useBetweenOperator(Number lower, Number upper) {
        DBNumber upperBoundNumber = new DBNumber(upper);
        DBNumber lowerBoundNumber = new DBNumber(lower);
        return super.useBetweenOperator(lowerBoundNumber, upperBoundNumber);
    }

    @Override
    public DBOperator useEqualsOperator(Object literal) {
        if (literal == null || literal.toString().isEmpty()) {
            super.useEqualsOperator(null);
//            this.numberValue = null;
        } else {
            this.useEqualsOperator(Double.parseDouble(literal.toString()));
        }
        return getOperator();
    }

    /**
     *
     * @param literal
     */
    public DBOperator useEqualsOperator(Number literal) {
        DBOperator useEqualsOperator = super.useEqualsOperator(literal);
//        this.numberValue = literal;
        return useEqualsOperator;
    }

    /**
     *
     * @param obj
     */
    @Override
    public DBOperator useLikeOperator(Object obj) {
        throw new RuntimeException("LIKE Comparison Cannot Be Used With Numeric Fields: " + obj);
    }

    @Override
    protected DBOperator useNullOperator() {
        DBOperator operator = super.useNullOperator();
//        numberValue = null;
//        lowerBoundNumber = null;
//        upperBoundNumber = null;
//        inValuesNumber = new DBNumber[]{};
        return operator;
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
//    @Override
//    public String toSQLString(DBDatabase db) {
//        if (this.isDBNull || this.numberValue == null) {
//            return db.getDefinition().getNull();
//        }
//        return this.numberValue.toString();
//    }
    /**
     *
     * @param db
     * @return
     */
    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        if (isNull()) {
            return defn.getNull();
        }
        return defn.beginNumberValue() + literalValue.toString() + defn.endNumberValue();
    }

    /**
     *
     * @return
     */
    @Override
    public Double doubleValue() {
        if (literalValue == null) {
            return null;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).doubleValue();
        } else {
            return Double.parseDouble(literalValue.toString());
        }
    }

    /**
     *
     * @return
     */
    @Override
    public Long longValue() {
        if (literalValue == null) {
            return null;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).longValue();
        } else {
            return Long.parseLong(literalValue.toString());
        }
    }

    /**
     *
     * @return
     */
    @Override
    public Integer intValue() {
        if (literalValue == null) {
            return null;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).intValue();
        } else {
            return Integer.parseInt(literalValue.toString());
        }
    }

    /**
     *
     * @param resultSet
     * @param fullColumnName
     */
    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.useNullOperator();
        } else {
            BigDecimal dbValue;
            try {
                dbValue = resultSet.getBigDecimal(fullColumnName);
                if (resultSet.wasNull()) {
                    dbValue = null;
                }
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
