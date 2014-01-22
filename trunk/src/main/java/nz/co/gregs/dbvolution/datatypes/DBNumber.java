/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;
import nz.co.gregs.dbvolution.variables.NumberVariable;

/**
 *
 * @author gregory.graham
 */
public class DBNumber extends QueryableDatatype implements NumberVariable{

    public static final long serialVersionUID = 1;

    public DBNumber() {
        super();
    }

    /**
     *
     * @param aNumber
     */
    public DBNumber(Number aNumber) {
        super(aNumber);
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
    public DBNumber copy() {
        return (DBNumber) super.copy();
    }

    @Override
    public void setValue(Object newLiteralValue) {
        initDBNumber(newLiteralValue);
    }

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

    @Override
    public void blankQuery() {
        super.blankQuery();
    }

    @Override
    public String getWhereClause(DBDatabase db, String columnName) {
        if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
            throw new RuntimeException("NUMBER COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else {
            return super.getWhereClause(db, columnName);
        }
    }

    @Override
    protected DBOperator setToNull() {
        DBOperator op = super.setToNull();
        return op;
    }

    /**
     *
     * @return the default database type as a string, may be gazumped by the DBDefinition
     */
    @Override
    public String getSQLDatatype() {
        return "NUMERIC(15,5)";
    }

    /**
     *
     * @param db
     * @return the underlying number formatted for a SQL statement
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
     * @return the number as a Double
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
     * @return the number as a Long
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
     * @return the number as an iInteger
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
     * Internal method to automatically set the value using information from the database
     *
     * @param resultSet
     * @param fullColumnName
     */
    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.setToNull();
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
                this.setToNull();
            } else {
                this.setValue(dbValue);
            }
        }
    }
}
