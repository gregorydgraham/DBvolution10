/*
 * Copyright 2013 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBLikeOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;
import nz.co.gregs.dbvolution.expressions.NumberResult;

/**
 *
 * @author Gregory Graham
 */
public class DBNumber extends QueryableDatatype implements NumberResult {

    private static final long serialVersionUID = 1;

    public DBNumber() {
        super();
    }

    public DBNumber(NumberResult numberExpression) {
        super(numberExpression);
    }

    /**
     *
     * @param aNumber
     */
    public DBNumber(Number aNumber) {
        super(aNumber);
    }

    @Override
    public DBNumber copy() {
        return (DBNumber) super.copy();
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if(newLiteralValue instanceof Number){
            setValue((Number) newLiteralValue);
        }else if(newLiteralValue instanceof DBNumber){
            setValue(((DBNumber) newLiteralValue).getValue());
        }else{
            throw new ClassCastException(this.getClass().getSimpleName()+".setValue() Called With A "+newLiteralValue.getClass().getSimpleName()+": Use only Numbers with this class");
        }
    }

    public void setValue(Number newLiteralValue) {
        if (newLiteralValue == null) {
            super.setLiteralValue(null);
        } else {
            super.setLiteralValue(newLiteralValue);
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
        } else if (this.getOperator() instanceof DBLikeOperator) {
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
     * @return the default database type as a string, may be gazumped by the
     * DBDefinition
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
     * Gets the current literal value of this DBNumber, without any formatting.
     *
     * <p>
     * The literal value is undefined (and {@code null}) if using an operator
     * other than {@code equals}.
     *
     * @return the literal value, if defined, which may be null
     */
    @Override
    public Number getValue() {
        return numberValue();
    }

    /**
     * The current {@link #getValue()  literal value} of this DBNumber as a
     * Number
     *
     * @return the number as the original number class
     */
    public Number numberValue() {
        if (literalValue == null) {
            return null;
        } else if (literalValue instanceof Number) {
            return (Number) literalValue;
        } else {
            return Double.parseDouble(literalValue.toString());
        }
    }

    /**
     * The current {@link #getValue()  literal value} of this DBNumber as a
     * Double
     *
     * @return the number as a Double
     */
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
     * The current {@link #getValue()  literal value} of this DBNumber as a Long
     *
     * @return the number as a Long
     */
    public Long longValue() {
        if (literalValue == null) {
            return null;
        } else if (literalValue instanceof Long) {
            return (Long) literalValue;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).longValue();
        } else {
            return Long.parseLong(literalValue.toString());
        }
    }

    /**
     * The current {@link #getValue()  literal value} of this DBNumber as an
     * Integer
     *
     * @return the number as an Integer
     */
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
     * Internal method to automatically set the value using information from the
     * database
     *
     * @param resultSet
     * @param fullColumnName
     */
    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        blankQuery();
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
        setUnchanged();
        setDefined(true);
    }

    @Override
    public DBNumber getQueryableDatatypeForExpressionValue() {
        return new DBNumber();
    }
}
