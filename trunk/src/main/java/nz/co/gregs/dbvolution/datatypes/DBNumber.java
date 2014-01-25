/*
 * Copyright 2013 gregory.graham.
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
 * @author gregory.graham
 */
public class DBNumber extends QueryableDatatype implements NumberResult{

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
            	// FIXME (Ticket 35): don't think this should be here - would be better to give ClassCastException
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
        }
        else if (this.getOperator() instanceof DBLikeOperator) {
            throw new RuntimeException("NUMBER COLUMNS CAN'T USE \"LIKE\": " + columnName);
        }
        else {
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
