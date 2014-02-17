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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.expressions.DateResult;

/**
 *
 * @author Gregory Graham
 */
public class DBDate extends QueryableDatatype implements DateResult{

    private static final long serialVersionUID = 1L;

    public DBDate() {
        super();
    }

    public DBDate(Date date) {
        super(date);
    }

    public DBDate(DateResult dateExpression) {
        super(dateExpression);
    }

    DBDate(Timestamp timestamp) {
        super(timestamp);
        if (timestamp == null) {
            this.isDBNull = true;
        } else {
            Date date = new Date();
            date.setTime(timestamp.getTime());
            literalValue = date;
        }
    }

    @SuppressWarnings("deprecation")
    DBDate(String str) {
        final long dateLong = Date.parse(str);
        Date dateValue = new Date();
        dateValue.setTime(dateLong);
        literalValue = dateValue;
    }

    @Override
    public String getWhereClause(DBDatabase db, String columnName) {
        if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
            throw new RuntimeException("DATE COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else {
            return super.getWhereClause(db, columnName);
        }
    }

    public Date dateValue() {
        if (literalValue instanceof Date) {
            return (Date) literalValue;
        } else {
            return null;
        }
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if(newLiteralValue instanceof Date){
            setValue((Date) newLiteralValue);
        }else if(newLiteralValue instanceof DBDate){
            setValue(((DBDate) newLiteralValue).literalValue);
        }else{
            throw new ClassCastException(this.getClass().getSimpleName()+".setValue() Called With A Non-Date: Use only Dates with this class");
        }
    }

    public void setValue(Date date) {
        super.setLiteralValue(date);
    }

    @SuppressWarnings("deprecation")
    public void setValue(String dateStr) {
        final long dateLong = Date.parse(dateStr);
        Date date = new Date();
        date.setTime(dateLong);
        setValue(date);
    }

    @Override
    public String getSQLDatatype() {
        return "TIMESTAMP";
    }

    @Override
    public String toString() {
        if (this.isDBNull || dateValue() == null) {
            return "";
        }
        return dateValue().toString();
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        return db.getDefinition().getDateFormattedForQuery(dateValue());
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.setToNull();
        } else {
            java.sql.Date dbValue;
            try {
                dbValue = resultSet.getDate(fullColumnName);
                if (resultSet.wasNull()){
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

    @Override
    public DBDate copy() {
        return (DBDate)super.copy(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Date getValue() {
        return dateValue();
    }

    @Override
    public DBDate getQueryableDatatypeForExpressionValue() {
        return new DBDate();
    }
    
    
}
