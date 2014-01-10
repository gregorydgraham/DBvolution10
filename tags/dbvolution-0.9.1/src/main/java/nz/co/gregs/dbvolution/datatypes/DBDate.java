/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;

/**
 *
 * @author gregory.graham
 */
public class DBDate extends QueryableDatatype {

    private static final long serialVersionUID = 1L;

    public DBDate() {
        super();
    }

    public DBDate(Date date) {
        super(date);
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

    public void setValue(Date date) {
        super.setValue(date);
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
}
