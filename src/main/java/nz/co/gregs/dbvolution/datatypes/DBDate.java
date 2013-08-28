/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

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
    public String getWhereClause(String columnName) {
        if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
            throw new RuntimeException("DATE COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else {
            return super.getWhereClause(columnName);
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
        useEqualsOperator(date);
    }

    public DBOperator useEqualsOperator(Date date) {
        return super.useEqualsOperator(date);
    }

    @SuppressWarnings("deprecation")
    public DBOperator useEqualsOperator(String dateStr) {
        final long dateLong = Date.parse(dateStr);
        Date date = new Date();
        date.setTime(dateLong);
        super.useEqualsOperator(date);
        return getOperator();
    }

    @Override
    public DBOperator useLikeOperator(Object obj) {
        throw new RuntimeException("LIKE Comparison Cannot Be Used With Date Fields: " + obj);
    }

    public DBOperator isGreaterThan(Date literalValue) {
        return this.useGreaterThanOperator(new DBDate(literalValue));
    }

    /**
     *
     * @param lower
     * @param upper
     */
    public DBOperator useBetweenOperator(Date lower, Date upper) {
        DBDate lowerDate = new DBDate(lower);
        DBDate upperDate = new DBDate(upper);
        super.useBetweenOperator(lowerDate, upperDate);
        return getOperator();
    }

    /**
     *
     * @param dates
     */
    public DBOperator useInOperator(Date... dates) {
        ArrayList<DBDate> dbDates = new ArrayList<DBDate>();
        for (Date date : dates) {
            dbDates.add(new DBDate(date));
        }
        super.useInOperator(dbDates.toArray(new DBDate[]{}));
        return getOperator();
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
    public String toSQLString() {
        DBDefinition defn = database.getDefinition();
        if (this.isDBNull || dateValue() == null) {
            return defn.getNull();
        }
        return defn.getDateFormattedForQuery(dateValue());
    }

    @Override
    public String getSQLValue() {
        return database.getDefinition().getDateFormattedForQuery(dateValue());
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.useNullOperator();
        } else {
            java.sql.Date dbValue;
            try {
                dbValue = resultSet.getDate(fullColumnName);
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
