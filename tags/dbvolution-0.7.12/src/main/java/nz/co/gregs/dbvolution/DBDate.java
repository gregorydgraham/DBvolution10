/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import nz.co.gregs.dbvolution.operators.DBLikeOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
public class DBDate extends QueryableDatatype {

    private static final long serialVersionUID = 1L;
    protected Date dateValue = null;

    public DBDate() {
        super();
    }

    public DBDate(Date date) {
        super(date);
        if (date == null) {
            this.isDBNull = true;
        } else {
            dateValue = date;
        }
//        this.isLiterally(date);
    }

    DBDate(Timestamp timestamp) {
        super(timestamp);
        if (timestamp == null) {
            this.isDBNull = true;
        } else {
            Date date = new Date();
            date.setTime(timestamp.getTime());
            dateValue = date;
//            this.isLiterally(dateValue);
        }
    }

    DBDate(String str) {
        final long dateLong = Date.parse(str);
        dateValue = new Date();
        dateValue.setTime(dateLong);
        this.isLiterally(dateValue);
    }

    @Override
    public void blankQuery() {
        super.blankQuery();
        this.dateValue = null;
    }

    @Override
    public String getWhereClause(String columnName) {
//        if (this.usingLikeComparison) {
        if (this.getOperator() instanceof DBLikeOperator) {
            throw new RuntimeException("DATE COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else {
            return super.getWhereClause(columnName);
        }
//        return whereClause.toString();
    }

    public Date dateValue() {
        return dateValue;
    }

    @Override
    public DBOperator isLiterally(Date date) {
        super.isLiterally(date);
        dateValue = date;
        return getOperator();
    }

    public DBOperator isLiterally(String dateStr) {
        final long dateLong = Date.parse(dateStr);
        Date date = new Date();
        date.setTime(dateLong);
        super.isLiterally(date);
        dateValue = date;
        return getOperator();
    }

    @Override
    public DBOperator isLike(Object obj) {
        throw new RuntimeException("LIKE Comparison Cannot Be Used With Date Fields: " + obj);
    }

    public DBOperator isGreaterThan(Date literalValue) {
        return this.isGreaterThan(new DBDate(literalValue));
    }

    /**
     *
     * @param lower
     * @param upper
     */
    public DBOperator isBetween(Date lower, Date upper) {
        DBDate lowerDate = new DBDate(lower);
        DBDate upperDate = new DBDate(upper);
        super.isBetween(lowerDate, upperDate);
        return getOperator();
    }

    /**
     *
     * @param dates
     */
    public DBOperator isIn(Date... dates) {
        ArrayList<DBDate> dbDates = new ArrayList<DBDate>();
        for (Date date : dates) {
            dbDates.add(new DBDate(date));
        }
        super.isIn(dbDates.toArray(new DBDate[]{}));
        return getOperator();
    }

    @Override
    public String getSQLDatatype() {
        return "TIMESTAMP";
    }

    @Override
    public String toString() {
        if (this.isDBNull || dateValue == null) {
            return "";
        }
        return dateValue.toString();
    }

    @Override
    public String toSQLString() {
        if (this.isDBNull || dateValue == null) {
            return this.database.getNull();
        }
        return getDatabase().getDateFormattedForQuery(this.dateValue);
    }

    @Override
    public String getSQLValue() {
        return database.getDateFormattedForQuery(dateValue);
    }

    @Override
    protected void setFromResultSet(ResultSet resultSet, String fullColumnName) throws SQLException {
        this.isLiterally(resultSet.getDate(fullColumnName));
    }
}
