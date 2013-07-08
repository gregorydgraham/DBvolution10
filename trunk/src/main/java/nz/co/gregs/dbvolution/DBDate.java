/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import nz.co.gregs.dbvolution.operators.DBLikeOperator;

/**
 *
 * @author gregory.graham
 */
public class DBDate extends QueryableDatatype {

    private static final long serialVersionUID = 1L;
    private Date dateValue = new Date();

    public DBDate() {
        super();
    }

    public DBDate(Date date) {
        if (date == null) {
            this.isDBNull = true;
        } else {
            dateValue = date;
        }
    }

    DBDate(Timestamp timestamp) {
        if (timestamp == null) {
            this.isDBNull = true;
        } else {
            dateValue.setTime(timestamp.getTime());
        }
    }

    DBDate(String str) {
        super(str);
        dateValue.setTime(Date.parse(str));
    }

    @Override
    public void blankQuery() {
        super.blankQuery();
        this.dateValue = new Date();
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

    @Override
    public void isLike(Object obj) {
        throw new RuntimeException("LIKE Comparison Cannot Be Used With Date Fields: " + obj);
    }

    /**
     *
     * @param lower
     * @param upper
     */
    public void isBetween(Date lower, Date upper) {
        DBDate lowerDate = new DBDate(lower);
        DBDate upperDate = new DBDate(upper);
        super.isBetween(lowerDate, upperDate);
    }

    /**
     *
     * @param dates
     */
    public void isIn(Date[] dates) {
        ArrayList<DBDate> dbDates = new ArrayList<DBDate>();
        for (Date date : dates) {
            dbDates.add(new DBDate(date));
        }
        super.isIn(dbDates.toArray(new DBDate[]{}));
    }

    @Override
    public String getSQLDatatype() {
        return "TIMESTAMP";
    }

    @Override
    public String toString() {
        return dateValue.toString();
    }

    @Override
    public String toSQLString() {
        if (this.isDBNull){
            return this.database.getNull();
        }
        return getDatabase().getDateFormattedForQuery(this.dateValue);
    }

    @Override
    public String getSQLValue() {
        return database.getDateFormattedForQuery(dateValue);
    }
}
