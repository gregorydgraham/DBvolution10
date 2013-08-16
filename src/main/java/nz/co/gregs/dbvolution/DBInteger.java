/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
public class DBInteger extends DBNumber {

    private static final long serialVersionUID = 1L;

    public DBInteger(Object someNumber) {
        this(Long.parseLong(someNumber.toString()));
    }

    public DBInteger(int anInt) {
        this(Integer.valueOf(anInt));
    }

    public DBInteger(Integer anInt) {
        super(anInt);
    }

    public DBInteger(long aLong) {
        super(Long.valueOf(aLong));
    }

    public DBInteger(Long aLong) {
        super(aLong);
    }

    public DBInteger() {
        super();
    }

    @Override
    public DBOperator useEqualsOperator(Object someNumber) {
        if (someNumber == null || someNumber.toString().isEmpty()) {
            super.useEqualsOperator((Object) null);
        } else {
            super.useEqualsOperator(Long.parseLong(someNumber.toString()));
        }
        return getOperator();
    }

    public void useInOperator(Integer... inValues) {
        ArrayList<DBInteger> intOptions = new ArrayList<DBInteger>();
        for (Integer num : inValues) {
            intOptions.add(new DBInteger(num));
        }
        useInOperator(intOptions.toArray(this.inValuesNumber));
    }

    public void useInOperator(Long... inValues) {
        ArrayList<DBInteger> intOptions = new ArrayList<DBInteger>();
        for (Long num : inValues) {
            intOptions.add(new DBInteger(num));
        }
        useInOperator(intOptions.toArray(this.inValuesNumber));
    }

    public void useInOperator(DBInteger... inValues) {
        super.useInOperator(inValues);
    }

    public DBOperator useGreaterThanOperator(Integer literalValue) {
        return this.useGreaterThanOperator(new DBInteger(literalValue));
    }

    @Override
    public String getSQLDatatype() {
        return "INTEGER";
    }

    @Override
    public String getSQLValue() {
        return database.beginNumberValue() + numberValue.toString() + database.endNumberValue();
    }

    @Override
    protected void setFromResultSet(ResultSet resultSet, String fullColumnName) throws SQLException {
        this.useEqualsOperator(resultSet.getLong(fullColumnName));
    }
}
