/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

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
        this(Long.valueOf(aLong));
    }

    public DBInteger(Long aLong) {
        super(aLong);
    }

    public DBInteger(double aDouble) {
        this(new Double(aDouble));
    }

    public DBInteger(Double aDouble) {
        this(aDouble.longValue());
    }

    public DBInteger() {
        super();
    }

    @Override
    public DBOperator useEqualsOperator(Object someNumber) {
        if (someNumber == null || someNumber.toString().isEmpty()) {
            return super.useEqualsOperator((Object) null);
        } else if (someNumber instanceof Number) {
            Number aNumber = (Number) someNumber;
            return super.useEqualsOperator(aNumber.longValue());
        } else {
            return super.useEqualsOperator(Long.parseLong(someNumber.toString()));
        }
//        return getOperator();
    }

    public DBOperator useInOperator(Integer... inValues) {
        ArrayList<DBInteger> intOptions = new ArrayList<DBInteger>();
        for (Integer num : inValues) {
            intOptions.add(new DBInteger(num));
        }
        return useInOperator(intOptions.toArray(new DBInteger[]{}));
    }

    public DBOperator useInOperator(Long... inValues) {
        ArrayList<DBInteger> intOptions = new ArrayList<DBInteger>();
        for (Long num : inValues) {
            intOptions.add(new DBInteger(num));
        }
        return useInOperator(intOptions.toArray(new DBInteger[]{}));
    }

    public DBOperator useInOperator(DBInteger... inValues) {
        return super.useInOperator(inValues);
    }

    public DBOperator useGreaterThanOperator(Integer literalValue) {
        return this.useGreaterThanOperator(new DBInteger(literalValue));
    }

    @Override
    public String getSQLDatatype() {
        return "INTEGER";
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.useNullOperator();
        } else {
            Long dbValue;
            try {
                dbValue = resultSet.getLong(fullColumnName);
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
