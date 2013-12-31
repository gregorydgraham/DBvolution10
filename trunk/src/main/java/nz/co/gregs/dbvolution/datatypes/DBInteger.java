/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;

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
        this(aDouble == null ? null : aDouble.longValue());
    }

    public DBInteger() {
        super();
    }

    @Override
    public String getSQLDatatype() {
        return "INTEGER";
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.setToNull();
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
                this.setToNull();
            } else {
                this.setValue(dbValue);
            }
        }
    }
}
