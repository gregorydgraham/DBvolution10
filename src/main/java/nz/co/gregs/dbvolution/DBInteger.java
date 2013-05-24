/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

/**
 *
 * @author gregory.graham
 */
public class DBInteger extends DBNumber {

    private static final long serialVersionUID = 1L;

    public DBInteger(int anInt) {
        super(Integer.valueOf(anInt).longValue());
    }

    public DBInteger(Integer anInt) {
        super(anInt.longValue());
    }

    public DBInteger(Long aLong) {
        super(aLong);
    }

    public DBInteger() {
        super();
    }

    @Override
    public String getSQLDatatype() {
        return "INTEGER";
    }

    @Override
    String getSQLValue() {
        return database.beginNumberValue() + numberValue.toString() + database.endNumberValue();
    }
}
