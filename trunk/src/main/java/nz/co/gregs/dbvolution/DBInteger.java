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
    
    public void isLiterally(Object someNumber){
        super.isLiterally(Long.parseLong(someNumber.toString()));
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
