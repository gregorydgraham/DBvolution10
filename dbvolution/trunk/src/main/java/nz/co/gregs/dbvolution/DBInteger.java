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
        super(Double.valueOf(anInt));
    }

    public DBInteger(Integer anInt) {
        super(anInt.doubleValue());
    }

    public DBInteger(Long aLong) {
        super(aLong.doubleValue());
    }

    public DBInteger() {
        super();
    }

    @Override
    public String getCreationClause() {
        return "INTEGER";
    }
}
