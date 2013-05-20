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

    DBInteger(Long aLong) {
        super(aLong.doubleValue());
    }

    public DBInteger() {
        super();
    }

    public String getCreationClause() {
        return "INTEGER";
    }
}
