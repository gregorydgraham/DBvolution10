/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
public class DBString extends QueryableDatatype {

    private static final long serialVersionUID = 1L;

    public DBString(String string) {
        super(string);
    }

    public DBString() {
        super();
    }

    public DBOperator isGreaterThan(String literalValue) {
        return this.isGreaterThan(new DBString(literalValue));
    }
}
