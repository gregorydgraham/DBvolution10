/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.superconductor;

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
}
