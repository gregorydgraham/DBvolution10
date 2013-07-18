/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

/**
 *
 * @author gregory.graham
 */
public class DBByteArray extends QueryableDatatype {

    public DBByteArray(Object object) {
        super(object);

    }

    public DBByteArray() {
        super();
    }

    @Override
    public String getSQLDatatype() {
        return "BLOB";
    }
}
