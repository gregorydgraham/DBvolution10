/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

/**
 *
 * @author gregory.graham
 */
public class DBBlob extends QueryableDatatype {

    public DBBlob(Object object) {
        super(object);

    }

    public DBBlob() {
        super();
    }

    @Override
    public String getSQLDatatype() {
        return "BLOB";
    }
}
