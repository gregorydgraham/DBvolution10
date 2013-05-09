/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.superconductor;

import java.util.Date;
import java.sql.Timestamp;

/**
 *
 * @author gregory.graham
 */
class DBDate extends QueryableDatatype{
    private static final long serialVersionUID = 1L;
    
    public DBDate(Date date) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    DBDate(Timestamp timestamp) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
}
