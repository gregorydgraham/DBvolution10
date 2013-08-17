/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * Implements the abstractions necessary to handle arbitrary byte streams and files stored in the database
 * 
 * @author gregory.graham
 */
public class DBByteArray extends QueryableDatatype {

    public static final long serialVersionUID = 1;

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
    
    @Override
        protected void setFromResultSet(ResultSet resultSet, String fullColumnName) throws SQLException{
        this.useEqualsOperator(resultSet.getBytes(fullColumnName));
    }

    @Override
    public String getSQLValue() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
