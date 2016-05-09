/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.StringExpression;


public class DBStringTrimmed extends DBString {

	private static final long serialVersionUID = 1L;

	public DBStringTrimmed() {
    }

    public DBStringTrimmed(String string) {
        super(string);
    }

    public DBStringTrimmed(StringExpression stringExpression) {
        super(stringExpression);
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        return db.getDefinition().doTrimFunction(super.formatValueForSQLStatement(db));
    }
    
    

    @Override
    public String formatColumnForSQLStatement(DBDatabase db, String formattedColumnName) {
        return db.getDefinition().doTrimFunction(formattedColumnName);
    }
     
}
