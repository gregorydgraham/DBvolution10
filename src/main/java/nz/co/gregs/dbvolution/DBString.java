/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.util.Date;
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

    @Override
    public String getSQLDatatype() {
        return "VARCHAR(1000)";
    }

    @Override
    public String getSQLValue() {
        if (this.isDBNull||literalValue==null) {
            return database.getNull();
        } else {
            if (literalValue instanceof Date) {
                return database.getDateFormattedForQuery((Date) literalValue);
            } else {
                String unsafeValue = literalValue.toString();
                return database.beginStringValue() + database.safeString(unsafeValue) + database.endStringValue();
            }
        }
    }

    public DBOperator isGreaterThan(String literalValue) {
        return this.isGreaterThan(new DBString(literalValue));
    }
}
