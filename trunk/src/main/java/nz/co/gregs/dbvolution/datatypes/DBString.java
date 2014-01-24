/*
 * Copyright 2013 gregory.graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.util.Date;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.variables.StringResult;

/**
 *
 * @author gregory.graham
 */
public class DBString extends QueryableDatatype implements StringResult{

    private static final long serialVersionUID = 1L;

    public DBString() {
        super();
    }

    public DBString(String string) {
        super(string);
    }
    
    public void setValue(String str) {
        super.setValue(str);
    }

    @Override
    public String getSQLDatatype() {
        return "VARCHAR(1000)";
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        
        if (literalValue instanceof Date) {
            return defn.getDateFormattedForQuery((Date) literalValue);
        } else {
            String unsafeValue = literalValue.toString();
            return defn.beginStringValue() + defn.safeString(unsafeValue) + defn.endStringValue();
        }
//    }
    }

    @Override
    public DBString copy() {
        return (DBString)super.copy();
    }

}
