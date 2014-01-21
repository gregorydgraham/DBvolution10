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

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.variables.DBValue;
import nz.co.gregs.dbvolution.variables.DateVariable;
import nz.co.gregs.dbvolution.variables.NumberVariable;
import nz.co.gregs.dbvolution.variables.StringVariable;

/**
 *
 * @author gregory.graham
 */
public class DBDataGenerator extends QueryableDatatype {

    public static final long serialVersionUID = 1L;

    public DBDataGenerator(DBValue dataGenerator) {
        super(dataGenerator);
    }

    public DBDataGenerator() {
    }

    @Override
    public String getSQLDatatype() {
        if(literalValue instanceof DateVariable){
            return new DBDate().getSQLDatatype();
        } else if(literalValue instanceof NumberVariable){
            return new DBNumber().getSQLDatatype();
        } else if(literalValue instanceof StringVariable){
            return new DBString().getSQLDatatype();
        } else {
            return new DBUnknownDatatype().getSQLDatatype();
        }
    }

    @Override
    protected String formatValueForSQLStatement(DBDatabase db) {
        return ((DBValue)literalValue).toSQLString(db);
    }
    
}
