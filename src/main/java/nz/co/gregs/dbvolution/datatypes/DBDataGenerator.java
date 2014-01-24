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
import nz.co.gregs.dbvolution.variables.DBExpression;
import nz.co.gregs.dbvolution.variables.DateResult;
import nz.co.gregs.dbvolution.variables.NumberResult;
import nz.co.gregs.dbvolution.variables.StringResult;

/**
 *
 * @author gregory.graham
 */
public class DBDataGenerator extends QueryableDatatype {

    public static final long serialVersionUID = 1L;

    public DBDataGenerator(DBExpression dataGenerator) {
        super(dataGenerator);
    }

    public DBDataGenerator() {
    }

    @Override
    public String getSQLDatatype() {
        if(literalValue instanceof DateResult){
            return new DBDate().getSQLDatatype();
        } else if(literalValue instanceof NumberResult){
            return new DBNumber().getSQLDatatype();
        } else if(literalValue instanceof StringResult){
            return new DBString().getSQLDatatype();
        } else {
            return new DBUnknownDatatype().getSQLDatatype();
        }
    }

    @Override
    protected String formatValueForSQLStatement(DBDatabase db) {
        return ((DBExpression)literalValue).toSQLString(db);
    }
    
}
