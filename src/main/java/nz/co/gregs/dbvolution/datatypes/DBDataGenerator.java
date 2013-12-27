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
import nz.co.gregs.dbvolution.datagenerators.DataGenerator;
import nz.co.gregs.dbvolution.datagenerators.DateGenerator;
import nz.co.gregs.dbvolution.datagenerators.NumberGenerator;
import nz.co.gregs.dbvolution.datagenerators.StringGenerator;

/**
 *
 * @author gregory.graham
 */
public class DBDataGenerator extends QueryableDatatype {

    public static final long serialVersionUID = 1L;

    public DBDataGenerator(DataGenerator dataGenerator) {
        super(dataGenerator);
    }

    public DBDataGenerator() {
    }

    @Override
    public String getSQLDatatype() {
        if(literalValue instanceof DateGenerator){
            return new DBDate().getSQLDatatype();
        } else if(literalValue instanceof NumberGenerator){
            return new DBNumber().getSQLDatatype();
        } else if(literalValue instanceof StringGenerator){
            return new DBString().getSQLDatatype();
        } else {
            return new DBUnknownDatatype().getSQLDatatype();
        }
    }

    @Override
    protected String formatValueForSQLStatement(DBDatabase db) {
        return ((DataGenerator)literalValue).generate(db.getDefinition());
    }
    
}
