/*
 * Copyright 2013 gregorygraham.
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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanResult;

public class DBBoolean extends QueryableDatatype implements BooleanResult{

    private static final long serialVersionUID = 1L;

    public DBBoolean() {
    }

    public DBBoolean(Boolean bool) {
        super(bool);
    }

    @Override
    public String getSQLDatatype() {
        return "BIT(1)";
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
//        if (this.isDBNull || literalValue == null) {
//            defn.getNull();
//        } else 
        if (literalValue instanceof Boolean) {
            Boolean boolValue = (Boolean) literalValue;
            return defn.beginNumberValue() + (boolValue == Boolean.TRUE ? 1 : 0) + defn.endNumberValue();
        }
        return defn.getNull();
    }

    public Boolean booleanValue() {
        if (this.literalValue instanceof Boolean) {
            return (Boolean) this.literalValue;
        } else {
            return null;
        }
    }

    @Override
    public DBBoolean copy() {
        return (DBBoolean) (BooleanResult) super.copy();
    }
}
