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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datagenerators.DataGenerator;

public class DBEqualsCaseInsensitiveOperator extends DBEqualsOperator {

    public static final long serialVersionUID = 1L;

    public DBEqualsCaseInsensitiveOperator() {
        super();
    }

    public DBEqualsCaseInsensitiveOperator(DataGenerator equalTo) {
        super(equalTo);
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        firstValue.setDatabase(database);
        DBDefinition defn = db.getDefinition();
        if (firstValue.toSQLString(db).equals(defn.getNull())) {
            DBIsNullOperator dbIsNullOperator = new DBIsNullOperator();
            return dbIsNullOperator.generateWhereLine(db, columnName);
        }
        return defn.beginAndLine() + defn.toLowerCase(columnName) + (invertOperator ? getInverse(defn) : getOperator(defn)) + defn.toLowerCase(firstValue.toSQLString(db)) + " ";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        DBDefinition defn = database.getDefinition();
        return defn.toLowerCase(columnName) + (invertOperator ? getInverse(defn) : getOperator(defn)) + defn.toLowerCase(otherColumnName);
    }

    @Override
    public DBEqualsCaseInsensitiveOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBEqualsCaseInsensitiveOperator op = new DBEqualsCaseInsensitiveOperator(typeAdaptor.convert(firstValue));
    	op.invertOperator = this.invertOperator;
    	op.includeNulls = this.includeNulls;
    	return op;
    }
}
