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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalTypeAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 *
 * @author gregory.graham
 */
public class DBGreaterThanOperator extends DBOperator {

    public static final long serialVersionUID = 1L;
    protected final QueryableDatatype greaterThanThis;

    /**
     *
     */
    public DBGreaterThanOperator() {
        super();
        this.greaterThanThis = null;
    }

    public DBGreaterThanOperator(QueryableDatatype greaterThanThis) {
        super();
        this.greaterThanThis = greaterThanThis;
    }

    public String getInverse() {
        return " <= ";
    }

    public String getOperator() {
        return " > ";
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        greaterThanThis.setDatabase(database);
        DBDefinition defn = db.getDefinition();
        return defn.beginAndLine() + columnName + (invertOperator ? getInverse() : getOperator()) + greaterThanThis.toSQLString(db) + " ";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        DBDefinition defn = database.getDefinition();
        return columnName + (invertOperator ? getInverse() : getOperator()) + otherColumnName + " ";
    }

    @Override
    public DBOperator getInverseOperator() {
        return new DBLessThanOrEqualOperator(greaterThanThis);
    }

    @Override
    public DBGreaterThanOperator copyAndAdapt(DBSafeInternalTypeAdaptor typeAdaptor) {
    	DBGreaterThanOperator op = new DBGreaterThanOperator(typeAdaptor.convert(greaterThanThis));
    	op.invertOperator = this.invertOperator;
    	return op;
    }
}
