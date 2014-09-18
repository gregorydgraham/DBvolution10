/*
 * Copyright 2013 Gregory Graham.
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

import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * @author Gregory Graham
 */
public class DBGreaterThanOperator extends DBOperator {

    public static final long serialVersionUID = 1L;
//    protected final QueryableDatatype firstValue;

    /**
     *
     */
    public DBGreaterThanOperator() {
        super();
        this.firstValue = null;
    }

    public DBGreaterThanOperator(DBExpression greaterThanThis) {
        super();
        this.firstValue = greaterThanThis == null ? greaterThanThis : greaterThanThis.copy();
    }

    public String getInverse() {
        return " <= ";
    }

    public String getOperator() {
        return " > ";
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        firstValue.setDatabase(database);
        DBDefinition defn = db.getDefinition();
        return columnName + (invertOperator ? getInverse() : getOperator()) + firstValue.toSQLString(db) + " ";
    }

//    @Override
//    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
//        return columnName + (invertOperator ? getInverse() : getOperator()) + otherColumnName + " ";
//    }

    @Override
    public DBOperator getInverseOperator() {
        return new DBLessThanOrEqualOperator(firstValue);
    }

    @Override
    public DBGreaterThanOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBGreaterThanOperator op = new DBGreaterThanOperator(typeAdaptor.convert(firstValue));
    	op.invertOperator = this.invertOperator;
    	op.includeNulls = this.includeNulls;
    	return op;
    }
}
