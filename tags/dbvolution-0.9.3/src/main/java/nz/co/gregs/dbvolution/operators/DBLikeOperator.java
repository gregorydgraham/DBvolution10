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

import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * @author gregorygraham
 */
public class DBLikeOperator extends DBOperator {

    public static final long serialVersionUID = 1L;
//    private final QueryableDatatype firstValue;

    public DBLikeOperator(DBExpression likeableValue) {
        super();
        this.firstValue = likeableValue == null ? likeableValue : likeableValue.copy();
    }

    public DBLikeOperator() {
        super();
        this.firstValue = null;
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        likeableValue.setDatabase(db);
        DBDefinition defn = db.getDefinition();
        return defn.beginAndLine() + (invertOperator ? "!(" : "(") + defn.formatColumnName(columnName) + getOperator() + firstValue.toSQLString(db) + ")";
    }

    private String getOperator() {
        return " like ";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        DBDefinition defn = database.getDefinition();
        return (invertOperator ? "!(" : "(") + defn.formatColumnName(columnName) + getOperator() + otherColumnName + ")";
    }

    @Override
    public DBOperator getInverseOperator() {
        return this;
    }

    
    @Override
    public DBLikeOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBLikeOperator op = new DBLikeOperator(typeAdaptor.convert(firstValue));
    	op.invertOperator = this.invertOperator;
    	op.includeNulls = this.includeNulls;
    	return op;
    }
}
