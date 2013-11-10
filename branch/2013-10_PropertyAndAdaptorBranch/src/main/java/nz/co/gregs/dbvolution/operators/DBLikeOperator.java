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

/**
 *
 * @author gregorygraham
 */
public class DBLikeOperator extends DBOperator {
    public static final long serialVersionUID = 1L;
    private final QueryableDatatype likeableValue;

    public DBLikeOperator(QueryableDatatype likeableValue) {
        super();
        this.likeableValue = likeableValue;
    }

    public DBLikeOperator() {
        super();
        this.likeableValue = null;
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        likeableValue.setDatabase(db);
        DBDefinition defn = db.getDefinition();
        return defn.beginAndLine() +(invertOperator?"!(":"(")+ defn.formatColumnName(columnName) + getOperator()+likeableValue.toSQLString(db)+")";
    }

    private String getOperator() {
        return " like ";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        DBDefinition defn = database.getDefinition();
        return (invertOperator?"!(":"(")+ defn.formatColumnName(columnName) + getOperator()+otherColumnName+")";
    }

    @Override
    public DBOperator getInverseOperator() {
        return this;
    }
    
    
    @Override
    public DBLikeOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBLikeOperator op = new DBLikeOperator(typeAdaptor.convert(likeableValue));
    	op.invertOperator = this.invertOperator;
    	return op;
    }
}
