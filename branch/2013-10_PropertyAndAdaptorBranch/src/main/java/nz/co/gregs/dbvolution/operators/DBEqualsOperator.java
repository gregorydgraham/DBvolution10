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
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalTypeAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 *
 * @author gregorygraham
 */
public class DBEqualsOperator extends DBOperator {

    public static final long serialVersionUID = 1L;
    protected final QueryableDatatype equalTo;
    protected DBDefinition defn;

    /**
     *
     */
    public DBEqualsOperator() {
        super();
        equalTo = null;
    }

    public DBEqualsOperator(QueryableDatatype equalTo) {
        super();
        this.equalTo = equalTo;
    }

    public String getInverse() {
        if (defn!=null){
            return defn.getNotEqualsComparator();
        }
        return " <> ";
    }

    public String getOperator() {
        if (defn!=null){
            return defn.getEqualsComparator();
        }
        return " = ";
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        equalTo.setDatabase(database);
        defn = db.getDefinition();
        if (equalTo.toSQLString(db).equals(defn.getNull())) {
            DBIsNullOperator dbIsNullOperator = new DBIsNullOperator();
            return dbIsNullOperator.generateWhereLine(db, columnName);
        }
        return defn.beginAndLine() + columnName + (invertOperator ? getInverse() : getOperator()) + equalTo.toSQLString(db) + " ";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        defn = database.getDefinition();
        return columnName + (invertOperator ? getInverse() : getOperator()) + otherColumnName;
    }

    @Override
    public DBOperator getInverseOperator() {
        return this;
    }

    @Override
    public DBEqualsOperator copyAndAdapt(DBSafeInternalTypeAdaptor typeAdaptor) {
    	DBEqualsOperator op = new DBEqualsOperator(typeAdaptor.convert(equalTo));
    	op.invertOperator = this.invertOperator;
    	return op;
    }
}
