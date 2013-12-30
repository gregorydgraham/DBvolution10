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

import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;

public class DBInCaseInsensitiveOperator extends DBInOperator {

    public static final long serialVersionUID = 1L;

    public DBInCaseInsensitiveOperator(List<QueryableDatatype> listOfPossibleValues) {
        super(listOfPossibleValues);
    }

    public DBInCaseInsensitiveOperator() {
        super();
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
        DBDefinition defn = db.getDefinition();
        StringBuilder whereClause = new StringBuilder();
        whereClause.append(defn.beginAndLine());
        if (listOfPossibleValues.isEmpty()) {
            // prevent any rows from returning as an empty list means no rows can match
            whereClause.append(defn.getFalseOperation());
        } else {
            whereClause.append(invertOperator ? "!(" : "(");
            whereClause.append(defn.toLowerCase(columnName));
            whereClause.append(getOperator());
            String sep = "";
            for (QueryableDatatype qdt : listOfPossibleValues) {
//                qdt.setDatabase(db);
                whereClause.append(sep).append(" ").append(qdt.toSQLString(db).toLowerCase()).append(" ");
                sep = ",";
            }
            whereClause.append("))");
        }
        return whereClause.toString();
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        DBDefinition defn = database.getDefinition();
        return defn.toLowerCase(columnName) + (invertOperator ? getInverse() : getOperator()) + defn.toLowerCase(otherColumnName) + " ) ";
    }
    
    @Override
    public DBInCaseInsensitiveOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	List<QueryableDatatype> list = new ArrayList<QueryableDatatype>();
    	for (QueryableDatatype item: listOfPossibleValues) {
    		list.add(typeAdaptor.convert(item));
    	}
    	DBInCaseInsensitiveOperator op = new DBInCaseInsensitiveOperator(list);
    	op.invertOperator = this.invertOperator;
    	op.includeNulls = this.includeNulls;
    	return op;
    }
}
