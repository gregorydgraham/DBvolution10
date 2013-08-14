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

import java.util.List;
import nz.co.gregs.dbvolution.QueryableDatatype;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public class DBInOperator extends DBOperator {

    private final List<QueryableDatatype> listOfPossibleValues;

    public DBInOperator(List<QueryableDatatype> listOfPossibleValues) {
        super();
        this.listOfPossibleValues = listOfPossibleValues;
    }

    @Override
    public String generateWhereLine(DBDatabase database, String columnName) {
        StringBuilder whereClause = new StringBuilder();
        whereClause.append(database.beginAndLine());
        if (listOfPossibleValues.isEmpty()) {
            // prevent any rows from returning as an empty list means no rows can match
            whereClause.append(database.getFalseOperation());
        } else {
            whereClause.append(invertOperator ? "!(" : "(");
            whereClause.append(columnName);
            whereClause.append(getOperator());
            String sep = "";
            for (QueryableDatatype qdt : listOfPossibleValues) {
                qdt.setDatabase(database);
                whereClause.append(sep).append(" ").append(qdt.getSQLValue()).append(" ");
                sep = ",";
            }
            whereClause.append("))");
        }
        return whereClause.toString();
    }

    private String getOperator() {
        return " in (";
    }

    private String getInverse() {
        return " not in (";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
                return database.beginAndLine() + columnName + (invertOperator ? getInverse() : getOperator()) + otherColumnName+" ) ";
    }
}
