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
package nz.co.gregs.dbvolution.databases.definitions;

import java.util.Date;
import nz.co.gregs.dbvolution.datatypes.*;

public class MySQLDBDefinition extends DBDefinition {

    @Override
    @SuppressWarnings("deprecation")
    public String getDateFormattedForQuery(Date date) {
        //SELECT STR_TO_DATE('01,5,2013','%d,%m,%Y');
        //SELECT STR_TO_DATE('09:30:17','%h:%i:%s');

        return " STR_TO_DATE('"
                + date.getDate() + ","
                + (date.getMonth() + 1) + ","
                + (date.getYear() + 1900) + " "
                + date.getHours() + ":"
                + date.getMinutes() + ":"
                + date.getSeconds()
                + "', '%d,%m,%Y %H:%i:%s') ";

    }

    @Override
    public String getEqualsComparator() {
        return " = ";
    }

    @Override
    public String getNotEqualsComparator() {
        return " <> ";
    }

    @Override
    public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
        if (qdt instanceof DBString) {
            return " VARCHAR(1000) CHARACTER SET latin1 COLLATE latin1_general_cs ";
        } else if (qdt instanceof DBDate) {
            return " DATETIME ";
        } else if (qdt instanceof DBByteArray) {
            return " LONGBLOB ";
        } else if (qdt instanceof DBLargeObject) {
            return " LONGBLOB ";
        } else {
            return qdt.getSQLDatatype();
        }
    }

    @Override
    public Object getLimitRowsSubClauseDuringSelectClause(Long rowLimit) {
        return "";
    }

    @Override
    public Object getLimitRowsSubClauseAfterWhereClause(Long rowLimit) {
        if (rowLimit != null) {
            return " Limit  " + rowLimit + " ";
        }else
            return "";
    }

    @Override
    public String getDropDatabase(String databaseName) {
        return "DROP DATABASE IF EXISTS "+databaseName+";";
    }

    @Override
    public String doConcatTransform(String firstString, String secondString) {
        return " CONCAT("+firstString+", "+secondString+") ";
    }
    
    
}
