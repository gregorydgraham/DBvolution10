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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;

public class MSSQLServerDBDefinition extends DBDefinition {

    private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public String getDateFormattedForQuery(Date date) {
        return "'" + DATETIME_FORMAT.format(date) + "'";
    }

    @Override
    public String formatTableName(DBRow table) {
        return "["+table.getTableName()+"]";
    }

    @Override
    public Object endSQLStatement() {
        return "";
    }

    @Override
    public Object getLimitRowsSubClauseDuringSelectClause(Long rowLimit) {
        return " TOP("+rowLimit+") "; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object getLimitRowsSubClauseAfterWhereClause(Long rowLimit) {
        return "";
    }


}
