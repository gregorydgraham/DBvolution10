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
package nz.co.gregs.dbvolution.databases.definitions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.MSSQLServerDB;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * Defines the features of the Microsoft SQL Server database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link MSSQLServerDB} instances, and
 * you should not need to use it directly.
 *
 * @author Gregory Graham
 */
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
    public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
        return " TOP("+options.getRowLimit()+") "; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
        return "";
    }

    @Override
    public String doTrimFunction(String enclosedValue) {
        return " LTRIM(RTRIM("+enclosedValue+")) "; //To change body of generated methods, choose Tools | Templates.
    }
    
    

    @Override
    public String doConcatTransform(String firstString, String secondString) {
        return firstString+"+"+secondString;
    }

    @Override
    public String getIfNullFunctionName() {
        return "ISNULL"; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getStandardDeviationFunctionName() {
        return "STDEV";
    }

    @Override
    public boolean supportsPagingNatively(QueryOptions options) {
        return false;
    }

	@Override
	public String getColumnAutoIncrementSuffix() {
		return " IDENTITY ";
	}

	public boolean supportsXOROperator() {
		return true;
	}
}
