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

import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;

public class InformixDBDefinition extends DBDefinition {

    private final SimpleDateFormat dateFormat;
    private final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    //TO_DATE("1998-07-07 10:24",   "%Y-%m-%d %H:%M")
    public String informixDateFormat = "%Y-%m-%d %H:%M:%S";

    public InformixDBDefinition() {

        this.dateFormat = new SimpleDateFormat(DATE_FORMAT);
    }

    @Override
    public boolean prefersIndexBasedGroupByClause() {
        return true;
    }

    @Override
    public boolean prefersIndexBasedOrderByClause() {
        return true;
    }

    @Override
    public String getDateFormattedForQuery(Date date) {
        return "TO_DATE('" + dateFormat.format(date) + "','" + informixDateFormat + "')";
    }

    /**
     *
     * @param table
     * @param columnName
     * @return a string of the table and column name for the select clause
     */
    @Override
    public String formatTableAndColumnName(DBRow table, String columnName) {
        return "" + formatTableName(table) + "." + formatColumnName(columnName) + "";
    }

    @Override
    public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
        Long rowLimit = options.getRowLimit();
        Long pageNumber = options.getPageIndex();
        if (rowLimit == null) {
            return "";
        } else {
            if (supportsPaging(options) && pageNumber != null) {
                Long offset = pageNumber * rowLimit;
                if (offset > 0L) {
                    return " SKIP " + offset + " FIRST " + rowLimit + " ";
                } else {
                    return " FIRST " + rowLimit + " ";
                }
            } else {
                return " FIRST " + rowLimit + " ";
            }
        }
    }

    @Override
    public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
        return "";
    }

    @Override
    public String getCurrentDateFunctionName() {
        return " current ";
    }

    @Override
    public String getDayFunction(String dateExpression) {
        return "DAY(" + dateExpression + ")";
    }

    @Override
    public String getMonthFunction(String dateExpression) {
        return "MONTH(" + dateExpression + ")";
    }

    @Override
    public String getYearFunction(String dateExpression) {
        return "YEAR(" + dateExpression + ")";
    }

    @Override
    public boolean supportsPaging(QueryOptions options) {
        return false;
    }
}
