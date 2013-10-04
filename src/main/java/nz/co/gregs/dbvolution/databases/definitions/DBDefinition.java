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
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 *
 * @author gregorygraham
 */
public abstract class DBDefinition {

    public abstract String getDateFormattedForQuery(Date date);

    public String formatColumnName(String columnName) {
        return columnName;
    }

    public String beginStringValue() {
        return "'";
    }

    public String endStringValue() {
        return "'";
    }

    public String beginNumberValue() {
        return "";
    }

    public String endNumberValue() {
        return "";
    }

    /**
     *
     * Formats the table and column name pair correctly for this database
     *
     * This should be used for column names in the select query
     *
     * e.g table, column => TABLE.COLUMN
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public String formatTableAndColumnName(String tableName, String columnName) {
        return formatTableName(tableName) + "." + formatColumnName(columnName);
    }

    public String formatTableName(String tableName) {
        return tableName;
    }

    /**
     *
     * Specifies the column alias used within the JDBC ResultSet to identify the
     * column.
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public String formatColumnNameForResultSet(String tableName, String columnName) {
        String formattedName = formatTableAndColumnName(tableName, columnName).replaceAll("\\.", "__");
        return ("DB" + formattedName.hashCode()).replaceAll("-", "_");
    }

    public String formatTableAndColumnNameForSelectClause(String tableName, String columnName) {
        return formatTableAndColumnName(tableName, columnName) + " " + formatColumnNameForResultSet(tableName, columnName);
    }

    public String safeString(String toString) {
        return toString.replaceAll("'", "''");
    }

    /**
     *
     * returns the required SQL to begin a line within the Where Clause
     *
     * usually, but not always " and "
     *
     * @return
     */
    public String beginAndLine() {
        return " and ";
    }

    public String getDropTableStart() {
        return "DROP TABLE ";
    }

    public String getCreateTablePrimaryKeyClauseStart() {
        return ",PRIMARY KEY (";
    }

    public String getCreateTablePrimaryKeyClauseMiddle() {
        return ", ";
    }

    public String getCreateTablePrimaryKeyClauseEnd() {
        return ")";
    }

    public String getCreateTableStart() {
        return "CREATE TABLE ";
    }

    public String getCreateTableColumnsStart() {
        return "(";
    }

    public String getCreateTableColumnsSeparator() {
        return ", ";
    }

    public String getCreateTableColumnsNameAndTypeSeparator() {
        return " ";
    }

    public Object getCreateTableColumnsEnd() {
        return ")";
    }

    public String toLowerCase(String string) {
        return " lower(" + string + ")";
    }

    public String beginInsertLine() {
        return "INSERT INTO ";
    }

    public String endInsertLine() {
        return ";";
    }

    public String beginInsertColumnList() {
        return "(";
    }

    public String endInsertColumnList() {
        return ") ";
    }

    public String beginDeleteLine() {
        return "DELETE FROM ";
    }

    public String endDeleteLine() {
        return ";";
    }

    public String getEqualsComparator() {
        return " = ";
    }

    public String getNotEqualsComparator() {
        return " <> ";
    }

    public String beginWhereClause() {
        return " WHERE ";
    }

    public String beginUpdateLine() {
        return "UPDATE ";
    }

    public String beginSetClause() {
        return " SET ";
    }

    public String getStartingSetSubClauseSeparator() {
        return "";
    }

    public String getSubsequentSetSubClauseSeparator() {
        return ",";
    }

    public String getStartingOrderByClauseSeparator() {
        return "";
    }

    public String getSubsequentOrderByClauseSeparator() {
        return ",";
    }

    public String getFalseOperation() {
        return " 1=0 ";
    }

    public String getTrueOperation() {
        return " 1=1 ";
    }

    public String getNull() {
        return " NULL ";
    }

    public String beginSelectStatement() {
        return " SELECT ";
    }

    public String beginFromClause() {
        return " FROM ";
    }

    public Object endSQLStatement() {
        return ";";
    }

    public String getStartingSelectSubClauseSeparator() {
        return "";
    }

    public String getSubsequentSelectSubClauseSeparator() {
        return ", ";
    }

    public String countStarClause() {
        return " COUNT(*) ";
    }

    /**
     * Provides an opportunity for the definition to insert a row limiting statement before the query
     * 
     * for example H2DB uses SELECT TOP 10 ... FROM ... WHERE ... ;
     * 
     * Based on the example for H2DB this method should return " TOP 10 "
     * 
     * If the database does not support row limiting 
     * this method should throw an exception when rowLimit is not null
     * 
     * If the database does not limit rows during the select clause 
     * this method should return ""
     * 
     * @param rowLimit
     * @return
     */
    abstract public Object getLimitRowsSubClauseDuringSelectClause(Long rowLimit);

    public String beginOrderByClause() {
        return " ORDER BY ";
    }

    public String endOrderByClause() {
        return " ";
    }

    public Object getOrderByDirectionClause(Boolean sortOrder) {
        if (sortOrder == null) {
            return "";
        } else if (sortOrder) {
            return " ASC ";
        } else {
            return " DESC ";
        }
    }

    public String getStartingJoinOperationSeparator() {
        return "";
    }

    public String getSubsequentJoinOperationSeparator() {
        return beginAndLine();
    }

    public String beginInnerJoin() {
        return " INNER JOIN ";
    }

    public String beginLeftOuterJoin() {
        return " LEFT OUTER JOIN ";
    }

    public String beginFullOuterJoin() {
        return " FULL OUTER JOIN ";
    }

    public String beginOnClause() {
        return " ON( ";
    }

    public String endOnClause() {
        return " ) ";
    }

    public Object getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
        return qdt.getSQLDatatype();
    }

    /**
     * Provides an opportunity for the definition to insert a row limiting statement after the query
     * 
     * for example MySQL/MariaDB use SELECT ... FROM ... WHERE ... LIMIT 10 ;
     * 
     * Based on the example for MySQL/MariaDB this method should return " LIMIT 10 "
     * 
     * If the database does not support row limiting 
     * this method should throw an exception when rowLimit is not null
     * 
     * If the database does not limit rows after the where clause 
     * this method should return ""
     * 
     * @param rowLimit
     * @return
     */
    abstract public Object getLimitRowsSubClauseAfterWhereClause(Long rowLimit);

    /**
     *
     * The place holder for variables inserted into a prepared statement, usually " ? "
     * 
     * @return 
     */
    public String getPreparedVariableSymbol() {
        return " ? ";
    }
}
