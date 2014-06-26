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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.QueryOptions;
import nz.co.gregs.dbvolution.query.RowDefinition;

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
	 * @param table
	 * @param columnName
	 * @return a string of the table and column name for the select clause
	 */
	public String formatTableAndColumnName(DBRow table, String columnName) {
		return formatTableName(table) + "." + formatColumnName(columnName);
	}

	public String formatTableAliasAndColumnName(RowDefinition table, String columnName) {
		return getTableAlias(table) + "." + formatColumnName(columnName);
	}

	public String formatTableAliasAndColumnNameForSelectClause(DBRow table, String columnName) {
		final String tableAliasAndColumn = formatTableAliasAndColumnName(table, columnName);
		return tableAliasAndColumn + " " + formatForColumnAlias(tableAliasAndColumn);//formatColumnNameForDBQueryResultSet(table, columnName);
	}

	public String formatTableName(DBRow table) {
		return table.getTableName();
	}

	/**
	 *
	 * Specifies the column alias used within the JDBC ResultSet to identify the
	 * column.
	 *
	 * @param table
	 * @param columnName
	 * @return a string of the column alias for the select clause
	 */
	public String formatColumnNameForResultSet(DBRow table, String columnName) {
		final String actualName = formatTableAndColumnName(table, columnName);
		return formatForColumnAlias(actualName);
	}

	public String formatColumnNameForDBQueryResultSet(RowDefinition table, String columnName) {
		final String actualName = formatTableAliasAndColumnName(table, columnName);
		return formatForColumnAlias(actualName);
	}

	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return ("DB" + formattedName.hashCode()).replaceAll("-", "_");
	}

//    public String formatTableAndColumnNameForSelectClause(DBRow table, String columnName) {
//        return formatTableAndColumnName(table, columnName) + " " + formatColumnNameForResultSet(table, columnName);
//    }
	public String safeString(String toString) {
		return toString.replaceAll("'", "''");
	}

	/**
	 *
	 * returns the required SQL to begin a line within the WHERE or ON Clause for conditions.
	 *
	 * usually, but not always " and "
	 *
	 * @return a string for the start of a where clause line
	 */
	public String beginWhereClauseLine() {
		return beginAndLine();
	}

	/**
	 *
	 * returns the required SQL to begin a line within the WHERE or ON Clause for conditions.
	 *
	 * usually, but not always " and "
	 *
	 * @param options
	 * @return a string for the start of a where clause line
	 */
	public String beginConditionClauseLine(QueryOptions options) {
		if (options.isMatchAllConditions()) {
			return beginAndLine();
		} else {
			return beginOrLine();
		}
	}
	
	/**
	 *
	 * returns the required SQL to begin a line within the WHERE or ON Clause for joins.
	 *
	 * usually, but not always " and "
	 *
	 * @param options
	 * @return a string for the start of a where clause line
	 */
	public String beginJoinClauseLine(QueryOptions options) {
		if (options.isMatchAllRelationships()) {
			return beginAndLine();
		} else {
			return beginOrLine();
		}
	}


	public boolean prefersIndexBasedGroupByClause() {
		return false;
	}

	public String beginAndLine() {
		return " AND ";
	}

	public String beginOrLine() {
		return " OR ";
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

	public String getWhereClauseBeginningCondition() {
		return getTrueOperation();
	}

	public String getWhereClauseBeginningCondition(QueryOptions options) {
		if (options.isMatchAllConditions()) {
			return getTrueOperation();
		} else {
			return getFalseOperation();
		}
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
	 * Provides an opportunity for the definition to insert a row limiting
	 * statement before the query
	 *
	 * for example H2DB uses SELECT TOP 10 ... FROM ... WHERE ... ;
	 *
	 * Based on the example for H2DB this method should return " TOP 10 "
	 *
	 * If the database does not support row limiting this method should throw an
	 * exception when rowLimit is not null
	 *
	 * If the database does not limit rows during the select clause this method
	 * should return ""
	 *
	 * @param options
	 * @return a string for the row limit sub-clause or ""
	 */
	abstract public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options);

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

	public String getSubsequentJoinOperationSeparator(QueryOptions options) {
		return beginConditionClauseLine(options);
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

	public final String getSQLTypeOfDBDatatype(PropertyWrapper field) {
		return getSQLTypeOfDBDatatype(field.getQueryableDatatype());
	}

	/**
	 * Supplied to allow the DBDefintion to override the standard QDT datatype.
	 *
	 * <p>
	 * When the
	 *
	 * @param qdt
	 * @return the databases type for the QDT as a string
	 */
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		return qdt.getSQLDatatype();
	}

	/**
	 * Provides an opportunity for the definition to insert a row limiting
	 * statement after the query
	 *
	 * for example MySQL/MariaDB use SELECT ... FROM ... WHERE ... LIMIT 10 ;
	 *
	 * Based on the example for MySQL/MariaDB this method should return " LIMIT
	 * 10 "
	 *
	 * If the database does not support row limiting this method should throw an
	 * exception when rowLimit is not null
	 *
	 * If the database does not limit rows after the where clause this method
	 * should return ""
	 *
	 * @param options
	 * @return the row limiting sub-clause or ""
	 */
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		int rowLimit = options.getRowLimit();
		Integer pageNumber = options.getPageIndex();
		if (rowLimit < 1) {
			return "";
		} else {
			long offset = pageNumber * rowLimit;

			return "LIMIT " + rowLimit + " OFFSET " + offset;
		}
	}

	/**
	 *
	 * The place holder for variables inserted into a prepared statement,
	 * usually " ? "
	 *
	 * @return the place holder for variables as a string
	 */
	public String getPreparedVariableSymbol() {
		return " ? ";
	}

	public boolean isColumnNamesCaseSensitive() {
		return false;
	}

	public String startMultilineComment() {
		return "/*";
	}

	public String endMultilineComment() {
		return "*/";
	}

	public String beginValueClause() {
		return " VALUES ( ";
	}

	public Object endValueClause() {
		return ")";
	}

	public String getValuesClauseValueSeparator() {
		return ",";
	}

	public String getValuesClauseColumnSeparator() {
		return ",";
	}

	public String beginTableAlias() {
		return " AS ";
	}

	public String endTableAlias() {
		return " ";
	}

	public Object getTableAlias(RowDefinition tabRow) {
		return ("_" + tabRow.getClass().getSimpleName().hashCode()).replaceAll("-", "_");
	}

	public String getCurrentDateFunctionName() {
		return " CURRENT_DATE ";
	}

	public String getCurrentTimestampFunction() {
		return " CURRENT_TIMESTAMP ";
	}

	public String getCurrentTimeFunction() {
		return " CURRENT_TIMESTAMP ";
	}

	@Deprecated
	public String getCurrentUserFunction() {
		return " CURRENT_USER ";
	}

	public String getDropDatabase(String databaseName) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("DROP DATABASE is not supported by this DBDatabase implementation");
	}

	public String doLeftTrimTransform(String enclosedValue) {
		return " LTRIM(" + enclosedValue + ") ";
	}

	public String doLowercaseTransform(String enclosedValue) {
		return " LOWER(" + enclosedValue + ") ";
	}

	public String doRightTrimTransform(String enclosedValue) {
		return " RTRIM(" + enclosedValue + " )";
	}

	public String doStringLengthTransform(String enclosedValue) {
		return " CHAR_LENGTH( " + enclosedValue + " ) ";
	}

	public String doTrimFunction(String enclosedValue) {
		return " TRIM(" + enclosedValue + ") ";
	}

	public String doUppercaseTransform(String enclosedValue) {
		return " UPPER(" + enclosedValue + ") ";
	}

	public String doConcatTransform(String firstString, String secondString) {
		return firstString + "||" + secondString;
	}

	public String getNextSequenceValueFunctionName() {
		return " NEXTVAL";
	}

	public String getConcatOperator() {
		return "||";
	}

	public String getReplaceFunctionName() {
		return "REPLACE";
	}

	public String getLeftTrimFunctionName() {
		return "LTRIM";
	}

	public String getRightTrimFunctionName() {
		return "RTRIM";
	}

	public String getLowercaseFunctionName() {
		return "LOWER";
	}

	public String getUppercaseFunctionName() {
		return "UPPER";
	}

	public String getStringLengthFunctionName() {
		return "CHAR_LENGTH";
	}

	public String getCurrentUserFunctionName() {
		return "CURRENT_USER";
	}

	public String getYearFunction(String dateExpression) {
		return "EXTRACT(YEAR FROM " + dateExpression + ")";
	}

	public String getMonthFunction(String dateExpression) {
		return "EXTRACT(MONTH FROM " + dateExpression + ")";
	}

	public String getDayFunction(String dateExpression) {
		return "EXTRACT(DAY FROM " + dateExpression + ")";
	}

	public String getHourFunction(String dateExpression) {
		return "EXTRACT(HOUR FROM " + dateExpression + ")";
	}

	public String getMinuteFunction(String dateExpression) {
		return "EXTRACT(MINUTE FROM " + dateExpression + ")";
	}

	public String getSecondFunction(String dateExpression) {
		return "EXTRACT(SECOND FROM " + dateExpression + ")";
	}

	public String getPositionFunction(String originalString, String stringToFind) {
		return "POSITION(" + stringToFind + " IN " + originalString + ")";
	}

	public String formatExpressionAlias(Object key) {
		return ("DB" + key.hashCode()).replaceAll("-", "_");
	}

	public String getIfNullFunctionName() {
		return "COALESCE";
	}

	public String getNegationFunctionName() {
		return "NOT";
	}

	public String getSubsequentGroupBySubClauseSeparator() {
		return ", ";
	}

	public String beginGroupByClause() {
		return " GROUP BY ";
	}

	public String getAverageFunctionName() {
		return "AVG";
	}

	public String getCountFunctionName() {
		return "COUNT";
	}

	public String getMaxFunctionName() {
		return "MAX";
	}

	public String getMinFunctionName() {
		return "MIN";
	}

	public String getSumFunctionName() {
		return "SUM";
	}

	public String getStandardDeviationFunctionName() {
		return "stddev";
	}

	public boolean prefersIndexBasedOrderByClause() {
		return false;
	}

	public boolean supportsPaging(QueryOptions options) {
		return true;
	}
        
	public boolean supportsGeneratedKeys(QueryOptions options) {
		return true;
	}

	public String getTruncFunctionName() {
		return "trunc";
	}

	public String doTruncTransform(String firstString, String secondString) {
		return getTruncFunctionName()+"("+firstString+", "+secondString+")";
	}

	public String doStringEqualsTransform(String firstString, String secondString) {
		return firstString+" = "+secondString;
	}

	public boolean prefersConditionsInWHEREClause() {
		return true;
	}

	public String beginReturningClause(DBRow row) {
		return "";
	}

	public String convertBitsToInteger(String columnName) {
		return columnName;
	}

}
