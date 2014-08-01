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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AutoIncrementFieldClassAndDatatypeMismatch;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.QueryOptions;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 *
 * @author gregorygraham
 */
public abstract class DBDefinition {

	/**
	 * Tranforms the Date instance into a SQL snippet that can be used as a date
	 * in a query.
	 *
	 * <p>
	 * For instance the date might be transformed into a string like "
	 * DATETIME('2013-03-23 00:00:00') "
	 *
	 * @param date
	 * @return the date formatted as a string that the database will correctly
	 * interpret as a date.
	 */
	public abstract String getDateFormattedForQuery(Date date);

	/**
	 * Formats the raw column name to the required convention of the database.
	 *
	 * <p>
	 * The default implementation does not change the column name.
	 *
	 * @param columnName
	 * @return the column name formatted for the database.
	 */
	public String formatColumnName(String columnName) {
		return columnName;
	}

	/**
	 * Returns the standard beginning of a string value in the database.
	 *
	 * <p>
	 * The default method returns "'", that is a single quote.
	 *
	 * @return the formatting required at the beginning of a string value.
	 */
	public String beginStringValue() {
		return "'";
	}

	/**
	 * Returns the standard ending of a string value in the database.
	 *
	 * <p>
	 * The default method returns "'", that is a single quote.
	 *
	 * @return the formatting required at the end of a string value.
	 */
	public String endStringValue() {
		return "'";
	}

	/**
	 * Returns the standard beginning of a number value in the database.
	 *
	 * <p>
	 * The default method returns "", that is an empty string.
	 *
	 * @return the formatting required at the beginning of a number value.
	 */
	public String beginNumberValue() {
		return "";
	}

	/**
	 * Returns the standard end of a number value in the database.
	 *
	 * <p>
	 * The default method returns "", that is an empty string.
	 *
	 * @return the formatting required at the end of a number value.
	 */
	public String endNumberValue() {
		return "";
	}

	/**
	 *
	 * Formats the table and column name pair correctly for this database.
	 *
	 * <p>
	 * This should only be used for column names in the select query when
	 * aliases are not being used. Which is probably never.
	 * <p>
	 * e.g table, column => TABLE.COLUMN
	 *
	 * @param table
	 * @param columnName
	 * @return a string of the table and column name for the select clause
	 */
	public String formatTableAndColumnName(DBRow table, String columnName) {
		return formatTableName(table) + "." + formatColumnName(columnName);
	}

	/**
	 *
	 * Formats the table alias and column name pair correctly for this database.
	 * <p>
	 * This should be used for column names in the select query.
	 * <p>
	 * e.g table, column => TABLEALIAS.COLUMN
	 *
	 * @param table
	 * @param columnName
	 * @return a string of the table and column name for the select clause
	 */
	public String formatTableAliasAndColumnName(RowDefinition table, String columnName) {
		return getTableAlias(table) + "." + formatColumnName(columnName);
	}

	/**
	 *
	 * Formats the table and column name pair correctly for this database
	 *
	 * This should be used for column names in the select clause, that is to say
	 * between SELECT and FROM
	 *
	 * e.g table, column => TABLEALIAS.COLUMN COLUMNALIAS
	 *
	 * @param table
	 * @param columnName
	 * @return a string of the table and column name for the select clause
	 */
	public String formatTableAliasAndColumnNameForSelectClause(DBRow table, String columnName) {
		final String tableAliasAndColumn = formatTableAliasAndColumnName(table, columnName);
		return tableAliasAndColumn + " " + formatForColumnAlias(tableAliasAndColumn);
	}

	/**
	 * Formats the table name correctly for this database.
	 *
	 * <p>
	 * Used wherever a table alias is inappropriate, for instance UPDATE
	 * statements.
	 *
	 * @param table
	 * @return a string of the table name formatted for this database definition
	 */
	public String formatTableName(DBRow table) {
		return table.getTableName();
	}

	/**
	 * Provides the column name as named in the SELECT clause and ResultSet.
	 *
	 * <p>
	 * This is the column alias that matches the result to the query. It must be
	 * consistent, unique, and deterministic.
	 *
	 * @param table
	 * @param columnName
	 * @return the table alias and the column name formatted correctly for this
	 * database.
	 */
	public String formatColumnNameForDBQueryResultSet(RowDefinition table, String columnName) {
		final String actualName = formatTableAliasAndColumnName(table, columnName);
		return formatForColumnAlias(actualName);
	}

	/**
	 * Apply standard formatting of the column alias to avoid issues with the
	 * database's column naming issues.
	 *
	 * @param actualName
	 * @return the column alias formatted for this database.
	 */
	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return ("DB" + formattedName.hashCode()).replaceAll("-", "_");
	}

	/**
	 * Apply necessary transformations on the string to avoid it being used for
	 * an SQL injection attack.
	 *
	 * <p>
	 * The default method changes every single quote (') into 2 single quotes
	 * ('').
	 *
	 * @param toString
	 * @return the string value safely escaped for use in an SQL query.
	 */
	public String safeString(String toString) {
		return toString.replaceAll("'", "''");
	}

	/**
	 *
	 * returns the required SQL to begin a line within the WHERE or ON Clause
	 * for conditions.
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
	 * returns the required SQL to begin a line within the WHERE or ON Clause
	 * for conditions.
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
	 * returns the required SQL to begin a line within the WHERE or ON Clause
	 * for joins.
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

	/**
	 * Indicates that the database does not accept named GROUP BY columns and
	 * the query generator should create the GROUP BY clause using indexes
	 * instead.
	 *
	 * @return TRUE if the database needs indexes for the group by columns,
	 * FALSE otherwise.
	 */
	public boolean prefersIndexBasedGroupByClause() {
		return false;
	}

	/**
	 * Returns the start of an AND line for this database.
	 *
	 * @return " AND " or the equivalent for this database.
	 */
	public String beginAndLine() {
		return " AND ";
	}

	/**
	 * Returns the start of an OR line for this database.
	 *
	 * @return " OR " or the equivalent for this database.
	 */
	public String beginOrLine() {
		return " OR ";
	}

	/**
	 * Provides the start of the DROP TABLE expression for this database.
	 *
	 * @return "DROP TABLE " or equivalent for the database.
	 */
	public String getDropTableStart() {
		return "DROP TABLE ";
	}

	/**
	 * Returns the start of the PRIMARY KEY clause of the CREATE TABLE
	 * statement.
	 *
	 * <p>
	 * This is the clause within the column definition clause after the columns
	 * themselves, i.e. CREATE TABLE tab (col integer<b>, PRIMARY KEY(col)</b>)
	 *
	 * @return ", PRIMARY KEY (" or the equivalent for this database.
	 */
	public String getCreateTablePrimaryKeyClauseStart() {
		return ",PRIMARY KEY (";
	}

	/**
	 * Returns the separator between the columns in the PRIMARY KEY clause of
	 * the CREATE TABLE statement.
	 *
	 * <p>
	 * This is the clause within the column definition clause after the columns
	 * themselves, i.e. CREATE TABLE tab (col integer<b>, PRIMARY KEY(col)</b>)
	 *
	 * @return ", " or the equivalent for this database.
	 */
	public String getCreateTablePrimaryKeyClauseMiddle() {
		return ", ";
	}

	/**
	 * Returns the conclusion of the PRIMARY KEY clause of the CREATE TABLE
	 * statement.
	 *
	 * <p>
	 * This is the clause within the column definition clause after the columns
	 * themselves, i.e. CREATE TABLE tab (col integer<b>, PRIMARY KEY(col)</b>)
	 *
	 * @return ")" or the equivalent for this database.
	 */
	public String getCreateTablePrimaryKeyClauseEnd() {
		return ")";
	}

	/**
	 * Returns the start of the CREATE TABLE statement.
	 *
	 * @return "CREATE TABLE " or the equivalent for this database.
	 */
	public String getCreateTableStart() {
		return "CREATE TABLE ";
	}

	/**
	 * Returns the start of the column list within the CREATE TABLE statement.
	 *
	 * <p>
	 * This is the clause within the CREATE TABLE that defines the columns
	 * themselves, i.e. CREATE TABLE tab <b>(col integer, PRIMARY KEY(col))</b>
	 *
	 * @return "(" or the equivalent for this database.
	 */
	public String getCreateTableColumnsStart() {
		return "(";
	}

	/**
	 * Returns the separator between column definitions in the column list of
	 * the CREATE TABLE statement.
	 *
	 * <p>
	 * This is the clause within the CREATE TABLE that defines the columns
	 * themselves, i.e. CREATE TABLE tab <b>(col integer, PRIMARY KEY(col))</b>
	 *
	 * @return ", " or the equivalent for this database.
	 */
	public String getCreateTableColumnsSeparator() {
		return ", ";
	}

	/**
	 * Returns the separator between the column name and the column datatype
	 * within the column definitions in the column list of the CREATE TABLE
	 * statement.
	 *
	 * <p>
	 * This is the clause within the CREATE TABLE that defines the columns
	 * themselves, i.e. CREATE TABLE tab <b>(col integer, PRIMARY KEY(col))</b>
	 *
	 * @return " " or the equivalent for this database.
	 */
	public String getCreateTableColumnsNameAndTypeSeparator() {
		return " ";
	}

	/**
	 * Returns the end of the column list within the CREATE TABLE statement.
	 *
	 * <p>
	 * This is the clause within the CREATE TABLE that defines the columns
	 * themselves, i.e. CREATE TABLE tab <b>(col integer, PRIMARY KEY(col))</b>
	 *
	 * @return ")" or the equivalent for this database.
	 */
	public Object getCreateTableColumnsEnd() {
		return ")";
	}

	/**
	 * Wraps the SQL snippet provided in the LOWER operator of the database.
	 *
	 * @param sql
	 * @return " lower("+string+")"
	 */
	public String toLowerCase(String sql) {
		return " lower(" + sql + ")";
	}

	/**
	 * Returns the beginning of an INSERT statement for this database.
	 *
	 * @return "INSERT INTO " or equivalent.
	 */
	public String beginInsertLine() {
		return "INSERT INTO ";
	}

	/**
	 * Returns the end of an INSERT statement for this database.
	 *
	 * @return ";" or equivalent.
	 */
	public String endInsertLine() {
		return ";";
	}

	/**
	 * Returns the beginning of the column list of an INSERT statement for this
	 * database.
	 *
	 * @return "(" or equivalent.
	 */
	public String beginInsertColumnList() {
		return "(";
	}

	/**
	 * Returns the end of the column list of an INSERT statement for this
	 * database.
	 *
	 * @return ") " or equivalent.
	 */
	public String endInsertColumnList() {
		return ") ";
	}

	/**
	 * Returns the beginning of a DELETE statement for this database.
	 *
	 * @return "DELETE FROM " or equivalent.
	 */
	public String beginDeleteLine() {
		return "DELETE FROM ";
	}

	/**
	 * Returns the end of a DELETE statement for this database.
	 *
	 * @return ";" or equivalent.
	 */
	public String endDeleteLine() {
		return ";";
	}

	/**
	 * The EQUALS operator for this database.
	 *
	 * @return " = " or equivalent
	 */
	public String getEqualsComparator() {
		return " = ";
	}

	/**
	 * The NOT EQUALS operator for this database.
	 *
	 * @return " <> " or equivalent
	 */
	public String getNotEqualsComparator() {
		return " <> ";
	}

	/**
	 * Returns the beginning of a WHERE clause for this database.
	 *
	 * @return " WHERE " or equivalent.
	 */
	public String beginWhereClause() {
		return " WHERE ";
	}

	/**
	 * Returns the beginning of an UPDATE statement for this database.
	 *
	 * @return "UPDATE " or equivalent.
	 */
	public String beginUpdateLine() {
		return "UPDATE ";
	}

	/**
	 * Returns the beginning of a SET clause of an UPDATE statement for this
	 * database.
	 *
	 * @return " SET " or equivalent.
	 */
	public String beginSetClause() {
		return " SET ";
	}

	/**
	 * Returns the initial separator of a SET sub-clause of an UPDATE statement
	 * for this database.
	 *
	 * @return "" or equivalent.
	 */
	public String getStartingSetSubClauseSeparator() {
		return "";
	}

	/**
	 * Returns the subsequent separator of a SET sub-clause of an UPDATE
	 * statement for this database.
	 *
	 * @return "," or equivalent.
	 */
	public String getSubsequentSetSubClauseSeparator() {
		return ",";
	}

	/**
	 * Returns the initial separator of a ORDER BY sub-clause of a SELECT
	 * statement for this database.
	 *
	 * @return "" or equivalent.
	 */
	public String getStartingOrderByClauseSeparator() {
		return "";
	}

	/**
	 * Returns the subsequent separator of a ORDER BY sub-clause of a SELECT
	 * statement for this database.
	 *
	 * @return "," or equivalent.
	 */
	public String getSubsequentOrderByClauseSeparator() {
		return ",";
	}

	/**
	 * Returns the initial clause of a WHERE clause of a SELECT statement for
	 * this database.
	 *
	 * <p>
	 * DBvolution inserts a constant operation to every WHERE clause to simplify
	 * the production of the query. This method returns a condition that always
	 * evaluates to true.
	 *
	 * @return a SQL snippet representing a TRUE operation.
	 * @see #getTrueOperation()
	 */
	public String getWhereClauseBeginningCondition() {
		return getTrueOperation();
	}

	/**
	 * Returns the initial clause of a WHERE clause of a SELECT statement for
	 * this database.
	 *
	 * <p>
	 * DBvolution inserts a constant operation to every WHERE clause to simplify
	 * the production of the query. This method checks the options parameter and
	 * returns a TRUE operation or a FALSE operation depending on the query
	 * requirements.
	 *
	 * @param options
	 * @return the required initial condition.
	 * @see #getTrueOperation()
	 * @see #getFalseOperation()
	 */
	public String getWhereClauseBeginningCondition(QueryOptions options) {
		if (options.isMatchAllConditions()) {
			return getTrueOperation();
		} else {
			return getFalseOperation();
		}
	}

	/**
	 * An SQL snippet that always evaluates to FALSE for this database.
	 *
	 * @return " 1=0 " or equivalent
	 */
	public String getFalseOperation() {
		return " 1=0 ";
	}

	/**
	 * An SQL snippet that always evaluates to TRUE for this database.
	 *
	 * @return " 1=1 " or equivalent
	 */
	public String getTrueOperation() {
		return " 1=1 ";
	}

	/**
	 * An SQL snippet that represents NULL for this database.
	 *
	 * @return " NULL " or equivalent
	 */
	public String getNull() {
		return " NULL ";
	}

	/**
	 * Returns the beginning of a SELECT statement for this database.
	 *
	 * @return "SELECT " or equivalent.
	 */
	public String beginSelectStatement() {
		return " SELECT ";
	}

	/**
	 * Returns the beginning of the FROM clause of a SELECT statement for this
	 * database.
	 *
	 * @return "FROM " or equivalent.
	 */
	public String beginFromClause() {
		return " FROM ";
	}

	/**
	 * Returns the default ending of an SQL statement for this database.
	 *
	 * @return ";" or equivalent.
	 */
	public Object endSQLStatement() {
		return ";";
	}

	/**
	 * Returns the initial separator of the column list sub-clause of a SELECT
	 * statement for this database.
	 *
	 * @return "" or equivalent.
	 */
	public String getStartingSelectSubClauseSeparator() {
		return "";
	}

	/**
	 * Returns the subsequent separator of the column list sub-clause of a
	 * SELECT statement for this database.
	 *
	 * @return "," or equivalent.
	 */
	public String getSubsequentSelectSubClauseSeparator() {
		return ",";
	}

	/**
	 * The COUNT(*) clause for this database.
	 *
	 * @return "COUNT(*)" or equivalent.
	 */
	public String countStarClause() {
		return " COUNT(*) ";
	}

	/**
	 * Provides an opportunity for the definition to insert a row limiting
	 * statement before the query.
	 * <p>
	 * for example H2DB uses SELECT TOP 10 ... FROM ... WHERE ... ;
	 * <p>
	 * Based on the example for H2DB this method should return " TOP 10 "
	 * <p>
	 * If the database does not support row limiting this method should throw an
	 * exception when rowLimit is not null
	 * <p>
	 * The default implementation returns "".
	 *
	 * @param options
	 * @return a string for the row limit sub-clause or ""
	 */
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return "";
	}

	/**
	 * Returns the beginning of the ORDER BY clause of a SELECT statement for this
	 * database.
	 *
	 * @return " ORDER BY " or equivalent.
	 */
	public String beginOrderByClause() {
		return " ORDER BY ";
	}

	/**
	 * Returns the end of the ORDER BY clause of a SELECT statement for this
	 * database.
	 *
	 * @return " " or equivalent.
	 */
	public String endOrderByClause() {
		return " ";
	}

	/**
	 * Returns the appropriate ascending or descending keyword for this database given the sort order.
	 *
	 * @param sortOrder
	 * @return " ASC " for TRUE, " DESC " for false or equivalent
	 */
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

	public String getTableAlias(RowDefinition tabRow) {
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

	/**
	 * Indicates that this database supports the JDBC generated keys API.
	 *
	 * <p>
	 * Generated keys are the preferred method for retrieving auto-increment
	 * primary keys.
	 *
	 * <p>
	 * Alternatively the DBDefinition can overload {@link #supportsRetrievingLastInsertedRowViaSQL()
	 * } and {@link #getRetrieveLastInsertedRowSQL() }
	 *
	 * <p>
	 * If both {@link #supportsGeneratedKeys(nz.co.gregs.dbvolution.query.QueryOptions)
	 * } and {@link #supportsRetrievingLastInsertedRowViaSQL() } return false
	 * DBvolution will not retrieve auto-incremented primary keys.
	 *
	 * @param options
	 * @return TRUE if this database supports the generated keys API, FLASE
	 * otherwise.
	 */
	public boolean supportsGeneratedKeys(QueryOptions options) {
		return true;
	}

	public String getTruncFunctionName() {
		return "trunc";
	}

	public String doTruncTransform(String firstString, String secondString) {
		return getTruncFunctionName() + "(" + firstString + ", " + secondString + ")";
	}

	public String doStringEqualsTransform(String firstString, String secondString) {
		return firstString + " = " + secondString;
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

	public String getColumnAutoIncrementSuffix() {
		return " GENERATED BY DEFAULT AS IDENTITY ";
	}

	public boolean usesTriggerBasedIdentities() {
		return false;
	}

	public List<String> getTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return new ArrayList<String>();
	}

	public Object getSQLTypeAndModifiersOfDBDatatype(PropertyWrapper field) {
		if (field.isAutoIncrement()) {
			if (propertyWrapperConformsToAutoIncrementType(field)) {
				if (hasSpecialAutoIncrementType()) {
					return getSpecialAutoIncrementType();
				} else {
					return getSQLTypeOfDBDatatype(field) + getColumnAutoIncrementSuffix();
				}
			} else {
				throw new AutoIncrementFieldClassAndDatatypeMismatch(field);
			}
		} else {
			return getSQLTypeOfDBDatatype(field);
		}
	}

	public String getPrimaryKeySequenceName(String table, String column) {
		return table + "_" + column + "dsq";
	}

	public String getPrimaryKeyTriggerName(String table, String column) {
		return table + "_" + column + "dtg";
	}

	protected boolean hasSpecialAutoIncrementType() {
		return false;
	}

	protected boolean propertyWrapperConformsToAutoIncrementType(PropertyWrapper field) {
		final QueryableDatatype qdt = field.getQueryableDatatype();
		return (qdt instanceof DBNumber) || (qdt instanceof DBInteger);
	}

	protected String getSpecialAutoIncrementType() {
		return "";
	}

	public boolean prefersTrailingPrimaryKeyDefinition() {
		return true;
	}

	public boolean prefersLargeObjectsReadAsBase64CharacterStream() {
		return false;
	}

	public boolean prefersLargeObjectsReadAsBytes() {
		return false;
	}

	public boolean prefersLargeObjectsReadAsCLOB() {
		return false;
	}

	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTRING("
				+ originalString
				+ " FROM "
				+ start
				+ (length.trim().isEmpty() ? "" : " FOR " + length)
				+ ") ";
	}

	public boolean prefersLargeObjectsSetAsCharacterStream() {
		return false;
	}

	public boolean prefersLargeObjectsSetAsBase64String() {
		return false;
	}

	public String getGreatestOfFunctionName() {
		return " GREATEST ";
	}

	public String getLeastOfFunctionName() {
		return " LEAST ";
	}

	public boolean prefersDatesReadAsStrings() {
		return false;
	}

	public DateFormat getDateGetStringFormat() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	/**
	 * Provides an opportunity to tweak the generated DBTableField before
	 * creating the Java classes
	 *
	 * @param dbTableField the current field being processed by
	 * DBTableClassGenerator
	 */
	@SuppressWarnings("empty-statement")
	public void sanityCheckDBTableField(DBTableField dbTableField) {
		;
	}

	/**
	 * Indicates whether this DBDefinition supports retrieving the primary key
	 * of the last inserted row using SQL.
	 *
	 * <p>
	 * Preferably the database should support
	 * {@link #supportsGeneratedKeys(nz.co.gregs.dbvolution.query.QueryOptions) generated keys}
	 * but if it doesn't this and {@link #getRetrieveLastInsertedRowSQL() }
	 * allow the DBDefinition to provide raw SQL for retrieving the last created
	 * primary key.
	 *
	 * <p>
	 * The database should support either generated keys or last inserted row
	 * SQL.
	 *
	 * <p>
	 * If both {@link #supportsGeneratedKeys(nz.co.gregs.dbvolution.query.QueryOptions)
	 * } and {@link #supportsRetrievingLastInsertedRowViaSQL() } return false
	 * DBvolution will not retrieve auto-incremented primary keys.
	 *
	 * <p>
	 * Originally provided for the SQLite-JDBC driver.
	 *
	 * @return TRUE if the database supports retrieving the last generated key
	 * using a SQL script, FALSE otherwise.
	 */
	public boolean supportsRetrievingLastInsertedRowViaSQL() {
		return false;
	}

	public String getRetrieveLastInsertedRowSQL() {
		return "";
	}

	public String getEmptyString() {
		return beginStringValue() + endStringValue();
	}

	/**
	 * Indicates whether the database supports the standard DEGREES function.
	 *
	 * <p>
	 * The default implementation returns TRUE.
	 *
	 * <p>
	 * If the database does not support the standard function then the
	 * definition may override {@link #doDegreesTransform(java.lang.String) } to
	 * implement the required functionality.
	 *
	 * @return TRUE if the database supports the standard DEGREES function,
	 * otherwise FALSE.
	 */
	public boolean supportsDegreesFunction() {
		return true;
	}

	/**
	 * Indicates whether the database supports the standard RADIANS function.
	 *
	 * <p>
	 * The default implementation returns TRUE.
	 *
	 * <p>
	 * If the database does not support the standard function then the
	 * definition may override {@link #doRadiansTransform(java.lang.String) } to
	 * implement the required functionality.
	 *
	 * @return TRUE if the database supports the standard RADIANS function,
	 * otherwise FALSE.
	 */
	public boolean supportsRadiansFunction() {
		return true;
	}

	/**
	 * Implements the degrees to radians transformation using simple maths.
	 *
	 * <p>
	 * If the database does not support the standard RADIANS function this
	 * method provides another method of providing the function.
	 *
	 * @param degreesSQL
	 * @return the degrees expression transformed into a radians expression
	 */
	public String doRadiansTransform(String degreesSQL) {
		return " (" + degreesSQL + ") * 0.0174532925 ";
	}

	/**
	 * Implements the radians to degrees transformation using simple maths.
	 *
	 * <p>
	 * If the database does not support the standard DEGREES function this
	 * method provides another method of providing the function.
	 *
	 * @param radiansSQL
	 * @return the radians expression transformed into a degrees expression
	 */
	public String doDegreesTransform(String radiansSQL) {
		return " " + radiansSQL + " * 57.2957795 ";
	}

	public String getExpFunctionName() {
		return "EXP";
	}

	public boolean supportsExpFunction() {
		return true;
	}

	public boolean supportsStandardDeviationFunction() {
		return true;
	}

	public boolean supportsLeftTrimFunction() {
		return true;
	}

	public String doLeftTrimFunction(String toSQLString) {
		return "LTRIM(" + toSQLString + ")";
	}

}
