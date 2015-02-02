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

import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRecursiveQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AutoIncrementFieldClassAndDatatypeMismatch;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.QueryOptions;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 *
 * @author Gregory Graham
 */
public abstract class DBDefinition {

	/**
	 * Transforms the Date instance into a SQL snippet that can be used as a
	 * date in a query.
	 *
	 * <p>
	 * For instance the date might be transformed into a string like "
	 * DATETIME('2013-03-23 00:00:00') "
	 *
	 * @param date	date
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
	 * @param columnName	columnName
	 * @return the column name formatted for the database.
	 */
	public String formatColumnName(String columnName) {
		return formatNameForDatabase(columnName);
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
	 * e.g table, column =&gt; TABLE.COLUMN
	 *
	 * @param table table
	 * @param columnName columnName
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
	 * e.g table, column =&gt; TABLEALIAS.COLUMN
	 *
	 * @param table table
	 * @param columnName columnName
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
	 * e.g table, column =&gt; TABLEALIAS.COLUMN COLUMNALIAS
	 *
	 * @param table table
	 * @param columnName columnName
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
	 * @param table	table
	 * @return a string of the table name formatted for this database definition
	 */
	public String formatTableName(DBRow table) {
		return formatNameForDatabase(table.getTableName());
	}

	/**
	 * Provides the column name as named in the SELECT clause and ResultSet.
	 *
	 * <p>
	 * This is the column alias that matches the result to the query. It must be
	 * consistent, unique, and deterministic.
	 *
	 * @param table table
	 * @param columnName columnName
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
	 * @param actualName	actualName
	 * @return the column alias formatted for this database.
	 */
	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return formatNameForDatabase("DB" + formattedName.hashCode()).replaceAll("-", "_");
	}

	/**
	 * Apply standard object name transformations required by the database.
	 *
	 * <p>
	 * This methods helps support database specific naming rules by allowing
	 * post-processing of the object names to conform to the rules.
	 *
	 * @param sqlObjectName
	 * @return
	 */
	protected String formatNameForDatabase(final String sqlObjectName) {
		return sqlObjectName;
	}

	/**
	 * Apply standard formatting of the expression alias to avoid issues with
	 * the database's alias naming issues.
	 *
	 * @param key	key
	 * @return the alias of the key formatted correctly.
	 */
	public String formatExpressionAlias(Object key) {
		return ("DB" + key.hashCode()).replaceAll("-", "_");
	}

	/**
	 * Apply necessary transformations on the string to avoid it being used for
	 * an SQL injection attack.
	 *
	 * <p>
	 * The default method changes every single quote (') into 2 single quotes
	 * ('').
	 *
	 * @param toString	toString
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
	 * @param options	options
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
	 * @param options	options
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
	 * @param sql	sql
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
	 * @return " &lt;&gt; " or equivalent
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
	 * @param options	options
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
	 * @param options	options
	 * @return a string for the row limit sub-clause or ""
	 */
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return "";
	}

	/**
	 * Returns the beginning of the ORDER BY clause of a SELECT statement for
	 * this database.
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
	 * Returns the appropriate ascending or descending keyword for this database
	 * given the sort order.
	 *
	 * @param sortOrder	sortOrder
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

	/**
	 * Used during the creation of an ANSI join to add a table with a normal, or
	 * "required" join.
	 *
	 * @return the default implementation returns " INNER JOIN ".
	 */
	public String beginInnerJoin() {
		return " INNER JOIN ";
	}

	/**
	 * Used during the creation of an ANSI join to add an optional table using a
	 * Left Outer Join.
	 *
	 * @return the default implementation returns " LEFT OUTER JOIN "
	 */
	public String beginLeftOuterJoin() {
		return " LEFT OUTER JOIN ";
	}

	/**
	 * Used during the creation of an ANSI join to add an optional table using a
	 * Full Outer Join.
	 *
	 * @return the default implementation returns " FULL OUTER JOIN ".
	 */
	public String beginFullOuterJoin() {
		return " FULL OUTER JOIN ";
	}

	/**
	 * Used during the creation of an ANSI join to add the criteria of an
	 * optional table using an ON clause.
	 *
	 * @return the default implementation returns " ON( ".
	 */
	public String beginOnClause() {
		return " ON( ";
	}

	/**
	 * Used during the creation of an ANSI join to complete the criteria of an
	 * optional table by closing the ON clause.
	 *
	 * @return the default implementation returns " ) ".
	 */
	public String endOnClause() {
		return " ) ";
	}

	private String getSQLTypeOfDBDatatype(PropertyWrapper field) {
		return getSQLTypeOfDBDatatype(field.getQueryableDatatype());
	}

	/**
	 * Supplied to allow the DBDefintion to override the standard QDT datatype.
	 *
	 * <p>
	 * When the
	 *
	 * @param qdt	qdt
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
	 * @param options	options
	 * @return the row limiting sub-clause or ""
	 */
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		int rowLimit = options.getRowLimit();
		Integer pageNumber = options.getPageIndex();
		if (rowLimit > 0 && supportsPagingNatively(options)) {
			long offset = pageNumber * rowLimit;
			return "LIMIT " + rowLimit + " OFFSET " + offset;
		} else {
			return "";
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

	/**
	 * Indicates whether this database distinguishes between upper and lowercase
	 * letters in column names.
	 *
	 * @return the default implementation returns FALSE.
	 */
	public boolean isColumnNamesCaseSensitive() {
		return false;
	}

	/**
	 * Used during output of BLOB columns to avoid complications in some
	 * scenarios.
	 *
	 * @return the default implementation returns "/*"
	 */
	public String startMultilineComment() {
		return "/*";
	}

	/**
	 * Used during output of BLOB columns to avoid complications in some
	 * scenarios.
	 *
	 * @return the default implementation returns "*\/"
	 */
	public String endMultilineComment() {
		return "*/";
	}

	/**
	 * Used within DBInsert to start the VALUES clause of the INSERT statement.
	 *
	 * @return the default implementation returns " VALUES( ".
	 */
	public String beginValueClause() {
		return " VALUES ( ";
	}

	/**
	 * Used within DBInsert to end the VALUES clause of the INSERT statement.
	 *
	 * @return the default implementation returns " ) ".
	 */
	public Object endValueClause() {
		return ")";
	}

	/**
	 * Used within DBInsert to separate the values within the VALUES clause of
	 * the INSERT statement.
	 *
	 * @return the default implementation returns ",".
	 */
	public String getValuesClauseValueSeparator() {
		return ",";
	}

	/**
	 * Used within DBInsert to separate the columns within the INSERT clause of
	 * the INSERT statement.
	 *
	 * @return the default implementation returns ",".
	 */
	public String getValuesClauseColumnSeparator() {
		return ",";
	}

	/**
	 * Used during the creation of ANSI queries to separate the table and its
	 * alias.
	 *
	 * @return the default implementation returns " AS ".
	 */
	public String beginTableAlias() {
		return " AS ";
	}

	/**
	 * Used during the creation of ANSI queries to conclude the table alias.
	 *
	 * @return the default implementation returns " ".
	 */
	public String endTableAlias() {
		return " ";
	}

	/**
	 * Transforms the table name into the unique and deterministic table alias.
	 *
	 * @param tabRow	tabRow
	 * @return the table alias.
	 */
	public String getTableAlias(RowDefinition tabRow) {
		return formatTableAlias("" + tabRow.getClass().getSimpleName().hashCode());
	}

	/**
	 * Formats the suggested table alias provided by DBvolution for the
	 * particular database..
	 *
	 * @param suggestedTableAlias	suggestedTableAlias
	 * @return the table alias.
	 */
	public String formatTableAlias(String suggestedTableAlias) {
		return "_" + suggestedTableAlias.replaceAll("-", "_");
	}

	/**
	 * Defines the function used to get the current date (excluding time) from
	 * the database.
	 *
	 * @return the default implementation returns " CURRENT_DATE "
	 */
	protected String getCurrentDateOnlyFunctionName() {
		return " CURRENT_DATE";
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	protected String getCurrentDateTimeFunction() {
		return " CURRENT_TIMESTAMP ";
	}

	/**
	 * Creates the CURRENTTIMESTAMP function for this database.
	 *
	 * @return a String of the SQL required to get the CurrentDateTime value.
	 */
	public String doCurrentDateTimeTransform() {
		return getCurrentDateTimeFunction();
	}

	/**
	 * Defines the function used to get the current time from the database.
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	protected String getCurrentTimeFunction() {
		return " CURRENT_TIMESTAMP ";
	}

	/**
	 * Creates the CURRENTTIME function for this database.
	 *
	 * @return a String of the SQL required to get the CurrentTime value.
	 */
	public String doCurrentTimeTransform() {
		return getCurrentTimeFunction();
	}

	/**
	 * Provides the SQL statement required to drop the named database.
	 *
	 * @param databaseName	databaseName
	 * @return the default implementation does not support dropping databases.
	 *
	 */
	public String getDropDatabase(String databaseName) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("DROP DATABASE is not supported by this DBDatabase implementation");
	}

	/**
	 * Wraps the provided SQL snippet in a statement that performs trims all
	 * spaces from the left of the value of the snippet.
	 *
	 * @param enclosedValue	enclosedValue
	 * @return SQL snippet
	 */
	public String doLeftTrimTransform(String enclosedValue) {
		return " LTRIM(" + enclosedValue + ") ";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that changes the value of
	 * the snippet to lowercase characters.
	 *
	 * @param enclosedValue	enclosedValue
	 * @return SQL snippet
	 */
	public String doLowercaseTransform(String enclosedValue) {
		return " LOWER(" + enclosedValue + ") ";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that trims all spaces from
	 * the right of the value of the snippet.
	 *
	 * @param enclosedValue	enclosedValue
	 * @return SQL snippet
	 */
	public String doRightTrimTransform(String enclosedValue) {
		return " RTRIM(" + enclosedValue + " )";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that the length of the
	 * value of the snippet.
	 *
	 * @param enclosedValue	enclosedValue
	 * @return SQL snippet
	 */
	public String doStringLengthTransform(String enclosedValue) {
		return " " + getStringLengthFunctionName() + "( " + enclosedValue + " ) ";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that performs trims all
	 * spaces from the left and right of the value of the snippet.
	 *
	 * @param enclosedValue	enclosedValue
	 * @return SQL snippet
	 */
	public String doTrimFunction(String enclosedValue) {
		return " TRIM(" + enclosedValue + ") ";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that changes the characters
	 * of the value of the snippet to their uppercase equivalent.
	 *
	 * @param enclosedValue	enclosedValue
	 * @return SQL snippet
	 */
	public String doUppercaseTransform(String enclosedValue) {
		return " UPPER(" + enclosedValue + ") ";
	}

	/**
	 * Wraps the provided SQL snippets in a statement that joins the two
	 * snippets into one SQL snippet.
	 *
	 * @param firstString firstString
	 * @param secondString secondString
	 * @return SQL snippet
	 * @see StringExpression#append(java.lang.String)
	 * @see StringExpression#append(java.lang.Number)
	 * @see
	 * StringExpression#append(nz.co.gregs.dbvolution.expressions.StringResult)
	 * @see
	 * StringExpression#append(nz.co.gregs.dbvolution.expressions.NumberResult)
	 */
	public String doConcatTransform(String firstString, String secondString) {
		return firstString + "||" + secondString;
	}

	/**
	 * Returns the function name of the function used to return the next value
	 * of a sequence.
	 *
	 * @return "NEXTVAL"
	 * @see NumberExpression#getNextSequenceValue(java.lang.String)
	 * @see NumberExpression#getNextSequenceValue(java.lang.String,
	 * java.lang.String)
	 */
	public String getNextSequenceValueFunctionName() {
		return "NEXTVAL";
	}

	/**
	 * Returns the function name of the function used to remove all the spaces
	 * padding the end of the value.
	 *
	 * @return "RTRIM"
	 */
	public String getRightTrimFunctionName() {
		return "RTRIM";
	}

	/**
	 * Returns the function name of the function used to change the case of all
	 * the letters in the value to lower case.
	 *
	 * <p>
	 * Usually databases only support lower and upper case functions for ASCII
	 * characters. Support for change the case of unicode characters is
	 * dependent on the underlying database.
	 *
	 * @return "LOWER"
	 */
	public String getLowercaseFunctionName() {
		return "LOWER";
	}

	/**
	 * Returns the function name of the function used to change the case of all
	 * the letters in the value to upper case.
	 *
	 * <p>
	 * Usually databases only support lower and upper case functions for ASCII
	 * characters. Support for change the case of unicode characters is
	 * dependent on the underlying database.
	 *
	 * @return "UPPER"
	 */
	public String getUppercaseFunctionName() {
		return "UPPER";
	}

	/**
	 * Returns the function name of the function used to determine the number of
	 * characters in the value.
	 *
	 * <p>
	 * DBvolution tries to ensure that the character length of a value is equal
	 * to the character length of an equivalent Java String.
	 *
	 * <p>
	 * That is to say: DBV.charlength() === java.lang.String.length()
	 *
	 * @return "LOWER"
	 */
	public String getStringLengthFunctionName() {
		return "CHAR_LENGTH";
	}

	/**
	 * Returns the function used to determine the username of the user currently
	 * logged into the database.
	 *
	 * <p>
	 * Usually this is the same username supplied when you created the
	 * DBDatabase instance.
	 *
	 * @return "CURRENT_USER'
	 */
	public String getCurrentUserFunctionName() {
		return "CURRENT_USER";
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the year part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * @return a SQL snippet that will produce the year of the supplied date.
	 */
	public String doYearTransform(String dateExpression) {
		return "EXTRACT(YEAR FROM " + dateExpression + ")";
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the month part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * @return a SQL snippet that will produce the month of the supplied date.
	 */
	public String doMonthTransform(String dateExpression) {
		return "EXTRACT(MONTH FROM " + dateExpression + ")";
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the day part of the date.
	 *
	 * <p>
	 * Day in this sense is the number of the day within the month: that is the
	 * 23 part of Monday 25th of August 2014
	 *
	 * @param dateExpression	dateExpression
	 * @return a SQL snippet that will produce the day of the supplied date.
	 */
	public String doDayTransform(String dateExpression) {
		return "EXTRACT(DAY FROM " + dateExpression + ")";
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the hour part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * @return a SQL snippet that will produce the hour of the supplied date.
	 */
	public String doHourTransform(String dateExpression) {
		return "EXTRACT(HOUR FROM " + dateExpression + ")";
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the minute part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * @return a SQL snippet that will produce the minute of the supplied date.
	 */
	public String doMinuteTransform(String dateExpression) {
		return "EXTRACT(MINUTE FROM " + dateExpression + ")";
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the second part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * @return a SQL snippet that will produce the second of the supplied date.
	 */
	public String doSecondTransform(String dateExpression) {
		return "EXTRACT(SECOND FROM " + dateExpression + ")";
	}

	/**
	 * Transforms an SQL snippet into an SQL snippet that provides the index of
	 * the string to find.
	 *
	 * @param originalString originalString
	 * @param stringToFind stringToFind
	 * @return a SQL snippet that will produce the index of the find string.
	 */
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "POSITION(" + stringToFind + " IN " + originalString + ")";
	}

	/**
	 * Provides the function name of the COALESCE, IFNULL, or NVL function.
	 *
	 * <p>
	 * This provides the function name for this database that transforms a NULL
	 * into another value.
	 *
	 * @return "COALESCE"
	 */
	public String getIfNullFunctionName() {
		return "COALESCE";
	}

	/**
	 * Returns the function name of the function that negates boolean values.
	 *
	 * @return "NOT"
	 */
	public String getNegationFunctionName() {
		return "NOT";
	}

	/**
	 * Provides the separator between GROUP BY clause items.
	 *
	 * @return ", "
	 */
	public String getSubsequentGroupBySubClauseSeparator() {
		return ", ";
	}

	/**
	 * Provides the key words and syntax that start the GROUP BY clause.
	 *
	 * @return " GROUP BY "
	 */
	public String beginGroupByClause() {
		return " GROUP BY ";
	}

	/**
	 * Provides the function of the function that provides the average of a
	 * selection.
	 *
	 * @return "AVG"
	 */
	public String getAverageFunctionName() {
		return "AVG";
	}

	/**
	 * Provides the function of the function that provides the count of items in
	 * a selection.
	 *
	 * @return "COUNT"
	 */
	public String getCountFunctionName() {
		return "COUNT";
	}

	/**
	 * Provides the function of the function that provides the maximum value in
	 * a selection.
	 *
	 * @return "MAX"
	 */
	public String getMaxFunctionName() {
		return "MAX";
	}

	/**
	 * Provides the function of the function that provides the minimum value in
	 * a selection.
	 *
	 * @return "MIN"
	 */
	public String getMinFunctionName() {
		return "MIN";
	}

	/**
	 * Provides the function of the function that provides the sum of a
	 * selection.
	 *
	 * @return "SUM"
	 */
	public String getSumFunctionName() {
		return "SUM";
	}

	/**
	 * Provides the function of the function that provides the standard
	 * deviation of a selection.
	 *
	 * @return "stddev"
	 */
	public String getStandardDeviationFunctionName() {
		return "stddev";
	}

	/**
	 * Indicates whether the database prefers (probably exclusively) the ORDER
	 * BY clause to use column indexes rather than column names.
	 *
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersIndexBasedOrderByClause() {
		return false;
	}

	/**
	 * Indicates whether the the database supports a form of paging natively.
	 *
	 * <p>
	 * Databases that don't support paging will have paging handled by the java
	 * side. Unfortunately this causes some problems as the entire dataset will
	 * be retrieved with the first call, making the first call expensive in time
	 * and memory. Subsequent calls will be more efficient but that probably
	 * won't help your developers.
	 *
	 * @param options	options
	 * @return the default implementation returns TRUE.
	 */
	public boolean supportsPagingNatively(QueryOptions options) {
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
	 * If both {@link #supportsGeneratedKeys()
	 * } and {@link #supportsRetrievingLastInsertedRowViaSQL() } return false
	 * DBvolution will not retrieve auto-incremented primary keys.
	 *
	 * @return TRUE if this database supports the generated keys API, FLASE
	 * otherwise.
	 */
	public boolean supportsGeneratedKeys() {
		return true;
	}

	/**
	 * Provides the name of the function that removes the decimal part of a real
	 * number.
	 *
	 * @return "trunc"
	 */
	public String getTruncFunctionName() {
		return "trunc";
	}

	/**
	 * Transforms 2 SQL snippets that represent a real number and a integer into
	 * a real number with the decimal places reduced to the integer.
	 *
	 * <p>
	 * 0 decimal places transforms the real number into an integer.
	 *
	 * @param realNumberExpression realNumberExpression
	 * @param numberOfDecimalPlacesExpression numberOfDecimalPlacesExpression
	 * @return an expression that reduces the realNumberExpression to only the
	 * number of decimal places in numberOfDecimalPlacesExpression.
	 */
	public String doTruncTransform(String realNumberExpression, String numberOfDecimalPlacesExpression) {
		return getTruncFunctionName() + "(" + realNumberExpression + ", " + numberOfDecimalPlacesExpression + ")";
	}

	/**
	 * Returns the SQL required to directly compare 2 strings.
	 *
	 * @param firstSQLExpression firstSQLExpression
	 * @param secondSQLExpression secondSQLExpression
	 * @return SQL snippet comparing the 2 strings
	 */
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return firstSQLExpression + " = " + secondSQLExpression;
	}

	/**
	 * Transforms a bit expression into an integer expression.
	 *
	 * <p>
	 * Used to allow comparison of bit columns in some databases.
	 *
	 * @param bitExpression	bitExpression
	 * @return the transformation necessary to transform bitExpression into an
	 * integer expression in the SQL.
	 */
	public String doBitsToIntegerTransform(String bitExpression) {
		return bitExpression;
	}

	/**
	 * Transforms a integer expression into an bit expression.
	 *
	 * <p>
	 * Used to allow comparison of integer columns in some databases.
	 *
	 * @param bitExpression	bitExpression
	 * @return the transformation necessary to transform bitExpression into an
	 * integer expression in the SQL.
	 */
	public String doIntegerToBitTransform(String bitExpression) {
		return bitExpression;
	}

	/**
	 * Returns the suffix added to a column definition to support
	 * auto-incrementing a column.
	 *
	 * @return " GENERATED BY DEFAULT AS IDENTITY "
	 */
	public String getColumnAutoIncrementSuffix() {
		return " GENERATED BY DEFAULT AS IDENTITY ";
	}

	/**
	 * Indicates whether the database prefers to use triggers and sequences to
	 * maintain auto-incrementing identities.
	 *
	 * @return the default implementation returns FALSE.
	 * @see Oracle11DBDefinition#prefersTriggerBasedIdentities()
	 */
	public boolean prefersTriggerBasedIdentities() {
		return false;
	}

	/**
	 * Provides all the SQL necessary to create a trigger and sequence based
	 * auto-incrementing identity.
	 *
	 * @param db db
	 * @param table table
	 * @param column column
	 * @return the default implementation returns an empty list.
	 * @see
	 * Oracle11DBDefinition#getTriggerBasedIdentitySQL(nz.co.gregs.dbvolution.DBDatabase,
	 * java.lang.String, java.lang.String)
	 */
	public List<String> getTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return new ArrayList<String>();
	}

	/**
	 * Provides the SQL type and modifiers required to create the column
	 * associated with the provided field.
	 *
	 * @param field the field of the column being created.
	 * @return the datatype and appropriate modifiers.
	 * @see PropertyWrapper#isAutoIncrement()
	 * @see
	 * #propertyWrapperConformsToAutoIncrementType(nz.co.gregs.dbvolution.internal.properties.PropertyWrapper)
	 * @see #hasSpecialAutoIncrementType()
	 * @see #getSpecialAutoIncrementType()
	 * @see
	 * #getSQLTypeOfDBDatatype(nz.co.gregs.dbvolution.internal.properties.PropertyWrapper)
	 * @see #getColumnAutoIncrementSuffix()
	 * @see AutoIncrementFieldClassAndDatatypeMismatch
	 */
	public final String getSQLTypeAndModifiersOfDBDatatype(PropertyWrapper field) {
		if (field.isAutoIncrement()) {
			if (propertyWrapperConformsToAutoIncrementType(field)) {
				if (hasSpecialAutoIncrementType()) {
					return getSpecialAutoIncrementType();
				} else if (hasSpecialPrimaryKeyTypeForDBDatatype(field)) {
					return getSpecialPrimaryKeyTypeOfDBDatatype(field) + getColumnAutoIncrementSuffix();
				} else {
					return getSQLTypeOfDBDatatype(field) + getColumnAutoIncrementSuffix();
				}
			} else {
				throw new AutoIncrementFieldClassAndDatatypeMismatch(field);
			}
		}
		if (field.isPrimaryKey()) {
			if (hasSpecialPrimaryKeyTypeForDBDatatype(field)) {
				return getSpecialPrimaryKeyTypeOfDBDatatype(field);
			} else {
				return getSQLTypeOfDBDatatype(field);
			}
		} else {
			return getSQLTypeOfDBDatatype(field);
		}
	}

	/**
	 * Provides the name that DBvolution will use for the sequence of a
	 * trigger-based auto-increment implementation.
	 *
	 * @param table table
	 * @param column column
	 * @return the name of the primary key sequence to be created.
	 */
	public String getPrimaryKeySequenceName(String table, String column) {
		return formatNameForDatabase(table + "_" + column + "dsq");
	}

	/**
	 * Provides the name that DBvolution will use for the trigger of a
	 * trigger-based auto-increment implementation.
	 *
	 * @param table table
	 * @param column column
	 * @return the name of the trigger to be created.
	 */
	public String getPrimaryKeyTriggerName(String table, String column) {
		return formatNameForDatabase(table + "_" + column + "dtg");
	}

	/**
	 * Indicates whether the database uses a special type for it's
	 * auto-increment columns.
	 *
	 * @return the default implementation returns FALSE.
	 */
	protected boolean hasSpecialAutoIncrementType() {
		return false;
	}

	/**
	 * Indicates whether field provided can be used as a auto-incrementing
	 * column in this database
	 *
	 *
	 * @return true if the QDT field can be used with this database's
	 * autoincrement feature.
	 */
	private boolean propertyWrapperConformsToAutoIncrementType(PropertyWrapper field) {
		final QueryableDatatype qdt = field.getQueryableDatatype();
		return propertyWrapperConformsToAutoIncrementType(qdt);
	}

	/**
	 * Indicates whether {@link QueryableDatatype} provided can be used as a
	 * auto-incrementing column in this database
	 *
	 * @param qdt	qdt
	 * @return the default implementation returns TRUE for DBNumber or DBString,
	 * FALSE otherwise.
	 */
	protected boolean propertyWrapperConformsToAutoIncrementType(QueryableDatatype qdt) {
		return (qdt instanceof DBNumber) || (qdt instanceof DBInteger);
	}

	/**
	 * Provides the special auto-increment type used by this database if it has
	 * one.
	 *
	 * @return the default implementation returns ""
	 */
	protected String getSpecialAutoIncrementType() {
		return "";
	}

	/**
	 * Indicates whether the database prefers the primary key to be defined at
	 * the end of the CREATE TABLE statement.
	 *
	 * @return the default implementation returns TRUE.
	 */
	public boolean prefersTrailingPrimaryKeyDefinition() {
		return true;
	}

	/**
	 * Indicates whether the database requires LargeObjects to be encoded as
	 * Base64 CLOBS using the CharacterStream method to read the value.
	 *
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsReadAsBase64CharacterStream() {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getBytes()
	 * method.
	 *
	 * @return the default implementation returns FALSE
	 */
	public boolean prefersLargeObjectsReadAsBytes() {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getClob()
	 * method.
	 *
	 * @return the default implementation returns FALSE
	 */
	public boolean prefersLargeObjectsReadAsCLOB() {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getBlob()
	 * method.
	 *
	 * @return the default implementation returns FALSE
	 */
	public boolean prefersLargeObjectsReadAsBLOB() {
		return false;
	}

	/**
	 * Transforms the arguments into a SQL snippet that produces a substring of
	 * the originalString from the start for length characters.
	 *
	 * @param originalString originalString
	 * @param start start
	 * @param length length
	 * @return an expression that will produce an appropriate substring of the
	 * originalString.
	 */
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTRING("
				+ originalString
				+ " FROM "
				+ start
				+ (length.trim().isEmpty() ? "" : " FOR " + length)
				+ ") ";
	}

	/**
	 * Indicates that the database prefers Large Object values to be set using
	 * the setCharacterStream method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream() } and
	 * {@link #prefersLargeObjectsSetAsBase64String()} return FALSE, DBvolution
	 * will use the setBinaryStream method to set the value.
	 *
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsSetAsCharacterStream() {
		return false;
	}

	/**
	 * Indicates that the database prefers Large Object values to be set using
	 * the setBLOB method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream() } and
	 * {@link #prefersLargeObjectsSetAsBase64String()} return FALSE, DBvolution
	 * will use the setBinaryStream method to set the value.
	 *
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsSetAsBLOB() {
		return false;
	}

	/**
	 * Indicates that the database prefers Large Object values to be set using
	 * the setCharacterStream method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream() } and
	 * {@link #prefersLargeObjectsSetAsBase64String()} return FALSE, DBvolution
	 * will use the setBinaryStream method to set the value.
	 *
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsSetAsBase64String() {
		return false;
	}

	/**
	 * Provides the name of the function that will choose the largest value from
	 * a list of options.
	 *
	 * @return " GREATEST "
	 */
	public String getGreatestOfFunctionName() {
		return " GREATEST ";
	}

	/**
	 * Provides the name of the function that will choose the smallest value
	 * from a list of options.
	 *
	 * @return " LEAST "
	 */
	public String getLeastOfFunctionName() {
		return " LEAST ";
	}

	/**
	 * Provides Cheeseburger.
	 *
	 * @return a cheeseburger.
	 */
	public String getCheezBurger() {
		return " LOL! De beez dont makes cheezburger.";
	}

	/**
	 * Indicates whether the database prefers date values to be read as Strings.
	 *
	 * <p>
	 * Normally dates are read as dates but this method switches DBvolution to
	 * using a text mode.
	 *
	 * @return the default implementation returns false.
	 * @see #parseDateFromGetString()
	 */
	public boolean prefersDatesReadAsStrings() {
		return false;
	}

	/**
	 * returns the date format used when reading dates as strings.
	 *
	 * <p>
	 * Normally dates are read as dates but this method allows DBvolution to
	 * read them using a text mode.
	 *
	 * @param getStringDate a date retrieved with {@link ResultSet#getString(java.lang.String)
	 * }
	 *
	 * @return return the date format required to interpret strings as dates.
	 * @throws java.text.ParseException
	 * @see #prefersDatesReadAsStrings()
	 */
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(getStringDate);
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
	 * {@link #supportsGeneratedKeys() generated keys} but if it doesn't this
	 * and {@link #getRetrieveLastInsertedRowSQL()
	 * }
	 * allow the DBDefinition to provide raw SQL for retrieving the last created
	 * primary key.
	 *
	 * <p>
	 * The database should support either generated keys or last inserted row
	 * SQL.
	 *
	 * <p>
	 * If both {@link #supportsGeneratedKeys()
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

	/**
	 * Provides the SQL required to retrieve that last inserted row if {@link #supportsRetrievingLastInsertedRowViaSQL()
	 * } returns TRUE.
	 *
	 * @return the default implementation returns "".
	 */
	public String getRetrieveLastInsertedRowSQL() {
		return "";
	}

	/**
	 * Provides the database's version of an empty string.
	 *
	 * @return '' or the database's equivalent.
	 */
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
	 * @param degreesSQL	degreesSQL
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
	 * @param radiansSQL	radiansSQL
	 * @return the radians expression transformed into a degrees expression
	 */
	public String doDegreesTransform(String radiansSQL) {
		return " " + radiansSQL + " * 57.2957795 ";
	}

	/**
	 * Provides the name of the function that raises e to the power of the
	 * provided value.
	 *
	 * @return "EXP"
	 */
	public String getExpFunctionName() {
		return "EXP";
	}

	/**
	 * Indicates whether the database has a built-in EXP function.
	 *
	 * <p>
	 * If the database does not support EXP, then DBvolution will use an
	 * expression to calculate the value.
	 *
	 * @return the default implementation returns TRUE.
	 */
	public boolean supportsExpFunction() {
		return true;
	}

	/**
	 * Indicates whether the database supports the standard deviation function.
	 *
	 * <p>
	 * DBvolution will use a terrible approximation of standard deviation if the
	 * database neither has a built-in function nor supports another method of
	 * implementing it.
	 *
	 * @return the default implementation returns TRUE.
	 */
	public boolean supportsStandardDeviationFunction() {
		return true;
	}

	/**
	 * Indicates whether the database supports the modulus function.
	 *
	 * @return the default implementation returns TRUE.
	 */
	public boolean supportsModulusFunction() {
		return true;
	}

	/**
	 * Implements the integer division remainder (mod) function.
	 *
	 * @param firstNumber firstNumber
	 * @param secondNumber secondNumber
	 * @return the SQL required to get the integer division remainder.
	 */
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return "(" + firstNumber + ") % (" + secondNumber + ")";
	}

	/**
	 * Indicates whether the database differentiates between NULL and an empty
	 * string.
	 *
	 * <p>
	 * This method has been replaced by {@link DBDatabase#supportsDifferenceBetweenNullAndEmptyString()
	 * }.
	 *
	 * @return the default implementation returns TRUE.
	 */
	@Deprecated
	public Boolean supportsDifferenceBetweenNullAndEmptyString() {
		return true;
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfSeconds seconds to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfSeconds numberOfSeconds
	 * @return an SQL snippet
	 */
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfSeconds + ") SECOND )";
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfMinutes minutes to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfMinutes numberOfMinutes
	 * @return an SQL snippet
	 */
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfMinutes + ") MINUTE )";
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfdays days to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfDays numberOfDays
	 * @return an SQL snippet
	 */
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfDays + ") DAY )";
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfHours hours to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfHours numberOfHours
	 * @return an SQL snippet
	 */
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfHours + ") HOUR )";
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfWeeks weeks to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfWeeks numberOfWeeks
	 * @return an SQL snippet
	 */
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfWeeks + ") WEEK )";
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfMonths months to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfMonths numberOfMonths
	 * @return an SQL snippet
	 */
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfMonths + ") MONTH )";
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfYears years to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfYears numberOfYears
	 * @return an SQL snippet
	 */
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfYears + ") YEAR )";
	}

	/**
	 * Transform a Java Boolean into the equivalent in an SQL snippet.
	 *
	 * @param boolValue	boolValue
	 * @return an SQL snippet
	 */
	public String doBooleanValueTransform(Boolean boolValue) {
		return beginNumberValue() + (boolValue ? 1 : 0) + endNumberValue();
	}

	/**
	 * Indicates whether the database supports use of the "^" operator to
	 * perform boolean XOR.
	 *
	 * @return TRUE if the database supports "^" as XOR, FALSE otherwise.
	 */
	public boolean supportsXOROperator() {
		return false;
	}

	/**
	 * Transform a set of SQL snippets into the database's version of the LEAST
	 * function.
	 *
	 * <p>
	 * The LEAST function takes a list of values and returns the smallest value.
	 *
	 * <p>
	 * Not to be confused with the MIN aggregate function.
	 *
	 * @param strs	strs
	 * @return a String of the SQL required to find the smallest value in the
	 * list provided.
	 */
	public String doLeastOfTransformation(List<String> strs) {
		if (supportsLeastOfNatively()) {
			StringBuilder sql = new StringBuilder(getLeastOfFunctionName() + "(");
			String comma = "";
			for (String str : strs) {
				sql.append(comma).append(str);
				comma = ", ";
			}
			return sql.append(")").toString();
		} else {
			return fakeLeastOfTransformation(strs);
		}
	}

	private String fakeLeastOfTransformation(List<String> strs) {
		String sql = "";
		String prevCase = null;
		if (strs.size() == 1) {
			return strs.get(0);
		}
		for (String str : strs) {
			if (prevCase == null) {
				prevCase = "(" + str + ")";
			} else {
				sql = "(case when " + str + " < " + prevCase + " then " + str + " else " + prevCase + " end)";
			}
		}
		return sql;
	}

	/**
	 * Transform a set of SQL snippets into the database's version of the
	 * GREATEST function.
	 *
	 * <p>
	 * The GREATEST function takes a list of values and returns the largest
	 * value.
	 *
	 * <p>
	 * Not to be confused with the MAX aggregate function.
	 *
	 * @param strs	strs
	 * @return a String of the SQL required to get the largest value in the
	 * supplied list.
	 */
	public String doGreatestOfTransformation(List<String> strs) {
		if (supportsGreatestOfNatively()) {
			StringBuilder sql = new StringBuilder(getGreatestOfFunctionName() + "(");
			String comma = "";
			for (String str : strs) {
				sql.append(comma).append(str);
				comma = ", ";
			}
			return sql.append(")").toString();
		} else {
			return fakeGreatestOfTransformation(strs);
		}
	}

	private String fakeGreatestOfTransformation(List<String> strs) {
		String sql = "";
		String prevCase = null;
		if (strs.size() == 1) {
			return strs.get(0);
		}
		for (String str : strs) {
			if (prevCase == null) {
				prevCase = "(" + str + ")";
			} else {
				sql = "(case when " + str + " > " + prevCase + " then " + str + " else " + prevCase + " end)";
			}
		}
		return sql;
	}

	/**
	 * Returns the SQL string used to replace the value of a substring with
	 * another string.
	 *
	 * @param withinString search within this value
	 * @param findString search for this value
	 * @param replaceString replace with this value
	 * @return "REPLACE(withinString, findString, replaceString)"
	 * @see StringExpression#replace(java.lang.String, java.lang.String)
	 * @see StringExpression#replace(java.lang.String,
	 * nz.co.gregs.dbvolution.expressions.StringResult)
	 * @see
	 * StringExpression#replace(nz.co.gregs.dbvolution.expressions.StringResult,
	 * java.lang.String)
	 * @see
	 * StringExpression#replace(nz.co.gregs.dbvolution.expressions.StringResult,
	 * nz.co.gregs.dbvolution.expressions.StringResult)
	 */
	public String doReplaceTransform(String withinString, String findString, String replaceString) {
		return "REPLACE(" + withinString + "," + findString + "," + replaceString + ")";
	}

	/**
	 * Transforms a SQL snippet of a number expression into a character
	 * expression for this database.
	 *
	 * @param numberExpression	numberExpression
	 * @return a String of the SQL required to transform the number supplied
	 * into a character or String type.
	 */
	public String doNumberToStringTransform(String numberExpression) {
		return doConcatTransform(getEmptyString(), numberExpression);
	}

	/**
	 * Creates the CURRENTDATE function for this database.
	 *
	 * @return a String of the SQL required to get the CurrentDateOnly value.
	 */
	public String doCurrentDateOnlyTransform() {
		return getCurrentDateOnlyFunctionName().trim() + "()";
	}

	/**
	 * Convert the boolean array of bit values into the SQL equivalent.
	 *
	 * @param booleanArray	booleanArray
	 * @return SQL snippet.
	 */
	public String doBitsValueTransform(boolean[] booleanArray) {
		String result = "";
		String separator = "ARRAY(";
		for (boolean c : booleanArray) {
			if (c) {
				result += separator + "true";
			} else {
				result += separator + "false";
			}
			separator = ",";
		}
		if (!separator.equals("(")) {
			result += ")";
		}
		return result;
	}

	/**
	 * Convert the 2 SQL date values into a difference in days.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * @return SQL
	 */
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('DAY', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Convert the 2 SQL date values into a difference in days.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * @return SQL
	 */
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	/**
	 * Convert the 2 SQL date values into a difference in months.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * @return SQL
	 */
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('MONTH', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Convert the 2 SQL date values into a difference in years.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * @return SQL
	 */
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('YEAR', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Convert the 2 SQL date values into a difference in hours.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * @return SQL
	 */
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('HOUR', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Convert the 2 SQL date values into a difference in minutes.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * @return SQL
	 */
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('MINUTE', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Convert the 2 SQL date values into a difference in seconds.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * @return SQL
	 */
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('SECOND', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Create a foreign key clause for use in a CREATE TABLE statement from the
	 * {@link PropertyWrapper} provided.
	 *
	 * @param field	field
	 * @return The default implementation returns something like " FOREIGN KEY
	 * (column) REFERENCES table(reference_column) "
	 */
	public String getForeignKeyClauseForCreateTable(PropertyWrapper field) {
		if (field.isForeignKey()) {
			return " FOREIGN KEY (" + field.columnName() + ") REFERENCES " + field.referencedTableName() + "(" + field.referencedColumnName() + ") ";
		}
		return "";
	}

	/**
	 * Produce SQL that will provide return the second value if the first is
	 * NULL.
	 *
	 * @param possiblyNullValue possiblyNullValue
	 * @param alternativeIfNull alternativeIfNull
	 * @return SQL
	 */
	public String doStringIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return this.getIfNullFunctionName() + "(" + possiblyNullValue
				+ ","
				+ (alternativeIfNull == null ? "NULL" : alternativeIfNull)
				+ ")";
	}

	/**
	 * Produce SQL that will provide return the second value if the first is
	 * NULL.
	 *
	 * @param possiblyNullValue possiblyNullValue
	 * @param alternativeIfNull alternativeIfNull
	 * @return SQL
	 */
	public String doNumberIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return doStringIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	/**
	 * Produce SQL that will provide return the second value if the first is
	 * NULL.
	 *
	 * @param possiblyNullValue possiblyNullValue
	 * @param alternativeIfNull alternativeIfNull
	 * @return SQL
	 */
	public String doDateIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return doStringIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	/**
	 * Produce SQL that will compare the first value to all the other values
	 * using the IN operator.
	 *
	 * @param comparableValue comparableValue
	 * @param values values
	 * @return SQL similar to "comparableValue IN (value, value, value)"
	 */
	public String doInTransform(String comparableValue, List<String> values) {
		StringBuilder builder = new StringBuilder();
		builder
				.append(comparableValue)
				.append(" IN ( ");
		String separator = "";
		for (String val : values) {
			if (val != null) {
				builder.append(separator).append(val);
			}
			separator = ", ";
		}
		builder.append(")");
		return builder.toString();
	}

	/**
	 * Returns FROM clause to be used for this table.
	 *
	 * @param table
	 * @return a SQL snippet for a FROM clause.
	 */
	public String getFromClause(DBRow table) {
		String recursiveTableAlias = table.getRecursiveTableAlias();
		if (recursiveTableAlias != null) {
			return recursiveTableAlias;
		} else {
			return formatTableName(table) + beginTableAlias() + getTableAlias(table) + endTableAlias();
		}
	}

	/**
	 * The beginning of the WITH variant supported by this database.
	 *
	 * @return "WITH RECURSIVE" by default.
	 */
	public String beginWithClause() {
		return " WITH RECURSIVE ";
	}

	/**
	 * Define the table to be used during a recursive query.
	 *
	 * @param recursiveTableAlias
	 * @param recursiveColumnNames
	 * @return by default something like: ALIAS(COL1, COL2, ... )
	 */
	public String formatWithClauseTableDefinition(String recursiveTableAlias, String recursiveColumnNames) {
		return recursiveTableAlias + "(" + recursiveColumnNames + ")" + " \n";
	}

	/**
	 * Return the default preamble to the priming query of a
	 * {@link DBRecursiveQuery}.
	 *
	 * @return " AS ("
	 */
	public String beginWithClausePrimingQuery() {
		return " AS (";
	}

	/**
	 * Return the necessary connector between the priming query and the
	 * recursive query used in a {@link DBRecursiveQuery}.
	 *
	 * @return " \n UNION ALL "
	 */
	public String endWithClausePrimingQuery() {
		return " \n UNION ALL ";
	}

	/**
	 * Return the default preamble to the recursive query of a
	 * {@link DBRecursiveQuery}.
	 *
	 * @return ""
	 */
	public String beginWithClauseRecursiveQuery() {
		return "";
	}

	/**
	 * Return the default preamble to the recursive query of a
	 * {@link DBRecursiveQuery}.
	 *
	 * @return " \n ) \n"
	 */
	public String endWithClauseRecursiveQuery() {
		return " \n ) \n";
	}

	/**
	 * Return the default select clause for the final query of a
	 * {@link DBRecursiveQuery}.
	 *
	 * @param recursiveTableAlias
	 * @param recursiveAliases
	 * @return " SELECT ... FROM ... ORDER BY ... ASC; ";
	 */
	public String doSelectFromRecursiveTable(String recursiveTableAlias, String recursiveAliases) {
		return " SELECT " + recursiveAliases + ", " + getRecursiveQueryDepthColumnName() + " FROM " + recursiveTableAlias + " ORDER BY " + getRecursiveQueryDepthColumnName() + " ASC; ";
	}

	/**
	 * Indicates whether this database needs the recursive query to use table
	 * aliases.
	 *
	 * @return TRUE
	 */
	public boolean requiresRecursiveTableAlias() {
		return true;
	}

	/**
	 * Return the default name for the depth column generated during a
	 * {@link DBRecursiveQuery}.
	 *
	 * @return " DBDEPTHCOLUMN "
	 */
	public String getRecursiveQueryDepthColumnName() {
		return " DBDEPTHCOLUMN ";
	}

	/**
	 * Expresses whether the database has a particular datatype for primary key
	 * columns.
	 *
	 * @param field
	 * @return FALSE by default
	 */
	protected boolean hasSpecialPrimaryKeyTypeForDBDatatype(PropertyWrapper field) {
		return false;
	}

	/**
	 * Return the necessary SQL data type for this field to be a primary key in
	 * this database.
	 *
	 * @param field
	 * @return by default DBvolution returns the standard datatype for this
	 * field.
	 */
	protected String getSpecialPrimaryKeyTypeOfDBDatatype(PropertyWrapper field) {
		return getSQLTypeOfDBDatatype(field);
	}

	/**
	 * Indicates whether the LEASTOF operator is supported by the database or
	 * needs to be emulated.
	 *
	 * @return TRUE by default.
	 */
	protected boolean supportsLeastOfNatively() {
		return true;
	}

	/**
	 * Indicates whether the GREATESTOF operator is supported by the database or
	 * needs to be emulated.
	 *
	 * @return TRUE by default.
	 */
	protected boolean supportsGreatestOfNatively() {
		return true;
	}

	/**
	 * Indicates whether the database supports grouping by columns that don't
	 * involve any tables.
	 *
	 * @return TRUE by default.
	 */
	public boolean supportsPurelyFunctionalGroupByColumns() {
		return true;
	}

	/**
	 * Creates a pattern that will exclude system tables during DBRow class
	 * generation i.e. {@link DBTableClassGenerator}.
	 *
	 * <p>
	 * By default this method returns ".*" as system tables are not a problem
	 * for most databases.
	 *
	 * @return default is ".*" so all tables are included.
	 */
	public String getSystemTableExclusionPattern() {
		return ".*";
	}

	/**
	 * Allows the database to have a different format for the primary key column
	 * name.
	 *
	 * <p>
	 * Most databases do not have a problem with this method but PostgreSQL
	 * likes the column name to be lowercase in this particular instance.
	 *
	 * @param primaryKeyColumnName
	 * @return
	 */
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return primaryKeyColumnName;
	}

	public String doChooseTransformation(String numberToChooseWith, List<String> strs) {
		if (supportsChooseNatively()) {
			StringBuilder sql = new StringBuilder(getChooseFunctionName() + "("+numberToChooseWith);
			String comma = ", ";
			for (String str : strs) {
				sql.append(comma).append(str);
			}
			return sql.append(")").toString();
		} else {
			return fakeChooseTransformation(numberToChooseWith, strs);
		}
	}

	private String fakeChooseTransformation(String numberToChooseWith, List<String> strs) {
		String sql = "(case ";
		String prevCase = null;
		if (strs.size() == 1) {
			return strs.get(0);
		}
		String op = " <= ";
		for (int index = 0; index < strs.size(); index++) {
			String str = strs.get(index);
			if (index==strs.size()-1) {
				sql+= " else "+str +" end)";
			} else {
				sql += " when " + numberToChooseWith + op + (index+1) + " then " + str+System.getProperty("line.separator");
				op = " = ";
			}
		}
		return sql;
	}

	public String getChooseFunctionName() {
		return "";
	}

	protected boolean supportsChooseNatively() {
		return false;
	}

	public String doIfThenElseTransform(String booleanTest, String thenResult, String elseResult) {
		return "(CASE WHEN "+booleanTest+" THEN "+thenResult+" ELSE "+elseResult+" END)";
	}

	abstract public String doDayOfWeekTransform(String dateSQL) ;
}
