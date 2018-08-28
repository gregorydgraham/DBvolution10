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

import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRecursiveQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeText;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import nz.co.gregs.dbvolution.exceptions.AutoIncrementFieldClassAndDatatypeMismatch;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateRepeatExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.internal.datatypes.DateRepeatImpl;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.Line2DResult;
import org.joda.time.Period;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public abstract class DBDefinition {

	public int getNumericPrecision() {
		return DBNumber.getNumericPrecision();
	}

	public int getNumericScale() {
		return DBNumber.getNumericScale();
	}

	/**
	 * Transforms the Date instance into a SQL snippet that can be used as a date
	 * in a query.
	 *
	 * <p>
	 * For instance the date might be transformed into a string like "
	 * DATETIME('2013-03-23 00:00:00') "
	 *
	 * @param date	date
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the date formatted as a string that the database will correctly
	 * interpret as a date.
	 */
	public abstract String getDateFormattedForQuery(Date date);

	/**
	 * Transforms the specific parts of a date from their SQL snippets into a SQL
	 * snippet that can be used as a date in a query.
	 *
	 * <p>
	 * For instance the date parts might be transformed into a string like "
	 * DATETIME('2013-03-23 00:00:00') "
	 *
	 * @param years the sql representing the years part of the date
	 * @param months the sql representing the months (1-12) part of the date
	 * @param days the sql representing the days (0-31) part of the date
	 * @param minutes the sql representing the minutes (0-60) part of the date
	 * @param hours the sql representing the hours (0-24) part of the date
	 * @param seconds the sql representing the seconds (0-59) part of the date
	 * @param subsecond the sql representing the subsecond (0.0-0.9999) part of
	 * the date, precision is based on the database's limitations
	 * @param timeZoneSign + or -
	 * @param timeZoneMinuteOffSet the sql representing the hours (0-13) part of
	 * the date's time zone
	 * @param timeZoneHourOffset the sql representing the minutes (0-59) part of
	 * the date's time zone
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the date formatted as a string that the database will be correctly
	 * interpret as a date.
	 */
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return "";
	}

	/**
	 * Transforms the Date instance into UTC time zone date.
	 *
	 * @param date the local date to be rolled to UTC.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that creates this Date as a UTC date in the database.
	 */
	@SuppressWarnings("deprecation")
	public String getUTCDateFormattedForQuery(Date date) {
		Double zoneOffset = (0.0 + date.getTimezoneOffset()) / 60.0;

		int hourPart = zoneOffset.intValue() * 100;
		int minutePart = (int) ((zoneOffset - (zoneOffset.intValue())) * 60);

		return doAddMinutesTransform(doAddHoursTransform(getDateFormattedForQuery(date), "" + hourPart), "" + minutePart);
	}

	/**
	 * Formats the raw column name to the required convention of the database.
	 *
	 * <p>
	 * The default implementation does not change the column name.
	 *
	 * @param columnName	columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * This should only be used for column names in the select query when aliases
	 * are not being used. Which is probably never.
	 * <p>
	 * e.g table, column =&gt; TABLE.COLUMN
	 *
	 * @param table table
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the column alias formatted for this database.
	 */
	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return formatNameForDatabase("DB" + formattedName.hashCode()).replaceAll("-", "_");
	}

	/**
	 * Get a name for the object that can be used safely in queries.
	 *
	 * @param anObject an Object that you would like a safe name for
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the column alias formatted for this database.
	 */
	public String getTableAliasForObject(final Object anObject) {
		return formatNameForDatabase("DB" + anObject.hashCode()).replaceAll("-", "_");
	}

	/**
	 * Apply standard object name transformations required by the database.
	 *
	 * <p>
	 * This methods helps support database specific naming rules by allowing
	 * post-processing of the object names to conform to the rules.
	 *
	 * @param sqlObjectName the Java object name to be transformed into a database
	 * object name.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the object name formatted for use with this database
	 */
	protected String formatNameForDatabase(final String sqlObjectName) {
		return sqlObjectName;
	}

	/**
	 * Apply standard formatting of the expression alias to avoid issues with the
	 * database's alias naming issues.
	 *
	 * @param key	key
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the alias of the key formatted correctly.
	 */
	public String formatExpressionAlias(Object key) {
		return ("DB" + key.hashCode()).replaceAll("-", "_");
	}

	/**
	 * Apply necessary transformations on the string to avoid it being used for an
	 * SQL injection attack.
	 *
	 * <p>
	 * The default method changes every single quote (') into 2 single quotes
	 * ('').
	 *
	 * @param toString	toString
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the string value safely escaped for use in an SQL query.
	 */
	public String safeString(String toString) {
		return toString.replaceAll("'", "''");
	}

	/**
	 *
	 * returns the required SQL to begin a line within the WHERE or ON Clause for
	 * conditions.
	 *
	 * usually, but not always " and "
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a string for the start of a where clause line
	 */
	public String beginWhereClauseLine() {
		return beginAndLine();
	}

	/**
	 *
	 * returns the required SQL to begin a line within the WHERE or ON Clause for
	 * conditions.
	 *
	 * usually, but not always " and "
	 *
	 * @param options	options
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * returns the required SQL to begin a line within the WHERE or ON Clause for
	 * joins.
	 *
	 * usually, but not always " and "
	 *
	 * @param options	options
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Indicates that the database does not accept named GROUP BY columns and the
	 * query generator should create the GROUP BY clause using indexes instead.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the database needs indexes for the group by columns, FALSE
	 * otherwise.
	 */
	public boolean prefersIndexBasedGroupByClause() {
		return false;
	}

	/**
	 * Returns the start of an AND line for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " AND " or the equivalent for this database.
	 */
	public String beginAndLine() {
		return " AND ";
	}

	/**
	 * Returns the start of an OR line for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " OR " or the equivalent for this database.
	 */
	public String beginOrLine() {
		return " OR ";
	}

	/**
	 * Provides the start of the DROP TABLE expression for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "DROP TABLE " or equivalent for the database.
	 */
	public String getDropTableStart() {
		return "DROP TABLE ";
	}

	/**
	 * Returns the start of the PRIMARY KEY clause of the CREATE TABLE statement.
	 *
	 * <p>
	 * This is the clause within the column definition clause after the columns
	 * themselves, i.e. CREATE TABLE tab (col integer<b>, PRIMARY KEY(col)</b>)
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return ", PRIMARY KEY (" or the equivalent for this database.
	 */
	public String getCreateTablePrimaryKeyClauseStart() {
		return ",PRIMARY KEY (";
	}

	/**
	 * Returns the separator between the columns in the PRIMARY KEY clause of the
	 * CREATE TABLE statement.
	 *
	 * <p>
	 * This is the clause within the column definition clause after the columns
	 * themselves, i.e. CREATE TABLE tab (col integer<b>, PRIMARY KEY(col)</b>)
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return ")" or the equivalent for this database.
	 */
	public String getCreateTablePrimaryKeyClauseEnd() {
		return ")";
	}

	/**
	 * Returns the start of the CREATE TABLE statement.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "(" or the equivalent for this database.
	 */
	public String getCreateTableColumnsStart() {
		return "(";
	}

	/**
	 * Returns the separator between column definitions in the column list of the
	 * CREATE TABLE statement.
	 *
	 * <p>
	 * This is the clause within the CREATE TABLE that defines the columns
	 * themselves, i.e. CREATE TABLE tab <b>(col integer, PRIMARY KEY(col))</b>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return " lower("+string+")"
	 */
	public String toLowerCase(String sql) {
		return " lower(" + sql + ")";
	}

	/**
	 * Returns the beginning of an INSERT statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "INSERT INTO " or equivalent.
	 */
	public String beginInsertLine() {
		return "INSERT INTO ";
	}

	/**
	 * Returns the end of an INSERT statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return ") " or equivalent.
	 */
	public String endInsertColumnList() {
		return ") ";
	}

	/**
	 * Returns the beginning of a DELETE statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "DELETE FROM " or equivalent.
	 */
	public String beginDeleteLine() {
		return "DELETE FROM ";
	}

	/**
	 * Returns the end of a DELETE statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return ";" or equivalent.
	 */
	public String endDeleteLine() {
		return ";";
	}

	/**
	 * The EQUALS operator for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " = " or equivalent
	 */
	public String getEqualsComparator() {
		return " = ";
	}

	/**
	 * The NOT EQUALS operator for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " &lt;&gt; " or equivalent
	 */
	public String getNotEqualsComparator() {
		return " <> ";
	}

	/**
	 * Returns the beginning of a WHERE clause for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " WHERE " or equivalent.
	 */
	public String beginWhereClause() {
		return " WHERE ";
	}

	/**
	 * Returns the beginning of an UPDATE statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "" or equivalent.
	 */
	public String getStartingSetSubClauseSeparator() {
		return "";
	}

	/**
	 * Returns the subsequent separator of a SET sub-clause of an UPDATE statement
	 * for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "," or equivalent.
	 */
	public String getSubsequentOrderByClauseSeparator() {
		return ",";
	}

	/**
	 * Returns the initial clause of a WHERE clause of a SELECT statement for this
	 * database.
	 *
	 * <p>
	 * DBvolution inserts a constant operation to every WHERE clause to simplify
	 * the production of the query. This method returns a condition that always
	 * evaluates to true.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a SQL snippet representing a TRUE operation.
	 * @see #getTrueOperation()
	 */
	public String getWhereClauseBeginningCondition() {
		return getTrueOperation();
	}

	/**
	 * Returns the initial clause of a WHERE clause of a SELECT statement for this
	 * database.
	 *
	 * <p>
	 * DBvolution inserts a constant operation to every WHERE clause to simplify
	 * the production of the query. This method checks the options parameter and
	 * returns a TRUE operation or a FALSE operation depending on the query
	 * requirements.
	 *
	 * @param options	options
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " 1=0 " or equivalent
	 */
	public String getFalseOperation() {
		return " (1=0) ";
	}

	/**
	 * An SQL snippet that always evaluates to TRUE for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " 1=1 " or equivalent
	 */
	public String getTrueOperation() {
		return " (1=1) ";
	}

	/**
	 * An SQL snippet that represents NULL for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " NULL " or equivalent
	 */
	public String getNull() {
		return " NULL ";
	}

	/**
	 * Returns the beginning of a SELECT statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "FROM " or equivalent.
	 */
	public String beginFromClause() {
		return " FROM ";
	}

	/**
	 * Returns the default ending of an SQL statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return ";" or equivalent.
	 */
	public String endSQLStatement() {
		return ";";
	}

	/**
	 * Returns the initial separator of the column list sub-clause of a SELECT
	 * statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "" or equivalent.
	 */
	public String getStartingSelectSubClauseSeparator() {
		return "";
	}

	/**
	 * Returns the subsequent separator of the column list sub-clause of a SELECT
	 * statement for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "," or equivalent.
	 */
	public String getSubsequentSelectSubClauseSeparator() {
		return ",";
	}

	/**
	 * The COUNT(*) clause for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a string for the row limit sub-clause or ""
	 */
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return "";
	}

	/**
	 * Returns the beginning of the ORDER BY clause of a SELECT statement for this
	 * database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return " ASC " for TRUE, " DESC " for false or equivalent
	 */
	public String getOrderByDirectionClause(Boolean sortOrder) {
		if (sortOrder == null) {
			return "";
		} else if (sortOrder) {
			return getOrderByAscending();
		} else {
			return getOrderByDescending();
		}
	}

	protected String getOrderByDescending() {
		return " DESC ";
	}

	protected String getOrderByAscending() {
		return " ASC ";
	}

	/**
	 * Used during the creation of an ANSI join to add a table with a normal, or
	 * "required" join.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " LEFT OUTER JOIN "
	 */
	public String beginLeftOuterJoin() {
		return " LEFT OUTER JOIN ";
	}

	/**
	 * Used during the creation of an ANSI join to add an optional table using a
	 * Right Outer Join.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " RIGHT OUTER JOIN "
	 */
	public String beginRightOuterJoin() {
		return " RIGHT OUTER JOIN ";
	}

	/**
	 * Used during the creation of an ANSI join to add an optional table using a
	 * Full Outer Join.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " FULL OUTER JOIN ".
	 */
	public String beginFullOuterJoin() {
		return " FULL OUTER JOIN ";
	}

	/**
	 * Used during the creation of an ANSI join to add the criteria of an optional
	 * table using an ON clause.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " ) ".
	 */
	public String endOnClause() {
		return " ) ";
	}

	private String getSQLTypeOfDBDatatype(PropertyWrapper field) {
		return getDatabaseDataTypeOfQueryableDatatype(field.getQueryableDatatype());
	}

	/**
	 * Supplied to allow the DBDefintion to override the standard QDT datatype.
	 *
	 * <p>
	 * When the
	 *
	 * @param qdt	qdt
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the databases type for the QDT as a string
	 */
	protected String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		return qdt.getSQLDatatype();
	}

	/**
	 * Provides an opportunity for the definition to insert a row limiting
	 * statement after the query
	 *
	 * for example MySQL/MariaDB use SELECT ... FROM ... WHERE ... LIMIT 10 ;
	 *
	 * Based on the example for MySQL/MariaDB this method should return " LIMIT 10
	 * "
	 *
	 * If the database does not support row limiting this method should throw an
	 * exception when rowLimit is not null
	 *
	 * If the database does not limit rows after the where clause this method
	 * should return ""
	 *
	 * @param state
	 * @param options	options
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the row limiting sub-clause or ""
	 */
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
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
	 * The place holder for variables inserted into a prepared statement, usually
	 * " ? "
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns "*\/"
	 */
	public String endMultilineComment() {
		return "*/";
	}

	/**
	 * Used within DBInsert to start the VALUES clause of the INSERT statement.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " VALUES( ".
	 */
	public String beginValueClause() {
		return " VALUES ( ";
	}

	/**
	 * Used within DBInsert to end the VALUES clause of the INSERT statement.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " ) ".
	 */
	public Object endValueClause() {
		return ")";
	}

	/**
	 * Used within DBInsert to separate the values within the VALUES clause of the
	 * INSERT statement.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " AS ".
	 */
	public String beginTableAlias() {
		return " AS ";
	}

	/**
	 * Used during the creation of ANSI queries to conclude the table alias.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the table alias.
	 */
	public String getTableAlias(RowDefinition tabRow) {
		return formatTableAlias(tabRow.getTableVariantAlias());
	}

	/**
	 * Formats the suggested table alias provided by DBvolution for the particular
	 * database..
	 *
	 * @param suggestedTableAlias	suggestedTableAlias
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the table alias.
	 */
	public String formatTableAlias(String suggestedTableAlias) {
		return "_" + suggestedTableAlias.replaceAll("-", "_");
	}

	/**
	 * Defines the function used to get the current date (excluding time) from the
	 * database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " CURRENT_DATE "
	 */
	protected String getCurrentDateOnlyFunctionName() {
		return " CURRENT_DATE";
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	protected String getCurrentDateTimeFunction() {
		return " CURRENT_TIMESTAMP ";
	}

	/**
	 * Creates the CURRENTTIMESTAMP function for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the SQL required to get the CurrentDateTime value.
	 */
	public String doCurrentDateTimeTransform() {
		return getCurrentDateTimeFunction();
	}

	/**
	 * Defines the function used to get the current time from the database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	protected String getCurrentTimeFunction() {
		return " CURRENT_TIMESTAMP ";
	}

	/**
	 * Creates the CURRENTTIME function for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet
	 */
	public String doLeftTrimTransform(String enclosedValue) {
		return " LTRIM(" + enclosedValue + ") ";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that changes the value of the
	 * snippet to lowercase characters.
	 *
	 * @param enclosedValue	enclosedValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet
	 */
	public String doRightTrimTransform(String enclosedValue) {
		return " RTRIM(" + enclosedValue + " )";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that the length of the value
	 * of the snippet.
	 *
	 * @param enclosedValue	enclosedValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet
	 */
	public String doUppercaseTransform(String enclosedValue) {
		return " UPPER(" + enclosedValue + ") ";
	}

	/**
	 * Wraps the provided SQL snippets in a statement that joins the two snippets
	 * into one SQL snippet.
	 *
	 * @param firstString firstString
	 * @param secondString secondString
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet
	 * @see StringExpression#append(java.lang.String)
	 * @see StringExpression#append(java.lang.Number)
	 * @see StringExpression#append(nz.co.gregs.dbvolution.results.StringResult)
	 * @see StringExpression#append(nz.co.gregs.dbvolution.results.NumberResult)
	 */
	public String doConcatTransform(String firstString, String secondString) {
		return firstString + "||" + secondString;
	}

	/**
	 * Returns the function name of the function used to return the next value of
	 * a sequence.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "NEXTVAL"
	 */
	public String getNextSequenceValueFunctionName() {
		return "NEXTVAL";
	}

	/**
	 * Returns the function name of the function used to remove all the spaces
	 * padding the end of the value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * characters. Support for change the case of unicode characters is dependent
	 * on the underlying database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * characters. Support for change the case of unicode characters is dependent
	 * on the underlying database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * DBvolution tries to ensure that the character length of a value is equal to
	 * the character length of an equivalent Java String.
	 *
	 * <p>
	 * That is to say: DBV.charlength() === java.lang.String.length()
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Usually this is the same username supplied when you created the DBDatabase
	 * instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Day in this sense is the number of the day within the month: that is the 23
	 * part of Monday 25th of August 2014
	 *
	 * @param dateExpression	dateExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet that will produce the second of the supplied date.
	 */
	public String doSecondTransform(String dateExpression) {
		return "EXTRACT(SECOND FROM " + dateExpression + ")";
	}

	/**
	 * Returns the partial second value from the date.
	 *
	 * <p>
	 * This should return the most detailed possible value less than a second for
	 * the date expression provided. It should always return a value less than 1s.
	 *
	 * @param dateExpression the date from which to get the subsecond part of.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doSubsecondTransform(String dateExpression) {
		return "(EXTRACT(MILLISECOND FROM " + dateExpression + ")/1000.0000)";
	}

	/**
	 * Transforms an SQL snippet into an SQL snippet that provides the index of
	 * the string to find.
	 *
	 * @param originalString originalString
	 * @param stringToFind stringToFind
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "COALESCE"
	 */
	public String getIfNullFunctionName() {
		return "COALESCE";
	}

	/**
	 * Indicates whether the database supports statements that compares to boolean
	 * values using EQUALS, NOT EQUALS, etc.
	 *
	 * <p>
	 * If the database supports statements that resolve to "true = true", this
	 * method will return TRUE.
	 *
	 * <p>
	 * Internally this method is used to allow DBvolution to alter the SQL for MS
	 * SQLServer so that DBBoolean columns can be compared like with other
	 * databases.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this database can compare boolean values, FALSE otherwise.
	 */
	public boolean supportsComparingBooleanResults() {
		return true;
	}

	/**
	 * Returns the function name of the function that negates boolean values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "NOT"
	 */
	public String getNegationFunctionName() {
		return "NOT";
	}

	/**
	 * Provides the separator between GROUP BY clause items.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return ", "
	 */
	public String getSubsequentGroupBySubClauseSeparator() {
		return ", ";
	}

	/**
	 * Provides the key words and syntax that start the GROUP BY clause.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "AVG"
	 */
	public String getAverageFunctionName() {
		return "AVG";
	}

	/**
	 * Provides the function of the function that provides the count of items in a
	 * selection.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "COUNT"
	 */
	public String getCountFunctionName() {
		return "COUNT";
	}

	/**
	 * Provides the function of the function that provides the maximum value in a
	 * selection.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "MAX"
	 */
	public String getMaxFunctionName() {
		return "MAX";
	}

	/**
	 * Provides the function of the function that provides the minimum value in a
	 * selection.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "MIN"
	 */
	public String getMinFunctionName() {
		return "MIN";
	}

	/**
	 * Provides the function of the function that provides the sum of a selection.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "SUM"
	 */
	public String getSumFunctionName() {
		return "SUM";
	}

	/**
	 * Provides the function of the function that provides the standard deviation
	 * of a selection.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "stddev"
	 */
	public String getStandardDeviationFunctionName() {
		return "STDDEV";
	}

	/**
	 * Indicates whether the database prefers (probably exclusively) the ORDER BY
	 * clause to use column indexes rather than column names.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * side. Unfortunately this causes some problems as the entire dataset will be
	 * retrieved with the first call, making the first call expensive in time and
	 * memory. Subsequent calls will be more efficient but that probably won't
	 * help your developers.
	 *
	 * @param options	options
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "trunc"
	 */
	public String getTruncFunctionName() {
		return "trunc";
	}

	/**
	 * Transforms 2 SQL snippets that represent a real number and a integer into a
	 * real number with the decimal places reduced to the integer.
	 *
	 * <p>
	 * 0 decimal places transforms the real number into an integer.
	 *
	 * @param realNumberExpression realNumberExpression
	 * @param numberOfDecimalPlacesExpression numberOfDecimalPlacesExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet comparing the 2 strings
	 */
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return doStringIfNullTransform(firstSQLExpression, "'<DBVOLUTION NULL PROTECTION>'") + " = " + doStringIfNullTransform(secondSQLExpression, "'<DBVOLUTION NULL PROTECTION>'");
	}

	/**
	 * Transforms a bit expression into an integer expression.
	 *
	 * <p>
	 * Used to allow comparison of bit columns in some databases.
	 *
	 * @param booleanExpression	bitExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the transformation necessary to transform bitExpression into an
	 * integer expression in the SQL.
	 */
	public String doBooleanToIntegerTransform(String booleanExpression) {
		return doIfThenElseTransform(booleanExpression, "" + 1, "" + 0);
	}

	/**
	 * Transforms a integer expression into an bit expression.
	 *
	 * <p>
	 * Used to allow comparison of integer columns in some databases.
	 *
	 * @param bitExpression	bitExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns FALSE.
	 * @see Oracle11XEDBDefinition#prefersTriggerBasedIdentities()
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns an empty list.
	 * @see
	 * Oracle11XEDBDefinition#getTriggerBasedIdentitySQL(nz.co.gregs.dbvolution.databases.DBDatabase,
	 * java.lang.String, java.lang.String)
	 */
	public List<String> getTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return new ArrayList<>();
	}

	public List<String> dropTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return new ArrayList<>();
	}

	/**
	 * Provides the SQL type and modifiers required to create the column
	 * associated with the provided field.
	 *
	 * @param field the field of the column being created.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the name of the trigger to be created.
	 */
	public String getPrimaryKeyTriggerName(String table, String column) {
		return formatNameForDatabase(table + "_" + column + "dtg");
	}

	/**
	 * Indicates whether the database uses a special type for it's auto-increment
	 * columns.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns FALSE.
	 */
	protected boolean hasSpecialAutoIncrementType() {
		return false;
	}

	/**
	 * Indicates whether field provided can be used as a auto-incrementing column
	 * in this database
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true if the QDT field can be used with this database's
	 * autoincrement feature.
	 */
	private boolean propertyWrapperConformsToAutoIncrementType(PropertyWrapper field) {
		final QueryableDatatype<?> qdt = field.getQueryableDatatype();
		return propertyWrapperConformsToAutoIncrementType(qdt);
	}

	/**
	 * Indicates whether {@link QueryableDatatype} provided can be used as a
	 * auto-incrementing column in this database
	 *
	 * @param qdt	qdt
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns TRUE for DBNumber or DBString,
	 * FALSE otherwise.
	 */
	protected boolean propertyWrapperConformsToAutoIncrementType(QueryableDatatype<?> qdt) {
		return (qdt instanceof DBNumber) || (qdt instanceof DBInteger);
	}

	/**
	 * Provides the special auto-increment type used by this database if it has
	 * one.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns ""
	 */
	protected String getSpecialAutoIncrementType() {
		return "";
	}

	/**
	 * Indicates whether the database prefers the primary key to be defined at the
	 * end of the CREATE TABLE statement.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param lob the DBLargeObject which we are querying about.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsReadAsBase64CharacterStream(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getBytes()
	 * method.
	 *
	 * @param lob the type of Large Object being processed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE
	 */
	public boolean prefersLargeObjectsReadAsBytes(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getClob()
	 * method.
	 *
	 * @param lob the type of Large Object being processed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE
	 */
	public boolean prefersLargeObjectsReadAsCLOB(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getBlob()
	 * method.
	 *
	 * @param lob the type of Large Object being processed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE
	 */
	public boolean prefersLargeObjectsReadAsBLOB(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Transforms the arguments into a SQL snippet that produces a substring of
	 * the originalString from the start for length characters.
	 *
	 * @param originalString originalString
	 * @param start start
	 * @param length length
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Indicates that the database prefers Large Object values to be set using the
	 * setCharacterStream method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream(nz.co.gregs.dbvolution.datatypes.DBLargeObject)
	 * } and
	 * {@link #prefersLargeObjectsSetAsBase64String(nz.co.gregs.dbvolution.datatypes.DBLargeObject) }
	 * return FALSE, DBvolution will use the setBinaryStream method to set the
	 * value.
	 *
	 * @param lob the DBLargeObject which we are querying about.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsSetAsCharacterStream(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates that the database prefers Large Object values to be set using the
	 * setBLOB method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream(nz.co.gregs.dbvolution.datatypes.DBLargeObject)
	 * } and
	 * {@link #prefersLargeObjectsSetAsBase64String(nz.co.gregs.dbvolution.datatypes.DBLargeObject) }
	 * return FALSE, DBvolution will use the setBinaryStream method to set the
	 * value.
	 *
	 * @param lob the DBLargeObject which we are querying about.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsSetAsBLOB(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates that the database prefers Large Object values to be set using the
	 * setCharacterStream method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream(nz.co.gregs.dbvolution.datatypes.DBLargeObject)
	 * } and
	 * {@link #prefersLargeObjectsSetAsBase64String(nz.co.gregs.dbvolution.datatypes.DBLargeObject) }
	 * return FALSE, DBvolution will use the setBinaryStream method to set the
	 * value.
	 *
	 * @param lob the DBLargeObject which we are querying about.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE.
	 */
	public boolean prefersLargeObjectsSetAsBase64String(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Provides the name of the function that will choose the largest value from a
	 * list of options.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " GREATEST "
	 */
	public String getGreatestOfFunctionName() {
		return " GREATEST ";
	}

	/**
	 * Provides the name of the function that will choose the smallest value from
	 * a list of options.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " LEAST "
	 */
	public String getLeastOfFunctionName() {
		return " LEAST ";
	}

	/**
	 * Provides Cheeseburger.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns false.
	 * @see #parseDateFromGetString(java.lang.String)
	 */
	public boolean prefersDatesReadAsStrings() {
		return false;
	}

	/**
	 * returns the date format used when reading dates as strings.
	 *
	 * <p>
	 * Normally dates are read as dates but this method allows DBvolution to read
	 * them using a text mode.
	 *
	 * @param getStringDate a date retrieved with {@link ResultSet#getString(java.lang.String)
	 * }
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return return the date format required to interpret strings as dates.
	 * @throws java.text.ParseException SimpleDateFormat may throw a parse
	 * exception
	 * @see #prefersDatesReadAsStrings()
	 */
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(getStringDate);
	}

	/**
	 * Provides an opportunity to tweak the generated DBTableField before creating
	 * the Java classes
	 *
	 * @param dbTableField the current field being processed by
	 * DBTableClassGenerator
	 */
	@SuppressWarnings("empty-statement")
	public void sanityCheckDBTableField(DBTableField dbTableField) {
		;
	}

	/**
	 * Indicates whether this DBDefinition supports retrieving the primary key of
	 * the last inserted row using SQL.
	 *
	 * <p>
	 * Preferably the database should support
	 * {@link #supportsGeneratedKeys() generated keys} but if it doesn't this and {@link #getRetrieveLastInsertedRowSQL()
	 * }
	 * allow the DBDefinition to provide raw SQL for retrieving the last created
	 * primary key.
	 *
	 * <p>
	 * The database should support either generated keys or last inserted row SQL.
	 *
	 * <p>
	 * If both {@link #supportsGeneratedKeys()
	 * } and {@link #supportsRetrievingLastInsertedRowViaSQL() } return false
	 * DBvolution will not retrieve auto-incremented primary keys.
	 *
	 * <p>
	 * Originally provided for the SQLite-JDBC driver.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns "".
	 */
	public String getRetrieveLastInsertedRowSQL() {
		return "";
	}

	/**
	 * Provides the database's version of an empty string.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * If the database does not support the standard function then the definition
	 * may override {@link #doDegreesTransform(java.lang.String) } to implement
	 * the required functionality.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * If the database does not support the standard function then the definition
	 * may override {@link #doRadiansTransform(java.lang.String) } to implement
	 * the required functionality.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * If the database does not support the standard RADIANS function this method
	 * provides another method of providing the function.
	 *
	 * @param degreesSQL	degreesSQL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the degrees expression transformed into a radians expression
	 */
	public String doRadiansTransform(String degreesSQL) {
		return " (" + degreesSQL + ") * 0.0174532925 ";
	}

	/**
	 * Implements the radians to degrees transformation using simple maths.
	 *
	 * <p>
	 * If the database does not support the standard DEGREES function this method
	 * provides another method of providing the function.
	 *
	 * @param radiansSQL	radiansSQL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the radians expression transformed into a degrees expression
	 */
	public String doDegreesTransform(String radiansSQL) {
		return " " + radiansSQL + " * 57.2957795 ";
	}

	/**
	 * Provides the name of the function that raises e to the power of the
	 * provided value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns TRUE.
	 */
	public boolean supportsStandardDeviationFunction() {
		return true;
	}

	/**
	 * Indicates whether the database supports the modulus function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to get the integer division remainder.
	 */
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return "(" + firstNumber + ") % (" + secondNumber + ")";
	}

	/**
	 * Does the required transformation to produce an SQL snippet that adds
	 * numberOfSeconds seconds to the dateValue.
	 *
	 * @param dateValue dateValue
	 * @param numberOfSeconds numberOfSeconds
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL snippet
	 */
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATE_ADD(" + dateValue + ", INTERVAL (" + numberOfYears + ") YEAR )";
	}

	/**
	 * Transform a Java Boolean into the equivalent in an SQL snippet.
	 *
	 * @param boolValue	boolValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL snippet
	 */
	public String doBooleanValueTransform(Boolean boolValue) {
		if (boolValue == null) {
			return getNull();
		} else if (boolValue) {
			return getTrueValue();
		} else {
			return getFalseValue();
		}
//		return beginNumberValue() + (boolValue ? 1 : 0) + endNumberValue();
	}

	/**
	 * Indicates whether the database supports use of the "^" operator to perform
	 * boolean XOR.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL required to find the smallest value in the list
	 * provided.
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
		String prevCase;
		if (strs.size() == 1) {
			return strs.get(0);
		}
		for (String str : strs) {
			if ("".equals(sql)) {
				sql = "(" + str + ")";
			} else {
				prevCase = sql;
				sql = "(case when (" + str + ") < (" + prevCase + ") then (" + str + ") else (" + prevCase + ") end)";
			}
		}
		return sql;
	}

	/**
	 * Transform a set of SQL snippets into the database's version of the GREATEST
	 * function.
	 *
	 * <p>
	 * The GREATEST function takes a list of values and returns the largest value.
	 *
	 * <p>
	 * Not to be confused with the MAX aggregate function.
	 *
	 * @param strs	strs
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
		String prevCase;
		if (strs.size() == 1) {
			return strs.get(0);
		}
		for (String str : strs) {
			if ("".equals(sql)) {
				sql = "(" + str + ")";
			} else {
				prevCase = sql;
				sql = "(case when (" + str + ") > (" + prevCase + ") then (" + str + ") else (" + prevCase + ") end)";
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return "REPLACE(withinString, findString, replaceString)"
	 * @see StringExpression#replace(java.lang.String, java.lang.String)
	 * @see StringExpression#replace(java.lang.String,
	 * nz.co.gregs.dbvolution.results.StringResult)
	 * @see StringExpression#replace(nz.co.gregs.dbvolution.results.StringResult,
	 * java.lang.String)
	 * @see StringExpression#replace(nz.co.gregs.dbvolution.results.StringResult,
	 * nz.co.gregs.dbvolution.results.StringResult)
	 */
	public String doReplaceTransform(String withinString, String findString, String replaceString) {
		return "REPLACE(" + withinString + "," + findString + "," + replaceString + ")";
	}

	/**
	 * Transforms a SQL snippet of a number expression into a character expression
	 * for this database.
	 *
	 * @param numberExpression	numberExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL required to transform the number supplied into
	 * a character or String type.
	 */
	public String doNumberToStringTransform(String numberExpression) {
		return doConcatTransform(getEmptyString(), numberExpression);
	}

	/**
	 * Transforms a SQL snippet of a integer expression into a character
	 * expression for this database.
	 *
	 * @param integerExpression	numberExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL required to transform the number supplied into
	 * a character or String type.
	 */
	public String doIntegerToStringTransform(String integerExpression) {
		return doConcatTransform(getEmptyString(), integerExpression);
	}

	/**
	 * Creates the CURRENTDATE function for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet.
	 */
	public String doBitsValueTransform(boolean[] booleanArray) {
		StringBuilder result = new StringBuilder("");
		String separator = "ARRAY(";
		for (boolean c : booleanArray) {
			if (c) {
				result.append(separator).append("true");
			} else {
				result.append(separator).append("false");
			}
			separator = ",";
		}
		if (!separator.equals("(")) {
			result.append(")");
		}
		return result.toString();
	}

	/**
	 * Convert the 2 SQL date values into a difference in days.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('MINUTE', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Convert the 2 SQL date values into a difference in whole seconds.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF('SECOND', " + dateValue + "," + otherDateValue + "))";
	}

	/**
	 * Convert the 2 SQL date values into a difference in milliseconds.
	 *
	 * @param dateValue dateValue
	 * @param otherDateValue otherDateValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
//	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
//		return "(DATEDIFF('MILLISECOND', " + dateValue + "," + otherDateValue + "))";
//	}
	/**
	 * Create a foreign key clause for use in a CREATE TABLE statement from the
	 * {@link PropertyWrapper} provided.
	 *
	 * @param field	field
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Produce SQL that will provide return the second value if the first is NULL.
	 *
	 * @param possiblyNullValue possiblyNullValue
	 * @param alternativeIfNull alternativeIfNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doStringIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return this.getIfNullFunctionName() + "(" + possiblyNullValue
				+ ","
				+ (alternativeIfNull == null ? "NULL" : alternativeIfNull)
				+ ")";
	}

	/**
	 * Produce SQL that will provide return the second value if the first is NULL.
	 *
	 * @param possiblyNullValue possiblyNullValue
	 * @param alternativeIfNull alternativeIfNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doNumberIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return doStringIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	/**
	 * Produce SQL that will provide return the second value if the first is NULL.
	 *
	 * @param possiblyNullValue possiblyNullValue
	 * @param alternativeIfNull alternativeIfNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doIntegerIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return doStringIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	/**
	 * Produce SQL that will provide return the second value if the first is NULL.
	 *
	 * @param possiblyNullValue possiblyNullValue
	 * @param alternativeIfNull alternativeIfNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return doStringIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	/**
	 * Produce SQL that will compare the first value to all the other values using
	 * the IN operator.
	 *
	 * @param comparableValue comparableValue
	 * @param values values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param table the table to transform into a FROM clause.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet for a FROM clause.
	 */
	public String getFromClause(DBRow table) {
		String recursiveTableAlias = table.getRecursiveTableAlias();
		final String selectQuery = table.getSelectQuery();
		if (recursiveTableAlias != null) {
			return recursiveTableAlias;
		} else if (selectQuery != null) {
			return "(" + selectQuery + ")" + beginTableAlias() + getTableAlias(table) + endTableAlias();
		} else {
			return formatTableName(table) + beginTableAlias() + getTableAlias(table) + endTableAlias();
		}
	}

	/**
	 * The beginning of the WITH variant supported by this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "WITH RECURSIVE" by default.
	 */
	public String beginWithClause() {
		return " WITH RECURSIVE ";
	}

	/**
	 * Define the table to be used during a recursive query.
	 *
	 * @param recursiveTableAlias the table alias used during the recursive query.
	 * @param recursiveColumnNames all the columns in the recursive part of the
	 * query.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return by default something like: ALIAS(COL1, COL2, ... )
	 */
	public String formatWithClauseTableDefinition(String recursiveTableAlias, String recursiveColumnNames) {
		return recursiveTableAlias + "(" + recursiveColumnNames + ")" + " \n";
	}

	/**
	 * Return the default preamble to the priming query of a
	 * {@link DBRecursiveQuery}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " AS ("
	 */
	public String beginWithClausePrimingQuery() {
		return " AS (";
	}

	/**
	 * Return the necessary connector between the priming query and the recursive
	 * query used in a {@link DBRecursiveQuery}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param recursiveTableAlias the table alias used in the recursive query.
	 * @param recursiveAliases all the column aliases used in the recursive query.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return " SELECT ... FROM ... ORDER BY ... ASC; ";
	 */
	public String doSelectFromRecursiveTable(String recursiveTableAlias, String recursiveAliases) {
		return " SELECT " + recursiveAliases + ", " + getRecursiveQueryDepthColumnName() + " FROM " + recursiveTableAlias + " ORDER BY " + getRecursiveQueryDepthColumnName() + " ASC; ";
	}

	/**
	 * Indicates whether this database needs the recursive query to use table
	 * aliases.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param field the property to check
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return FALSE by default
	 */
	protected boolean hasSpecialPrimaryKeyTypeForDBDatatype(PropertyWrapper field) {
		return false;
	}

	/**
	 * Return the necessary SQL data type for this field to be a primary key in
	 * this database.
	 *
	 * @param field the property to check
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return by default DBvolution returns the standard datatype for this field.
	 */
	protected String getSpecialPrimaryKeyTypeOfDBDatatype(PropertyWrapper field) {
		return getSQLTypeOfDBDatatype(field);
	}

	/**
	 * Indicates whether the LEASTOF operator is supported by the database or
	 * needs to be emulated.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * By default this method returns ".*" as system tables are not a problem for
	 * most databases.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Most databases do not have a problem with this method but PostgreSQL likes
	 * the column name to be lowercase in this particular instance.
	 *
	 * @param primaryKeyColumnName the name of the primary key column formatted
	 * for this database
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the Primary Key formatted for this database.
	 */
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return primaryKeyColumnName;
	}

	/**
	 * Choose a string option based on the number in the first parameter.
	 *
	 * <p>
	 * Based on the MS SQLserver CHOOSE function, this method allows the first
	 * parameter to determine which string is appropriate. If the number is 1, the
	 * first string is returned, 2 returns the second and so forth. If the number
	 * exceeds the number of strings the last string is returned.s
	 *
	 * @param numberToChooseWith the index to use
	 * @param strs the options to choose from.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doChooseTransformation(String numberToChooseWith, List<String> strs) {
		if (supportsChooseNatively()) {
			StringBuilder sql = new StringBuilder()
					.append(getChooseFunctionName())
					.append("(")
					.append(numberToChooseWith);
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
		StringBuilder sql = new StringBuilder("(case ");

		if (strs.size() == 1) {
			return strs.get(0);
		}
		String op = " <= ";
		for (int index = 0; index < strs.size() + 1; index++) {
			if (index == strs.size()) {
				sql.append(" else ").append(getNull()).append(" end)");
			} else {
				String str = strs.get(index);
				sql.append(" when ")
						.append(numberToChooseWith)
						.append(op).append(index + 1)
						.append(" then ")
						.append(str)
						.append(System.getProperty("line.separator"));
				op = " = ";
			}
		}
		return sql.toString();
	}

	/**
	 * Allows the DBDatabase instance to provide the database-specific name for
	 * the CHOOSE function.
	 *
	 * <p>
	 * Used by {@link #doChooseTransformation(java.lang.String, java.util.List)
	 * } to connect to the correct database function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return SQL
	 */
	public String getChooseFunctionName() {
		return "";
	}

	/**
	 * Switchs the
	 * {@link #doChooseTransformation(java.lang.String, java.util.List)} to using
	 * the database's native function.
	 *
	 * <p>
	 * Override this method and return TRUE if the database has a native
	 * equivalent to the CHOOSE function as used by  {@link #doChooseTransformation(java.lang.String, java.util.List) }.
	 *
	 * <p>
	 * You will also need to implement {@link #getChooseFunctionName() }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the database has a CHOOSE equivalent, otherwise FALSE
	 */
	protected boolean supportsChooseNatively() {
		return false;
	}

	/**
	 * Implements functionality similar to IF THEN ELSE probably using CASE.
	 *
	 * <p>
	 * Returns the second parameter if the first is TRUE, otherwise returns the
	 * third parameter.
	 *
	 * @param booleanTest the true/false test
	 * @param thenResult the result to return if the test returns TRUE
	 * @param elseResult the result to return if the test returns FALSE
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return IF the booleanTest is TRUE returns the thenResult, otherwise
	 * returns elseResult.
	 */
	public String doIfThenElseTransform(String booleanTest, String thenResult, String elseResult) {
		return "(CASE WHEN " + booleanTest + " THEN " + thenResult + " ELSE " + elseResult + " END)";
	}

	/**
	 * Extracts the weekday from the date provided as a number from 1 to 7.
	 *
	 * <p>
	 * Provides access to the day of the week as a number from 1 for Sunday to 7
	 * for Saturday.
	 *
	 * @param dateSQL the date to get the day of the week for.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a number between 1 and 7 for the weekday.
	 */
	abstract public String doDayOfWeekTransform(String dateSQL);

	/**
	 * Provides the CREATE INDEX clause for this database.
	 *
	 * <p>
	 * Used in {@link DBDatabase#createIndexesOnAllFields(nz.co.gregs.dbvolution.DBRow)
	 * } to create indexes for the fields of the table.
	 *
	 * @param field the field to generate an index for
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String getIndexClauseForCreateTable(PropertyWrapper field) {
		return "CREATE INDEX " + formatNameForDatabase("DBI_" + field.tableName() + "_" + field.columnName()) + " ON " + formatNameForDatabase(field.tableName()) + "(" + formatNameForDatabase(field.columnName()) + ")";
	}

	/**
	 * Transforms the array of booleans into the database format.
	 *
	 * <p>
	 * The default implementation changes the array into a string of 0s and 1s.
	 *
	 * @param bools all the true/false values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a string of 1s and 0s representing the boolean array.
	 */
	public String doBooleanArrayTransform(Boolean[] bools) {
		StringBuilder str = new StringBuilder();
		for (Boolean bool : bools) {
			str.append((bool == true ? "1" : "0"));
		}
		return "'" + str.toString() + "'";
	}

	/**
	 * Reverses the {@link #doBooleanArrayTransform(java.lang.Boolean[]) } and
	 * creates an array of booleans.
	 *
	 * <p>
	 * The default implementation transforms a string of 0s and 1s into an array
	 * of Booleans.
	 *
	 * @param stringOfBools all the true/false values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an array of Booleans.
	 */
	public Boolean[] doBooleanArrayResultInterpretation(String stringOfBools) {
		if (stringOfBools != null && stringOfBools.length() > 0) {
			Boolean[] result = new Boolean[stringOfBools.length()];
			for (int i = 0; i < stringOfBools.length(); i++) {
				result[i] = (stringOfBools.substring(i, i + 1)).equals("1");
			}
			return result;
		} else {
			return null;
		}
	}

	/**
	 * Indicates if the database supports ARRAYs natively and the functionality
	 * has been implemented.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE by default.
	 */
	public boolean supportsArraysNatively() {
		return true;
	}

	/**
	 * Implement this method if the database implements ARRAYs but not BOOLEAN.
	 *
	 * @param objRepresentingABoolean an object to be used in the boolean array
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean derived from objRepresentingABoolean.
	 */
	public Boolean doBooleanArrayElementTransform(Object objRepresentingABoolean) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Transform the to numbers to compare then with equals.
	 *
	 * <p>
	 * The default implementation is {@code leftHandSide + " = " + rightHandSide}.
	 *
	 * @param leftHandSide the first value to compare
	 * @param rightHandSide the second value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to compare the two numbers.
	 */
	public String doNumberEqualsTransform(String leftHandSide, String rightHandSide) {
		return "" + leftHandSide + " = " + rightHandSide + "";
	}

	/**
	 * Transform the to numbers to compare then with equals.
	 *
	 * <p>
	 * The default implementation is {@code leftHandSide + " = " + rightHandSide}.
	 *
	 * @param leftHandSide the first value to compare
	 * @param rightHandSide the second value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to compare the two numbers.
	 */
	public String doIntegerEqualsTransform(String leftHandSide, String rightHandSide) {
		return "" + leftHandSide + " = " + rightHandSide + "";
	}

	/**
	 * Creates the ALTER TABLE statement required to add a FOREIGN KEY constraint.
	 *
	 * <p>
	 * Remember to check {@code field.isForeignKey()} first.
	 *
	 * @param newTableRow the table to be altered.
	 * @param field the field to add a foreign key from
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL to add a foreign key.
	 */
	public String getAlterTableAddForeignKeyStatement(DBRow newTableRow, PropertyWrapper field) {
		if (field.isForeignKey()) {
			return "ALTER TABLE " + this.formatTableName(newTableRow) + " ADD " + this.getForeignKeyClauseForCreateTable(field);
		}
		return "";
	}

	/**
	 * Creates the ALTER TABLE statement required to remove a FOREIGN KEY
	 * constraint.
	 *
	 * <p>
	 * Remember to check {@code field.isForeignKey()} first.
	 *
	 * @param newTableRow the table to be altered.
	 * @param field the field to remove the foreign key from.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL to remove a foreign key.
	 */
	public String getAlterTableDropForeignKeyStatement(DBRow newTableRow, PropertyWrapper field) {
		if (field.isForeignKey()) {
			return "ALTER TABLE " + this.formatTableName(newTableRow) + " DROP FOREIGN KEY " + field.columnName();
		}
		return "";
	}

	/**
	 * Perform necessary transformations on the stored value to make it readable
	 * by Java.
	 *
	 * <p>
	 * Primarily used on Spatial types, this method allows a data type unknown to
	 * JDBC to be transformed into the necessary type (usually a String) to be
	 * read by Java and DBvolution.
	 *
	 * @param qdt the DBV value to be stored
	 * @param selectableName the selectable value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doColumnTransformForSelect(QueryableDatatype<?> qdt, String selectableName) {
		return selectableName;
	}

	/**
	 * Creates a string representation of a DateRepeat from the Period
	 *
	 * @param interval the interval to be transformed into a DateRepeat.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateRpeat as an SQL string
	 */
	public String transformPeriodIntoDateRepeat(Period interval) {
		StringBuilder str = new StringBuilder();
		str.append("'").append(DateRepeatExpression.INTERVAL_PREFIX);
		str.append(interval.getYears()).append(DateRepeatExpression.YEAR_SUFFIX);
		str.append(interval.getMonths()).append(DateRepeatExpression.MONTH_SUFFIX);
		str.append(interval.getDays() + (interval.getWeeks() * 7)).append(DateRepeatExpression.DAY_SUFFIX);
		str.append(interval.getHours()).append(DateRepeatExpression.HOUR_SUFFIX);
		str.append(interval.getMinutes()).append(DateRepeatExpression.MINUTE_SUFFIX);
		str.append(Integer.valueOf(interval.getSeconds()).doubleValue()).append(DateRepeatExpression.SECOND_SUFFIX);
		str.append("'");
		return str.toString();
	}

	/**
	 * Create a DateRepeat by subtracting the 2 dates.
	 *
	 * @param leftHandSide the first date
	 * @param rightHandSide the second date to subtract from the first
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create a DateRepeat from the dates
	 */
	public String doDateMinusToDateRepeatTransformation(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " - " + rightHandSide + ")";
	}

	/**
	 * Compare 2 DateRepeats using EQUALS.
	 *
	 * @param leftHandSide the first value to compare
	 * @param rightHandSide the second value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create to compare DateRepeats
	 */
	public String doDateRepeatEqualsTransform(String leftHandSide, String rightHandSide) {
		//return "(" + leftHandSide + " = " + rightHandSide + ")";
		throw new UnsupportedOperationException("No Native Support For DateRepeat Has Been Implemented");
	}

	/**
	 * Compare 2 DateRepeats using NOT EQUALS.
	 *
	 * @param leftHandSide the first DateRepeat value to compare
	 * @param rightHandSide the second DateRepeat value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create to compare DateRepeats
	 */
	public String doDateRepeatNotEqualsTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " <> " + rightHandSide + ")";
	}

	/**
	 * Compare 2 DateRepeats using LESSTHAN.
	 *
	 * @param leftHandSide the first DateRepeat value to compare
	 * @param rightHandSide the second DateRepeat value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create to compare DateRepeats
	 */
	public String doDateRepeatLessThanTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " < " + rightHandSide + ")";
	}

	/**
	 * Compare 2 DateRepeats using LESSTHANEQUALS.
	 *
	 * @param leftHandSide the first DateRepeat value to compare
	 * @param rightHandSide the second DateRepeat value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create to compare DateRepeats
	 */
	public String doDateRepeatLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " <= " + rightHandSide + ")";
	}

	/**
	 * Compare 2 DateRepeats using GREATERTHAN.
	 *
	 * @param leftHandSide the first DateRepeat value to compare
	 * @param rightHandSide the second DateRepeat value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create to compare DateRepeats
	 */
	public String doDateRepeatGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " > " + rightHandSide + ")";
	}

	/**
	 * Compare 2 DateRepeats using GREATERTHANEQUALS.
	 *
	 * @param leftHandSide the first DateRepeat value to compare
	 * @param rightHandSide the second DateRepeat value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create to compare DateRepeats
	 */
	public String doDateRepeatGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " >= " + rightHandSide + ")";
	}

	/**
	 * Offset the date by the DateRepeat.
	 *
	 * @param leftHandSide the first DateRepeat value to compare
	 * @param rightHandSide the second DateRepeat value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to change the date by the required amount.
	 */
	public String doDatePlusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " + " + rightHandSide + ")";
	}

	/**
	 * Offset the date by subtracting the DateRepeat.
	 *
	 * @param leftHandSide the first DateRepeat value to compare
	 * @param rightHandSide the second DateRepeat value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to change the date by the required amount.
	 */
	public String doDateMinusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return leftHandSide + "-" + rightHandSide;
	}

	/**
	 * Create a Period from the database version of the DateRepeat.
	 *
	 * @param intervalStr the DateRepeat value to convert into a Jodatime Period
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Period.
	 */
	public Period parseDateRepeatFromGetString(String intervalStr) {
		return DateRepeatImpl.parseDateRepeatFromGetString(intervalStr);
	}

	/**
	 * Compare 2 polygons with EQUALS.
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Spatial Operations Haven't Been Defined Yet");
	}

	/**
	 * Creates a Polygon2D representing the union of the Polygon2Ds.
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that represents a polygon of the union, null if both polygons
	 * are null.
	 */
	public String doPolygon2DUnionTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Spatial Operations Haven't Been Defined Yet");
	}

	/**
	 * Creates a Polygon2D representing the intersection of the Polygon2Ds.
	 *
	 * @param firstGeometry the first polygon2d value
	 * @param secondGeometry the second polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that represents a polygon of the intersection, null if there is
	 * no intersection.
	 */
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Spatial Operations Haven't Been Defined Yet");
	}

	/**
	 * Test whether the 2 polygons intersect.
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that returns TRUE if they intersect.
	 */
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Spatial Operations Haven't Been Defined Yet");
	}

	/**
	 * Test whether the first polygon completely contains the second polygon.
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that is TRUE if the first polygon contains the second.
	 */
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Inverse of {@link #doPolygon2DIntersectsTransform(java.lang.String, java.lang.String)
	 * }, tests whether the 2 polygons are non-coincident.
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that is FALSE if the polygons intersect.
	 */
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Test whether the 2 polygons intersect but not contained or within.
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that is TRUE if the polygons have intersecting and
	 * non-intersecting parts.
	 */
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Tests whether the polygons touch.
	 *
	 * <p>
	 * Checks that a) the polygons have at least on point in common and b) that
	 * their interiors do not overlap.
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet
	 */
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Test whether the first polygon is completely within the second polygon.
	 *
	 * <p>
	 * Compare this to {@link #doPolygon2DContainsPolygon2DTransform(java.lang.String, java.lang.String)
	 * }
	 *
	 * @param firstGeometry the first polygon2d value to compare
	 * @param secondGeometry the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that is TRUE if the first polygon is within the second.
	 */
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Returns the dimension of the polygon.
	 *
	 * <p>
	 * This will be "2"
	 *
	 * @param polygon2DSQL a polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return "2" unless something has gone horribly wrong.
	 */
	public String doPolygon2DMeasurableDimensionsTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Create a simple four sided bounding for the polygon.
	 *
	 * @param polygon2DSQL a polygon2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the SQL required to create a bounding box for the polygon.
	 */
	public String doPolygon2DGetBoundingBoxTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Retrieve the area of the polygon.
	 *
	 * @param polygon2DSQL a polygon2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that will return the area of the Polygon2D
	 */
	public String doPolygon2DGetAreaTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Defines the transformation require to transform an SQL Polygon2D into a
	 * linestring representing the exterior ring of the polygon.
	 *
	 * @param polygon2DSQL a polygon2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Geometry Operations Have Not Been Defined For This Database Yet.");
	}

	/**
	 * Indicates that this database supports hyperbolic functions natively.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE by default.
	 */
	public boolean supportsHyperbolicFunctionsNatively() {
		return true;
	}

	/**
	 * Provides the ARCTAN2 function name for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "atan2" by default.
	 */
	public String getArctan2FunctionName() {
		return "atan2";
	}

	/**
	 * Get the year part of the DateRepeat, an integer
	 *
	 * @param dateRepeatSQL a date repeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateRepeatGetYearsTransform(String dateRepeatSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Get the month part of the DateRepeat, an integer
	 *
	 * @param dateRepeatSQL a date repeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateRepeatGetMonthsTransform(String dateRepeatSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Get the Days part of the DateRepeat, an integer
	 *
	 * @param dateRepeatSQL a date repeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateRepeatGetDaysTransform(String dateRepeatSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Get the hour part of the DateRepeat, an integer
	 *
	 * @param dateRepeatSQL a date repeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateRepeatGetHoursTransform(String dateRepeatSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Get the minute part of the DateRepeat, an integer
	 *
	 * @param dateRepeatSQL a date repeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateRepeatGetMinutesTransform(String dateRepeatSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Get the seconds part of the DateRepeat, a decimal number
	 *
	 * @param dateRepeatSQL a date repeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateRepeatGetSecondsTransform(String dateRepeatSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Transform the DateRepeat into it's character based equivalent.
	 *
	 * @param dateRepeatSQL a date repeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doDateRepeatToStringTransform(String dateRepeatSQL) {
		return doConcatTransform(getEmptyString(), dateRepeatSQL);
	}

	/**
	 * Provide SQL to interpret the String value as a number.
	 *
	 * <p>
	 * Full of ways to fail this is.
	 *
	 * @param stringResultContainingANumber a number value to be coerced to string
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that converts the string value into number.
	 */
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return "(0.0+(" + stringResultContainingANumber + "))";
	}

	/**
	 * Indicates that the database supports the ARCSINE function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true by default.
	 */
	public boolean supportsArcSineFunction() {
		return true;
	}

	/**
	 * Indicates that the database supports the COTANGENT function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true by default.
	 */
	public boolean supportsCotangentFunction() {
		return true;
	}

	/**
	 * Transform a datatype not supported by the database into a type that the
	 * database does support.
	 *
	 * <p>
	 * Used mostly to turn Booleans into numbers.
	 *
	 * <p>
	 * By default this method just returns the input DBExpression.
	 *
	 * @param columnExpression a column expression that might need to change type
	 * for this database
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The DBExpression as a DBExpression supported by the database.
	 */
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		return columnExpression;
	}

	/**
	 * Provide the SQL to compare 2 Point2Ds
	 *
	 * @param firstPoint a point2d value to compare
	 * @param secondPoint a point2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		throw new UnsupportedOperationException("Spatial Operations Haven't Been Defined Yet");
	}

	/**
	 * Provide the SQL to return the X coordinate of the Point2D
	 *
	 * @param pont2DSQL a point2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPoint2DGetXTransform(String pont2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL to return the Y coordinate of the Point2D
	 *
	 * @param point2DSQL a point2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPoint2DGetYTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL to return the dimension of the Point2D
	 *
	 * <p>
	 * Point is a 0-dimensional objects for this purpose.
	 *
	 * @param point2DSQL a point2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPoint2DMeasurableDimensionsTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL to derive the Polygon2D representing the Bounding Box of
	 * the Point2D.
	 *
	 * @param point2DSQL a point2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPoint2DGetBoundingBoxTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL to derive the WKT version of the Point2D.
	 *
	 * @param point2DSQL a point2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPoint2DAsTextTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL that correctly represents this Point2D in this database.
	 *
	 * @param point a point to be turned into an SQL point2d value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		String wktValue = point.toText();
		return "'" + wktValue + "'";
	}

	/**
	 * Provide the SQL that correctly represents these coordinates in this
	 * database.
	 *
	 * <p>
	 * The same as
	 * {@link #transformPoint2DIntoDatabaseFormat(com.vividsolutions.jts.geom.Point)}
	 * but for two coordinates as SQL.
	 *
	 * @param xValue a number value
	 * @param yValue a number value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "'POINT(" + xValue + " " + yValue + ")'";
	}

	/**
	 * From the database's representation of a Point2D create a JTS Point.
	 *
	 * <p>
	 * This is the inverse of {@link #transformPoint2DIntoDatabaseFormat(com.vividsolutions.jts.geom.Point)
	 * }.
	 *
	 * @param pointAsString a point2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a point created from the point2d value
	 * @throws com.vividsolutions.jts.io.ParseException if the database result is
	 * not a valid WKT
	 */
	public Point transformDatabasePoint2DValueToJTSPoint(String pointAsString) throws com.vividsolutions.jts.io.ParseException {
		Point point = (new GeometryFactory()).createPoint(new Coordinate(0, 0));
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(pointAsString);
		if (geometry instanceof Point) {
			point = (Point) geometry;
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, point);
		}
		return point;
	}

	/**
	 * From the database's representation of a Polygon2D create a JTS Polygon.
	 *
	 * <p>
	 * This is the inverse of
	 * {@link #transformPolygonIntoDatabasePolygon2DFormat(com.vividsolutions.jts.geom.Polygon)}.
	 *
	 * @param polygon2DSQL a polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon created from the polygon2d value
	 * @throws com.vividsolutions.jts.io.ParseException if the database result is
	 * not a valid WKT
	 */
	public Polygon transformDatabasePolygon2DToJTSPolygon(String polygon2DSQL) throws com.vividsolutions.jts.io.ParseException {
		Polygon poly = (new GeometryFactory()).createPolygon(new Coordinate[]{});
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(polygon2DSQL);
		if (geometry instanceof Polygon) {
			poly = (Polygon) geometry;
		} else if (geometry instanceof LineString) {
			GeometryFactory geofactory = new GeometryFactory();
			LineString lineString = (LineString) geometry;
			poly = geofactory.createPolygon(lineString.getCoordinateSequence());
		} else if (geometry instanceof Point) {
			GeometryFactory geofactory = new GeometryFactory();
			Point point = (Point) geometry;
			poly = geofactory.createPolygon(new Coordinate[]{point.getCoordinate(), point.getCoordinate(), point.getCoordinate(), point.getCoordinate(), point.getCoordinate()});
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, poly);
		}
		return poly;
	}

	/**
	 * From the database's representation of a Lin2D create a JTS LineString.
	 *
	 * <p>
	 * This is the inverse of
	 * {@link #transformPolygonIntoDatabasePolygon2DFormat(com.vividsolutions.jts.geom.Polygon)}.
	 *
	 * @param lineStringAsSQL a line2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a linestring created from the line2d
	 * @throws com.vividsolutions.jts.io.ParseException if the database result is
	 * not a valid WKT
	 */
	public LineString transformDatabaseLine2DValueToJTSLineString(String lineStringAsSQL) throws com.vividsolutions.jts.io.ParseException {
		LineString lineString = (new GeometryFactory()).createLineString(new Coordinate[]{});
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(lineStringAsSQL);
		if (geometry instanceof LineString) {
			lineString = (LineString) geometry;
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, lineString);
		}
		return lineString;
	}

	/**
	 * Provide the SQL that correctly represents this LineString in this database.
	 *
	 * @param lineString a linestring to transform in to a Line2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String transformLineStringIntoDatabaseLine2DFormat(LineString lineString) {
		String wktValue = lineString.toText();
		return "'" + wktValue + "'";
	}

	/**
	 * Provide the SQL to derive the WKT version of the Line2D.
	 *
	 * @param line2DSQL a line2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DAsTextTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Transform the 2 Line2D SQL snippets into an EQUALS comparison of the 2
	 *
	 * @param line2DSQL the first line2d value to compare
	 * @param otherLine2DSQL the second line2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Transform the 2 Line2D SQL snippets into an NOT_EQUALS comparison of the 2
	 *
	 * @param line2DSQL the first line2d value to compare
	 * @param otherLine2DSQL the second line2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DNotEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return "NOT (" + doLine2DEqualsTransform(line2DSQL, otherLine2DSQL) + ")";
	}

	/**
	 * Create the SQL required to get the dimension of this Line2D SQL.
	 *
	 * @param line2DSQL the line2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the dimension (probably 1)
	 */
	public String doLine2DMeasurableDimensionsTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Create the SQL to derive the bounding box of this Line2D SQL
	 *
	 * @param line2DSQL the line2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DGetBoundingBoxTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Create the SQL to transform a Point2DArray SQL into a Polygon2D
	 *
	 * @param pointSQL the point2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String transformPoint2DArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the largest X value within the Line2D
	 * expression.
	 *
	 * @param line2DSQL the line2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DGetMaxXTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the smallest X value within the Line2D
	 * expression.
	 *
	 * @param line2DSQL the line2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DGetMinXTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the largest Y value within the Line2D
	 * expression.
	 *
	 * @param line2DSQL the line2 value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DGetMaxYTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the smallest Y value within the Line2D
	 * expression.
	 *
	 * @param line2DSQL the line2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLine2DGetMinYTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the largest X value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL the polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the smallest X value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL the polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the largest X value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL the polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will return the smallest Y value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL the polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL that will transform a WKT version of a Polygon2D into the
	 * database's version of a Polygon2D.
	 *
	 * @param polygon2D the polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate SQL to derive the distance between the two Polygon2D expressions.
	 *
	 * @param polygon2DSQL the first polygon2d value to compare
	 * @param otherPolygon2DSQL the second polygon2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL:
	 */
	public String doPoint2DDistanceBetweenTransform(String polygon2DSQL, String otherPolygon2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL to apply rounding to the Number expressions
	 *
	 * @param numberSQL the number value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doRoundTransform(String numberSQL) {
		return "ROUND(" + numberSQL + ")";
	}

	/**
	 * Generate the SQL to apply rounding to the Number expressions with the
	 * specified number of decimal places.
	 *
	 * @param number the number value
	 * @param decimalPlaces the number value of the decimal places required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doRoundWithDecimalPlacesTransform(String number, String decimalPlaces) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Generate the SQL to use the SUBSTRING_BEFORE function with the 2 String
	 * expressions.
	 *
	 * @param fromThis the string value to be dissected
	 * @param beforeThis the string value that indicates the end of the required
	 * text. Not included in the returned value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doSubstringBeforeTransform(String fromThis, String beforeThis) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL to use the SUBSTRING_AFTER function with the 2 String
	 * expressions.
	 *
	 * @param fromThis the string value to be dissected
	 * @param afterThis the string value that indicates the beginning of the
	 * required text. Not included in the returned value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doSubstringAfterTransform(String fromThis, String afterThis) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Indicates that closing or canceling a statement will cause the connection
	 * to close as well.
	 *
	 * <p>
	 * Override this method and return FALSE if the database closes connections
	 * when closing statements
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if closing a statement does NOT effect the connection,
	 * otherwise FALSE.
	 */
	public boolean willCloseConnectionOnStatementCancel() {
		return false;
	}

	/**
	 * Indicates that the database driver does not provide the
	 * Statement.isClosed() method.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE by default.
	 */
	public boolean supportsStatementIsClosed() {
		return true;
	}

	/**
	 * Generates the SQL to determine whether the first (polygon) argument
	 * contains the second point argument.
	 *
	 * @param polygon2DSQL the polygon2d to compare with
	 * @param point2DSQL the point2d value that might be inside the polygon2d
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DContainsPoint2DTransform(String polygon2DSQL, String point2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generates the SQL to convert the polygon to the standard text version of a
	 * polygon.
	 *
	 * @param polygonSQL the polygon2d value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generates the SQL required to find whether the 2 lines cross at any point.
	 *
	 * @param firstLine the first line2d value to compare
	 * @param secondLine the second line2d value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL expression that will evaluate to TRUE FALSE or NULL,
	 * depending on whether the lines cross at any point.
	 */
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the intersection point of the 2 line
	 * segment SQL expressions.
	 *
	 * @param firstLine the first line2d to compare
	 * @param secondLine the second line2d to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL expression that will evaluate to the intersection point of
	 * the 2 line segments or NULL.
	 */
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the complete set of all points of
	 * intersection between the tow 2 lines.
	 *
	 * @param firstLine the first line2d to compare
	 * @param secondLine the second line2d to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL expression that will evaluate to the intersection point of
	 * the 2 line segments or NULL.
	 */
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstLine, String secondLine) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Convert the String object returned by the database into a JTS LineSegment
	 * object.
	 *
	 * @param lineSegmentAsSQL the database linesegment2d value to create a
	 * {@link com.vividsolutions.jts.geom.LineSegment JTS LineSegment} with
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a JTS LineSegment derived from the database's response, may be
	 * null.
	 * @throws com.vividsolutions.jts.io.ParseException malformed WKT will throw
	 * an exception
	 */
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineSegmentAsSQL) throws com.vividsolutions.jts.io.ParseException {
		LineString lineString = (new GeometryFactory()).createLineString(new Coordinate[]{});
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(lineSegmentAsSQL);
		if (geometry instanceof LineString) {
			lineString = (LineString) geometry;
			if (lineSegmentAsSQL == null) {
				return null;
			} else {
				return new LineSegment(lineString.getCoordinateN(0), lineString.getCoordinateN(1));
			}
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, lineString);
		}
	}

	/**
	 * Convert the JTS LineSegment object into a SQL expression that the database
	 * will accept as a line segment.
	 *
	 * <p>
	 * By default, creates a WKT representation
	 *
	 * @param lineSegment the LineSegment to convert to database format.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL expression that can be interpreted by the database as a line
	 * segment.
	 */
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		LineString line = (new GeometryFactory()).createLineString(new Coordinate[]{lineSegment.getCoordinate(0), lineSegment.getCoordinate(1)});
		String wktValue = line.toText();
		return "'" + wktValue + "'";
	}

	/**
	 * Generates the database specific SQL for testing whether the 2 line segment
	 * expressions ever cross.
	 *
	 * @param firstSQL the first Line2D value to compare
	 * @param secondSQL the second Line2D value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL expression that will report whether the 2 line segments
	 * intersect.
	 * @see
	 * #doLineSegment2DIntersectionPointWithLineSegment2DTransform(java.lang.String,
	 * java.lang.String)
	 */
	public String doLineSegment2DIntersectsLineSegment2DTransform(String firstSQL, String secondSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the largest X value in the line segment
	 * SQL expression.
	 *
	 * @param lineSegment the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DGetMaxXTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the smallest X value in the line segment
	 * SQL expression.
	 *
	 * @param lineSegment the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DGetMinXTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the largest Y value in the line segment
	 * SQL expression.
	 *
	 * @param lineSegment the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DGetMaxYTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the smallest Y value in the line segment
	 * SQL expression.
	 *
	 * @param lineSegment the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DGetMinYTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to the rectangular boundary that fully encloses
	 * the line segment SQL expression.
	 *
	 * @param lineSegment the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the dimension of the line segment SQL
	 * expression.
	 *
	 * @param lineSegment the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DDimensionTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find whether the 2 line segment SQL
	 * expressions are NOT equal.
	 *
	 * @param firstLineSegment the first LineSegment2D value
	 * @param secondLineSegment the second LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DNotEqualsTransform(String firstLineSegment, String secondLineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find whether the 2 line segment SQL
	 * expressions are equal.
	 *
	 * @param firstLineSegment the first LineSegment2D value
	 * @param secondLineSegment the second LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DEqualsTransform(String firstLineSegment, String secondLineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to convert the line segment SQL expression into
	 * the WKT string format.
	 *
	 * @param lineSegment the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DAsTextTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find the intersection point of the 2 line
	 * segment SQL expressions.
	 *
	 * @param firstLineSegment the first LineSegment2D value
	 * @param secondLineSegment the second LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an SQL expression that will evaluate to the intersection point of
	 * the 2 line segments or NULL.
	 */
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to return the starting point of the provided
	 * LineSegment2D expression.
	 *
	 * @param lineSegmentSQL the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DStartPointTransform(String lineSegmentSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to return the starting point of the provided
	 * LineSegment2D expression.
	 *
	 * @param lineSegmentSQL the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doLineSegment2DEndPointTransform(String lineSegmentSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL that correctly represents this MultiPoint2D value in this
	 * database.
	 *
	 * @param points the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		String wktValue = points.toText();
		return "'" + wktValue + "'";
	}

	/**
	 * Convert the database's string representation of a MultiPoint2D value into a
	 * MultiPoint..
	 *
	 * @param pointsAsString the MultiPoint2D value to create a
	 * {@link com.vividsolutions.jts.geom.MultiPoint JTS MultiPoint} with.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the MultiPoint2D as a
	 * {@link com.vividsolutions.jts.geom.MultiPoint JTS MultiPoint} instance
	 * @throws com.vividsolutions.jts.io.ParseException malformed WKT values will
	 * throw an exception
	 */
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		MultiPoint mpoint = (new GeometryFactory()).createMultiPoint(new Coordinate[]{});
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(pointsAsString);
		if (geometry instanceof MultiPoint) {
			mpoint = (MultiPoint) geometry;
		} else if (geometry instanceof Point) {
			mpoint = (new GeometryFactory().createMultiPoint(new Point[]{((Point) geometry)}));
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, geometry);
		}
		return mpoint;
	}

	/**
	 * Provide the SQL to compare 2 MultiPoint2Ds using the equivalent of EQUALS.
	 *
	 * @param firstMultiPointValue the first MultiPoint2D value to compare
	 * @param secondMultiPointValue the second MultiPoint2D value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DEqualsTransform(String firstMultiPointValue, String secondMultiPointValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL to compare 2 MultiPoint2Ds using the equivalent of NOT
	 * EQUALS.
	 *
	 * @param first the first MultiPoint2D value to compare
	 * @param second the second MultiPoint2D value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DNotEqualsTransform(String first, String second) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provide the SQL to get point at the supplied index within the MultiPoint2D
	 *
	 * @param first the first MultiPoint2D value to retrieve a point from.
	 * @param index the index at which the required point is at.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL the derive the number of points in the multipoint2d value.
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DGetNumberOfPointsTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL the derive the dimension (2 basically) of the MultiPoint2D
	 * value.
	 *
	 * @param multipoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DMeasurableDimensionsTransform(String multipoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL the derive the bounding box containing all the points in
	 * the MultiPoint2D value.
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DGetBoundingBoxTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL the transform the MultiPoint2D value into a WKT value.
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DAsTextTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL the transform the MultiPoint2D value into a
	 * {@link Line2DResult} value.
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DToLine2DTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

//	public String doMultiPoint2DToPolygon2DTransform(String first) {
//		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//	}
	/**
	 * Provides the SQL that will derive the smallest Y value of all the points in
	 * the MultiPoint2D value
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DGetMinYTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL that will derive the smallest X value of all the points in
	 * the MultiPoint2D value
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DGetMinXTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL that will derive the largest Y value of all the points in
	 * the MultiPoint2D value
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DGetMaxYTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Provides the SQL that will derive the largest X value of all the points in
	 * the MultiPoint2D value
	 *
	 * @param multiPoint2D the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doMultiPoint2DGetMaxXTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Returns true if the database supports has built-in support for limiting
	 * number of rows returned by a query.
	 *
	 * @param options the query options used for this query.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if there is an SQL way of limiting rows numbers, otherwise
	 * FALSE
	 */
	public boolean supportsRowLimitsNatively(QueryOptions options) {
		return true;
	}

	/**
	 * Return if, like Oracle, the database requires Spatial indexes to perform
	 * standard spatial operations.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return FALSE by default
	 */
	public boolean requiresSpatial2DIndexes() {
		return false;
	}

	/**
	 * Return the sequence of SQL operations required to create the necessary
	 * Spatial2D indexes.
	 *
	 * @param database the database for which we require spatial indexes.
	 * @param formatTableName the table for which the index should apply.
	 * @param formatColumnName the column which the index will index.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an ordered list of SQL.
	 */
	public List<String> getSpatial2DIndexSQL(DBDatabase database, String formatTableName, String formatColumnName) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Wrap query with any required syntax to provide paging functionality.
	 *
	 * <p>
	 * Required to support Oracle's ROWNUM-based paging "system".
	 *
	 * <p>
	 * By default the method just returns the sqlQuery.
	 *
	 * @param sqlQuery the SQL query to add paging functionality
	 * @param options the options that apply to the query.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	public String doWrapQueryForPaging(String sqlQuery, QueryOptions options) {
		return sqlQuery;
	}

	/**
	 * Return the number of spatial dimensions that this geometry is defined in.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D, 3D, etc.
	 *
	 * @param line2DSQL the Line2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the number of spatial dimensions that this geometry is defined in.
	 */
	public String doLine2DSpatialDimensionsTransform(String line2DSQL) {
		return "2";
	}

	/**
	 * Return whether this geometry includes a magnitude (M) value along with X,
	 * Y, etc.
	 *
	 * @param line2DSQL the Line2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE or FALSE
	 */
	public String doLine2DHasMagnitudeTransform(String line2DSQL) {
		return this.getFalseOperation();
	}

	/**
	 * Return the magnitude (M) value of this line.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D or 3D.
	 *
	 * @param line2DSQL the Line2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the value for the magnitude, or NULL if there is no magnitude.
	 */
	public String doLine2DGetMagnitudeTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Return the number of spatial dimensions that this geometry is defined in.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D, 3D, etc.
	 *
	 * @param point2DSQL the Point2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the number of spatial dimensions that this geometry is defined in.
	 */
	public String doPoint2DSpatialDimensionsTransform(String point2DSQL) {
		return "2";
	}

	/**
	 * Return whether this geometry includes a magnitude (M) value along with X,
	 * Y, etc.
	 *
	 * @param point2DSQL the Point2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE or FALSE
	 */
	public String doPoint2DHasMagnitudeTransform(String point2DSQL) {
		return this.getFalseOperation();
	}

	/**
	 * Return the magnitude (M) value of this line.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D or 3D.
	 *
	 * @param point2DSQL the Point2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the value for the magnitude, or NULL if there is no magnitude.
	 */
	public String doPoint2DGetMagnitudeTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Return the number of spatial dimensions that this geometry is defined in.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D, 3D, etc.
	 *
	 * @param multipoint2DSQL the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the number of spatial dimensions that this geometry is defined in.
	 */
	public String doMultiPoint2DSpatialDimensionsTransform(String multipoint2DSQL) {
		return "2";
	}

	/**
	 * Return whether this geometry includes a magnitude (M) value along with X,
	 * Y, etc.
	 *
	 * @param multipoint2DSQL the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE or FALSE
	 */
	public String doMultiPoint2DHasMagnitudeTransform(String multipoint2DSQL) {
		return this.getFalseOperation();
	}

	/**
	 * Return the magnitude (M) value of this line.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D or 3D.
	 *
	 * @param multipoint2DSQL the MultiPoint2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the value for the magnitude, or NULL if there is no magnitude.
	 */
	public String doMultiPoint2DGetMagnitudeTransform(String multipoint2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Return the number of spatial dimensions that this geometry is defined in.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D, 3D, etc.
	 *
	 * @param polygon2DSQL the Polygon2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the number of spatial dimensions that this geometry is defined in.
	 */
	public String doPolygon2DSpatialDimensionsTransform(String polygon2DSQL) {
		return "2";
	}

	/**
	 * Return whether this geometry includes a magnitude (M) value along with X,
	 * Y, etc.
	 *
	 * @param polygon2DSQL the Polygon2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE or FALSE
	 */
	public String doPolygon2DHasMagnitudeTransform(String polygon2DSQL) {
		return this.getFalseOperation();
	}

	/**
	 * Return the magnitude (M) value of this line.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D or 3D.
	 *
	 * @param polygon2DSQL the Polygon2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the value for the magnitude, or NULL if there is no magnitude.
	 */
	public String doPolygon2DGetMagnitudeTransform(String polygon2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Return the number of spatial dimensions that this geometry is defined in.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D, 3D, etc.
	 *
	 * @param lineSegment2DSQL the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the number of spatial dimensions that this geometry is defined in.
	 */
	public String doLineSegment2DSpatialDimensionsTransform(String lineSegment2DSQL) {
		return "2";
	}

	/**
	 * Return whether this geometry includes a magnitude (M) value along with X,
	 * Y, etc.
	 *
	 * @param lineSegment2DSQL the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE or FALSE
	 */
	public String doLineSegment2DHasMagnitudeTransform(String lineSegment2DSQL) {
		return this.getFalseOperation();
	}

	/**
	 * Return the magnitude (M) value of this line.
	 *
	 * <p>
	 * Effectively indicates whether the geometry is 2D or 3D.
	 *
	 * @param lineSegment2DSQL the LineSegment2D value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the value for the magnitude, or NULL if there is no magnitude.
	 */
	public String doLineSegment2DGetMagnitudeTransform(String lineSegment2DSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Override this method to provide the SQL that will create a database
	 * Polygon2D value from the list of presumed coordinates.
	 *
	 * <p>
	 * Coordinates are a series of number values that are presumed to be pairs of
	 * X and Y values. That is to say the list is a list number values with no
	 * formatting other than that required to express the values as numbers.
	 *
	 * @param coordinateSQL lots of numbers
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	public String transformCoordinateArrayToDatabasePolygon2DFormat(List<String> coordinateSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Override this method to provide a specific transform that will derive the
	 * last day of the month from the date value provided.
	 *
	 * <p>
	 * If no override is provided for this method a default implementation will be
	 * used instead.
	 *
	 * @param dateSQL the date value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the last day of the month that the date is in.
	 */
	public String doEndOfMonthTransform(String dateSQL) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Override this method to implement changing a date to another time zone.
	 *
	 * <p>
	 * The method should roll the time forward or backward to the correct time for
	 * the time zone. That is if the date is 12/Aug/2015 10:13:34 GMT+1200 and the
	 * new time zone is GMT+1000, then the new date should be 12/Aug/2015 8:13:34
	 * GMT+1000.
	 *
	 * <p>
	 * When implementing this method be aware that time zones are very complex,
	 * and you will need to deal with "GMT+1345", "PST", "Pacific/Auckland", and
	 * lots of other variants.
	 *
	 * @param dateSQL the date to be move to another time.
	 * @param timeZone the required time zone
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL representing the date value in the requested time zone.
	 */
	public String doDateAtTimeZoneTransform(String dateSQL, TimeZone timeZone) throws UnsupportedOperationException {
		return "(" + dateSQL + " AT TIME ZONE '" + timeZone.getDisplayName(false, TimeZone.SHORT) + "')";
//		Double zoneOffset = 0.0 + timeZone.getRawOffset();//(0.0 + this.getSecond().getRawOffset()) / 60.0;
//		final double inHours = zoneOffset / 1000 / 60 / 60;
//
//		int hourPart = Double.valueOf(inHours).intValue();
//		int minutePart = Double.valueOf((inHours-hourPart)*60).intValue();
//		return "("+dateSQL+ " AT TIME ZONE INTERVAL '"
//				+(hourPart>0?"+":"-")+(hourPart<10?"0"+hourPart:""+hourPart)+":"
//				+(minutePart<10?"0"+minutePart:""+minutePart)+"')";
//		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Returns the {@link QueryableDatatype} class to be used with the named
	 * database specific datatype.
	 *
	 * <p>
	 * This method is called during {@link DBTableClassGenerator} to resolve data
	 * types that JDBC doesn't recognize into a QDT. In particular anything that
	 * JDBC reports as {@link java.sql.Types#OTHER} will be resolved using this
	 * method.
	 *
	 * <p>
	 * The default method returns NULL which causes the generator to use a
	 * DBJavaObject.
	 *
	 * @param typeName the name of the SQL data type as reported by JDBC
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the class of the QDT that can be used with this columns of this
	 * type name.
	 */
	public Class<? extends QueryableDatatype<?>> getQueryableDatatypeClassForSQLDatatype(String typeName) {
		switch (typeName.toUpperCase()) {
			case "POLYGON":
				return DBPolygon2D.class;
			case "LINESTRING":
				return DBLine2D.class;
			case "POINT":
				return DBPoint2D.class;
			case "MULTIPOINT":
				return DBMultiPoint2D.class; // obviously this is not going to work in all cases 
			default:
				return null;
		}
	}

	/**
	 * Supplies the beginning of the HAVING clause.
	 *
	 * <p>
	 * Default implementation returns "HAVING ".
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return "HAVING "
	 */
	public String getHavingClauseStart() {
		return "HAVING ";
	}

	/**
	 * The value used for TRUE boolean values.
	 *
	 * <p>
	 * The default method returns " TRUE ".
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " TRUE "
	 */
	public String getTrueValue() {
		return " TRUE ";
	}

	/**
	 * The value used for FALSE boolean values.
	 *
	 * <p>
	 * The default method returns " FALSE ".
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return " FALSE "
	 */
	public String getFalseValue() {
		return " FALSE ";
	}

	/**
	 * Transforms the boolean statement to a value that can be compared by this
	 * database.
	 *
	 * <p>
	 * If the database supports comparing booleans (see {@link #supportsComparingBooleanResults()
	 * }) just return the input.
	 *
	 * @param booleanStatement SQL the resolves to a boolean statement
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the statement transformed so that the value can be compared using
	 * the standard operators, by default the method returns the input unchanged.
	 */
	public String doBooleanStatementToBooleanComparisonValueTransform(String booleanStatement) {
		if (this.supportsComparingBooleanResults()) {
			return booleanStatement;
		} else {
			return " CASE WHEN " + booleanStatement + " THEN " + getTrueValue() + " WHEN NOT " + booleanStatement + " THEN " + getFalseValue() + " ELSE -1 END ";
		}
	}

	/**
	 * Transforms the boolean value (as an SQL snippet) to a value that can be
	 * compared by this database.
	 *
	 * <p>
	 * If the database supports comparing booleans (see {@link #supportsComparingBooleanResults()
	 * }) just return the input.
	 *
	 * @param booleanValueSQL the resolves to a boolean statement
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the statement transformed so that the value can be compared using
	 * the standard operators, by default the method returns the input unchanged.
	 */
	public String doBooleanValueToBooleanComparisonValueTransform(String booleanValueSQL) {
		if (this.supportsComparingBooleanResults()) {
			return booleanValueSQL;
		} else {
			return " CASE WHEN " + booleanValueSQL + " IS NULL THEN -1 ELSE " + booleanValueSQL + " END ";
		}
	}

	/**
	 * Returns this database's version of the UNION DISTINCT syntax
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the standard definition returns " UNION DISTINCT "
	 */
	public String getUnionDistinctOperator() {
		return " UNION DISTINCT  ";
	}

	/**
	 * Returns this database's version of the UNION syntax
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the standard definition returns " UNION "
	 */
	public String getUnionOperator() {
		return " UNION "; //To change body of generated methods, choose Tools | Templates.
	}

	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.CLOB;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.JAVAOBJECT;
		} else {
			return LargeObjectHandlerType.BLOB;
		}
	}

	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.CLOB;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.JAVAOBJECT;
		} else {
			return LargeObjectHandlerType.BLOB;
		}
	}

	/**
	 * Return the function name for the RoundUp function.
	 *
	 * <p>
	 * By default this method returns <b>ceil</b></p>
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	public String getRoundUpFunctionName() {
		return "ceil";
	}

	/**
	 * Return the function name for the Natural Logarithm function.
	 *
	 * <p>
	 * By default this method returns <b>ln</b></p>
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	public String getNaturalLogFunctionName() {
		return "ln";
	}

	/**
	 * Return the function name for the Logarithm Base10 function.
	 *
	 * <p>
	 * By default this method returns <b>log10</b></p>
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	public String getLogBase10FunctionName() {
		return "log10";
	}

	/**
	 * Returns the required code to generate a random number.
	 *
	 * <p>
	 * For each call of this method a new random number is generated.
	 * </p>
	 *
	 * <p>
	 * By default this method returns <b>rand()</b></p>
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when creating random numbers
	 */
	public String doRandomNumberTransform() {
		return " rand() ";
	}

	public String doRandomIntegerTransform() {
		return " rand() ";
	}

	/**
	 * Return the Natural Logarithm.
	 *
	 * <p>
	 * By default this method returns <b>log10(sql)</b></p>
	 *
	 * @param sql
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the name of the function to use when rounding numbers up
	 */
	public String doLogBase10NumberTransform(String sql) {
		return "log10(" + sql + ")";
	}

	/**
	 * Return the Natural Logarithm.
	 *
	 * <p>
	 * By default this method returns <b>log10(sql)</b></p>
	 *
	 * @param sql
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the name of the function to use when rounding numbers up
	 */
	public String doLogBase10IntegerTransform(String sql) {
		return doNumberToIntegerTransform("log10(" + sql + ")");
	}

	public String doNumberToIntegerTransform(String sql) {
		return doTruncTransform(sql, "0");
	}

	public String doFindNumberInStringTransform(String toSQLString) {
		return "(case when regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+(\\.[0-9]+)?).*', '$1') = " + toSQLString + " then null else regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+(\\.[0-9]+)?).*', '$1') end)";
	}

	public String doFindIntegerInStringTransform(String toSQLString) {
		return "(case when regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+).*', '$1') = " + toSQLString + " then null else regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+).*', '$1') end)";
	}

	public String doIntegerToNumberTransform(String toSQLString) {
		return toSQLString;
	}

	/**
	 * Indicates whether the database requires a persistent connection to operate
	 * correctly.
	 *
	 * <p>
	 * Some, usually in-memory, databases require a continuous connection to
	 * maintain their data.
	 *
	 * <p>
	 * DBvolution is usually clever with its connections and does not require a
	 * persistent connection.
	 *
	 * <p>
	 * However if a continuous connection is required to maintain the data,
	 * override this method to return TRUE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the database requires a continuous connection to maintain
	 * data, FALSE otherwise.
	 */
	public boolean persistentConnectionRequired() {
		return false;
	}

	/**
	 * Oracle does not differentiate between NULL and an empty string.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return FALSE.
	 */
	public Boolean supportsDifferenceBetweenNullAndEmptyString() {
		return true;
	}

	/**
	 * Indicates that the database supports the UNION DISTINCT syntax
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this database supports the UNION DISTINCT syntax, FALSE
	 * otherwise.
	 */
	public Boolean supportsUnionDistinct() {
		return true;
	}

	/**
	 * Indicates that this database supplies sufficient tools to create native
	 * recursive queries.
	 *
	 * <p>
	 * Please note that this may not be actual support for standard "WITH
	 * RECURSIVE".
	 *
	 * <p>
	 * If the database does not support recursive queries natively then DBvolution
	 * will emulate recursive queries. Native queries are faster and easier on the
	 * network and application server, so emulation should be a last resort.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE by default, but some DBDatabases may return FALSE.
	 */
	public boolean supportsRecursiveQueriesNatively() {
		return true;
	}

	/**
	 * Indicates whether this database supports full outer joins.
	 *
	 * <p>
	 * Some databases don't yet support queries where all the tables are optional,
	 * that is FULL OUTER joins.
	 *
	 * <p>
	 * This method indicates whether or not this instance can perform full outer
	 * joins.
	 *
	 * <p>
	 * Please note: there are plans to implement full outer joins within DBV for
	 * databases without native support, at which point this method will return
	 * TRUE for all databases. Timing for this implementation is not yet
	 * available.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this DBDatabase supports full outer joins , FALSE
	 * otherwise.
	 */
	public boolean supportsFullOuterJoin() {
		return true;
		//return supportsFullOuterJoinNatively()||supportsRightOuterJoinNatively();
	}

	/**
	 * Indicates whether this database supports full outer joins natively.
	 *
	 * <p>
	 * Some databases don't yet support queries where all the tables are optional,
	 * that is FULL OUTER joins.
	 *
	 * <p>
	 * This method indicates whether or not this instance can perform full outer
	 * joins.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the underlying database supports full outer joins natively,
	 * FALSE otherwise.
	 */
	public boolean supportsFullOuterJoinNatively() {
		return true;
	}

	/**
	 * Indicates whether this database supports the RIGHT OUTER JOIN syntax.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return Returns TRUE if this database supports RIGHT OUTER JOIN, otherwise
	 * FALSE
	 */
	public boolean supportsRightOuterJoinNatively() {
		return true;
	}

	boolean supportsPaging(QueryOptions options) {
		return supportsPagingNatively(options);
	}

	public boolean supportsAlterTableAddConstraint() {
		return true;
	}

	public String getSQLToCheckTableExists(DBRow table) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	public boolean supportsTableCheckingViaMetaData() {
		return true;
	}

	public boolean requiresOnClauseForAllJoins() {
		return false;
	}

	public boolean requiresSequenceUpdateAfterManualInsert() {
		return false;
	}

	public String getSequenceUpdateSQL(String tableName, String columnName, long primaryKeyGenerated) {
		return "UPDATE SEQUENCE FOR TABLE " + tableName + " ON COLUMN " + columnName + " TO " + (primaryKeyGenerated + 1);
	}

	public Collection<? extends String> getInsertPreparation(DBRow table) {
		return new ArrayList<String>();
	}

	public Collection<? extends String> getInsertCleanUp(DBRow table) {
		return new ArrayList<String>();
	}

	public String getAlterTableAddColumnSQL(DBRow existingTable, PropertyWrapper columnPropertyWrapper) {
		return "ALTER TABLE " + formatTableName(existingTable) + " ADD COLUMN " + getAddColumnColumnSQL(columnPropertyWrapper) + endSQLStatement();
	}

	public String getAddColumnColumnSQL(PropertyWrapper field) {

		StringBuilder sqlScript = new StringBuilder();

		if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
			String colName = field.columnName();
			sqlScript
					.append(formatColumnName(colName))
					.append(getCreateTableColumnsNameAndTypeSeparator())
					.append(getSQLTypeAndModifiersOfDBDatatype(field));
		}

		return sqlScript.toString();
	}
}
