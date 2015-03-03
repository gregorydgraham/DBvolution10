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

import java.text.*;
import java.util.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.MSSQLServerDB;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * Defines the features of the Microsoft SQL Server database that differ from
 * the standard database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link MSSQLServerDB}
 * instances, and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class MSSQLServerDBDefinition extends DBDefinition {
	
	private static final String[] reservedWordsArray = new String[]{"ADD", "EXTERNAL", "PROCEDURE", "ALL", "FETCH", "PUBLIC", "ALTER", "FILE", "RAISERROR", "AND", "FILLFACTOR", "READ", "ANY", "FOR", "READTEXT", "AS", "FOREIGN", "RECONFIGURE", "ASC", "FREETEXT", "REFERENCES", "AUTHORIZATION", "FREETEXTTABLE", "REPLICATION", "BACKUP", "FROM", "RESTORE", "BEGIN", "FULL", "RESTRICT", "BETWEEN", "FUNCTION", "RETURN", "BREAK", "GOTO", "REVERT", "BROWSE", "GRANT", "REVOKE", "BULK", "GROUP", "RIGHT", "BY", "HAVING", "ROLLBACK", "CASCADE", "HOLDLOCK", "ROWCOUNT", "CASE", "IDENTITY", "ROWGUIDCOL", "CHECK", "IDENTITY_INSERT", "RULE", "CHECKPOINT", "IDENTITYCOL", "SAVE", "CLOSE", "IF", "SCHEMA", "CLUSTERED", "IN", "SECURITYAUDIT", "COALESCE", "INDEX", "SELECT", "COLLATE", "INNER", "SEMANTICKEYPHRASETABLE", "COLUMN", "INSERT", "SEMANTICSIMILARITYDETAILSTABLE", "COMMIT", "INTERSECT", "SEMANTICSIMILARITYTABLE", "COMPUTE", "INTO", "SESSION_USER", "CONSTRAINT", "IS", "SET", "CONTAINS", "JOIN", "SETUSER", "CONTAINSTABLE", "KEY", "SHUTDOWN", "CONTINUE", "KILL", "SOME", "CONVERT", "LEFT", "STATISTICS", "CREATE", "LIKE", "SYSTEM_USER", "CROSS", "LINENO", "TABLE", "CURRENT", "LOAD", "TABLESAMPLE", "CURRENT_DATE", "MERGE", "TEXTSIZE", "CURRENT_TIME", "NATIONAL", "THEN", "CURRENT_TIMESTAMP", "NOCHECK", "TO", "CURRENT_USER", "NONCLUSTERED", "TOP", "CURSOR", "NOT", "TRAN", "DATABASE", "NULL", "TRANSACTION", "DBCC", "NULLIF", "TRIGGER", "DEALLOCATE", "OF", "TRUNCATE", "DECLARE", "OFF", "TRY_CONVERT", "DEFAULT", "OFFSETS", "TSEQUAL", "DELETE", "ON", "UNION", "DENY", "OPEN", "UNIQUE", "DESC", "OPENDATASOURCE", "UNPIVOT", "DISK", "OPENQUERY", "UPDATE", "DISTINCT", "OPENROWSET", "UPDATETEXT", "DISTRIBUTED", "OPENXML", "USE", "DOUBLE", "OPTION", "USER", "DROP", "OR", "VALUES", "DUMP", "ORDER", "VARYING", "ELSE", "OUTER", "VIEW", "END", "OVER", "WAITFOR", "ERRLVL", "PERCENT", "WHEN", "ESCAPE", "PIVOT", "WHERE", "EXCEPT", "PLAN", "WHILE", "EXEC", "PRECISION", "WITH", "EXECUTE", "PRIMARY", "WITHIN GROUP", "EXISTS", "PRINT", "WRITETEXT", "EXIT", "PROC"};
	private static final List<String> reservedWords = Arrays.asList(reservedWordsArray);

	@Override
	public String getDateFormattedForQuery(Date date) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		DateFormat tzFormat = new SimpleDateFormat("Z");
		String tz = tzFormat.format(date);
		if (tz.length() == 4) {
			tz = "+" + tz.substring(0, 2) + ":" + tz.substring(2, 4);
		} else if (tz.length() == 5) {
			tz = tz.substring(0, 3) + ":" + tz.substring(3, 5);
		} else {
			throw new DBRuntimeException("TIMEZONE was :\"" + tz + "\"");
		}
		final String result = " CAST('" + format.format(date) + " " + tz + "' as DATETIMEOFFSET) ";
//		System.out.println(""+result);
		return result;
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBBoolean) {
			return " BIT ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDate) {
			return " DATETIMEOFFSET ";
		} else if (qdt instanceof DBLargeObject) {
			return " NTEXT ";
		} else if (qdt instanceof DBString) {
			return " NVARCHAR(1000) COLLATE Latin1_General_CS_AS_KS_WS ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return true;
	}

	@Override
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		String tempString = getStringDate.replaceAll(":([0-9]*)$", "$1");
		Date parsed;
		try {
			parsed = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z").parse(tempString);
		} catch (ParseException ex) {
			parsed = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(tempString);
		}
		return parsed;
	}

	@Override
	public String formatTableName(DBRow table) {
		return "[" + table.getTableName() + "]";
	}

	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (reservedWords.contains(sqlObjectName.toUpperCase())) {
			return ("O" + sqlObjectName.hashCode()).replaceAll("^[_-]", "O").replaceAll("-", "_");
		}
		return sqlObjectName;
	}

	@Override
	public Object endSQLStatement() {
		return "";
	}

	@Override
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return " TOP(" + options.getRowLimit() + ") "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		return "";
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTRING("
				+ originalString
				+ ", "
				+ start
				+ (length.trim().isEmpty() ? "" : ", " + length)
				+ ") ";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LEN";
	}

	/**
	 * SQLServer follows the standard, unlike anyone else, and pads the short
	 * string with spaces before comparing.
	 *
	 * <p>
	 * This effectively means strings are trimmed during comparisons whether you
	 * like it or not.
	 *
	 * <p>
	 * While this seems useful, in fact it prevents checking for incorrect
	 * strings and breaks the industrial standard.
	 *
	 * @param firstSQLExpression
	 * @param secondSQLExpression
	 * @return
	 */
	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return "(" + firstSQLExpression + "+'@') = (" + secondSQLExpression + "+'@')";
	}

	@Override
	public String doTrimFunction(String enclosedValue) {
		return " LTRIM(RTRIM(" + enclosedValue + ")) "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "CHARINDEX(" + stringToFind + ", " + originalString + ")";
	}

	@Override
	public String doConcatTransform(String firstString, String secondString) {
		return firstString + "+" + secondString;
	}

	@Override
	public String getIfNullFunctionName() {
		return "ISNULL"; //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * MSSQLserver only supports integer degrees, and that's not good enough.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsDegreesFunction() {
		return false;
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

	@Override
	public boolean prefersLargeObjectsSetAsBase64String() {
		return true;
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream() {
		return true;
	}

	@Override
	protected String getCurrentDateOnlyFunctionName() {
		return " GETDATE";
	}

	@Override
	public String doAddMillisecondsTransform(String dateValue, String numberOfSeconds) {
		return "DATEADD( MILLISECOND, " + numberOfSeconds + "," + dateValue + ")";
	}

		@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "DATEADD( SECOND, " + numberOfSeconds + "," + dateValue + ")";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "DATEADD( MINUTE, " + numberOfMinutes + "," + dateValue + ")";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "DATEADD( DAY, " + numberOfDays + "," + dateValue + ")";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATEADD( HOUR, " + numberOfHours + "," + dateValue + ")";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD( WEEK, " + numberOfWeeks + "," + dateValue + ")";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD( MONTH, " + numberOfMonths + "," + dateValue + ")";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATEADD( YEAR, " + numberOfYears + "," + dateValue + ")";
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(DAY, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(WEEK, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(MONTH, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(YEAR, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(HOUR, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(MINUTE, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(SECOND, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(MILLISECOND, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doTruncTransform(String realNumberExpression, String numberOfDecimalPlacesExpression) {
		//When the third parameter != 0 it truncates rather than rounds
		return " ROUND(" + realNumberExpression + ", " + numberOfDecimalPlacesExpression + ", 1)";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return "DATEPART(YEAR, " + dateExpression + ")";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return "DATEPART(MONTH, " + dateExpression + ")";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return "DATEPART(DAY, " + dateExpression + ")";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return "DATEPART(HOUR, " + dateExpression + ")";
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return "DATEPART(MINUTE, " + dateExpression + ")";
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return "DATEPART(SECOND , " + dateExpression + ")";
	}

	/**
	 * MS SQLServer does not support the LEASTOF operation natively.
	 * 
	 * @return FALSE
	 */
	@Override
	protected boolean supportsLeastOfNatively() {
		return false;
	}

	/**
	 * MS SQLServer does not support the GREATESTOF operation natively.
	 * 
	 * @return FALSE
	 */
	@Override
	protected boolean supportsGreatestOfNatively() {
		return false;
	}

	/**
	 * MS SQLServer does not support the grouping by columns that do not access table data.
	 * 
	 * @return FALSE
	 */
	@Override
	public boolean supportsPurelyFunctionalGroupByColumns() {
		return false;
	}

	/**
	 * Transforms a SQL snippet of a number expression into a character expression
	 * for this database.
	 *
	 * @param numberExpression	numberExpression
	 * @return a String of the SQL required to transform the number supplied into
	 * a character or String type.
	 */
	@Override
	public String doNumberToStringTransform(String numberExpression) {
		DBString dbs = new DBString();
		return "CONVERT(NVARCHAR(1000), "+numberExpression+")";
	}

	@Override
	public String beginWithClause() {
		return " WITH ";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " datepart(dw,("+dateSQL+"))";
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}
//
//	@Override
//	public String doBooleanArrayTransform(Boolean[] bools) {
//		StringBuilder str = new StringBuilder();
//		for (Boolean bool : bools) {
//			str.append((bool?"1":"0"));
//		}
//		Long parseLong = Long.parseLong(str.toString(),2);
//		return "0x"+ parseLong.byteValue();
//	}
}
