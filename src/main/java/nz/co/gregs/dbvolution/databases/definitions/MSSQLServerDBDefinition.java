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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;
import java.text.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.AbstractColumn;
import nz.co.gregs.dbvolution.databases.MSSQLServerDB;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.Spatial2DExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import nz.co.gregs.dbvolution.internal.sqlserver.*;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;
import nz.co.gregs.dbvolution.results.ExpressionHasStandardStringResult;
import nz.co.gregs.dbvolution.results.Spatial2DResult;
import nz.co.gregs.separatedstring.SeparatedString;
import nz.co.gregs.dbvolution.utility.TemporalStringParser;
import nz.co.gregs.separatedstring.SeparatedStringBuilder;
import org.apache.commons.lang3.ArrayUtils;

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

	public static final long serialVersionUID = 1L;

	private static final String[] RESERVED_WORDS_ARRAY = new String[]{"ADD", "EXTERNAL", "PROCEDURE", "ALL", "FETCH", "PUBLIC", "ALTER", "FILE", "RAISERROR", "AND", "FILLFACTOR", "READ", "ANY", "FOR", "READTEXT", "AS", "FOREIGN", "RECONFIGURE", "ASC", "FREETEXT", "REFERENCES", "AUTHORIZATION", "FREETEXTTABLE", "REPLICATION", "BACKUP", "FROM", "RESTORE", "BEGIN", "FULL", "RESTRICT", "BETWEEN", "FUNCTION", "RETURN", "BREAK", "GOTO", "REVERT", "BROWSE", "GRANT", "REVOKE", "BULK", "GROUP", "RIGHT", "BY", "HAVING", "ROLLBACK", "CASCADE", "HOLDLOCK", "ROWCOUNT", "CASE", "IDENTITY", "ROWGUIDCOL", "CHECK", "IDENTITY_INSERT", "RULE", "CHECKPOINT", "IDENTITYCOL", "SAVE", "CLOSE", "IF", "SCHEMA", "CLUSTERED", "IN", "SECURITYAUDIT", "COALESCE", "INDEX", "SELECT", "COLLATE", "INNER", "SEMANTICKEYPHRASETABLE", "COLUMN", "INSERT", "SEMANTICSIMILARITYDETAILSTABLE", "COMMIT", "INTERSECT", "SEMANTICSIMILARITYTABLE", "COMPUTE", "INTO", "SESSION_USER", "CONSTRAINT", "IS", "SET", "CONTAINS", "JOIN", "SETUSER", "CONTAINSTABLE", "KEY", "SHUTDOWN", "CONTINUE", "KILL", "SOME", "CONVERT", "LEFT", "STATISTICS", "CREATE", "LIKE", "SYSTEM_USER", "CROSS", "LINENO", "TABLE", "CURRENT", "LOAD", "TABLESAMPLE", "CURRENT_DATE", "MERGE", "TEXTSIZE", "CURRENT_TIME", "NATIONAL", "THEN", "CURRENT_TIMESTAMP", "NOCHECK", "TO", "CURRENT_USER", "NONCLUSTERED", "TOP", "CURSOR", "NOT", "TRAN", "DATABASE", "NULL", "TRANSACTION", "DBCC", "NULLIF", "TRIGGER", "DEALLOCATE", "OF", "TRUNCATE", "DECLARE", "OFF", "TRY_CONVERT", "DEFAULT", "OFFSETS", "TSEQUAL", "DELETE", "ON", "UNION", "DENY", "OPEN", "UNIQUE", "DESC", "OPENDATASOURCE", "UNPIVOT", "DISK", "OPENQUERY", "UPDATE", "DISTINCT", "OPENROWSET", "UPDATETEXT", "DISTRIBUTED", "OPENXML", "USE", "DOUBLE", "OPTION", "USER", "DROP", "OR", "VALUES", "DUMP", "ORDER", "VARYING", "ELSE", "OUTER", "VIEW", "END", "OVER", "WAITFOR", "ERRLVL", "PERCENT", "WHEN", "ESCAPE", "PIVOT", "WHERE", "EXCEPT", "PLAN", "WHILE", "EXEC", "PRECISION", "WITH", "EXECUTE", "PRIMARY", "WITHIN GROUP", "EXISTS", "PRINT", "WRITETEXT", "EXIT", "PROC"};
	private static final List<String> RESERVED_WORDS = Arrays.asList(RESERVED_WORDS_ARRAY);

	@Override
	public String getDateFormattedForQuery(Date date) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		final String result = " CAST('" + format.format(date) + "' as DATETIME2(7)) ";
		return result;
	}

	@Override
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return "CAST("
				+ doConcatTransform(
						doNumberToStringTransform(years), "'-'", doNumberToStringTransform(months), "'-'", doNumberToStringTransform(days), "' '",
						doNumberToStringTransform(hours), "':'", doNumberToStringTransform(minutes), "':'", doNumberToStringTransform("(" + seconds + "+" + subsecond + ")")
				)
				+ " as DATETIME2(7))";
	}

	@Override
	public String doConcatTransform(String firstString, String secondString, String... rest) {
		SeparatedString sep = SeparatedStringBuilder.startsWith("(").separatedBy("+").endsWith(")")
				.add(firstString)
				.add(secondString)
				.addAll(rest);
		return sep.toString();
	}

	@Override
	protected String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBBoolean) {
			return " BIT ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDate) {
			return " DATETIME2(7) ";
		} else if (qdt instanceof DBInstant) {
			return " DATETIME2(7) ";
		} else if (qdt instanceof DBLocalDate) {
			return " DATETIME2(7) ";
		} else if (qdt instanceof DBLocalDateTime) {
			return " DATETIME2(7) ";
		} else if (qdt instanceof DBLargeBinary) {
			return " IMAGE ";
		} else if (qdt instanceof DBLargeText) {
			return " NTEXT ";
		} else if (qdt instanceof DBLargeObject) {
			return " IMAGE ";
		} else if (qdt instanceof DBString) {
			return " NVARCHAR(1000) COLLATE Latin1_General_CS_AS_KS_WS ";
		} else if (qdt instanceof DBPoint2D) {
			return " GEOMETRY ";
		} else if (qdt instanceof DBLineSegment2D) {
			return " GEOMETRY ";
		} else if (qdt instanceof DBLine2D) {
			return " GEOMETRY ";
		} else if (qdt instanceof DBPolygon2D) {
			return " GEOMETRY ";
		} else if (qdt instanceof DBMultiPoint2D) {
			return " GEOMETRY ";
//		} else if (qdt instanceof DBDuration) {
//			return " VARCHAR(65) ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public String doColumnTransformForSelect(QueryableDatatype<?> qdt, String selectableName) {
		if (!qdt.hasColumnExpression()) {
			if (qdt instanceof Spatial2DResult) {
				return "CAST((" + selectableName + ").STAsText() AS NVARCHAR(2000))";
			}
		}
		return super.doColumnTransformForSelect(qdt, selectableName);
//		return selectableName;
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return true;
	}

	//'2019-06-11 23:09:06.1075250 +12:00'
	DateTimeFormatter DATETIMEFORMATTER_WITH_ZONE = DateTimeFormatter.ofPattern("y-M-d H:m:s.n XXX");
	DateTimeFormatter DATETIMEFORMATTER_WITHOUT_ZONE = DateTimeFormatter.ofPattern("y-M-d H:m:s.n");

	@Override
	public String formatTableName(DBRow table) {
		final String schemaName = table.getSchemaName();
		if (table.getSchemaName() == null || "".equals(schemaName)) {
			return "[" + table.getTableName() + "]";
		} else {
			return "[" + table.getSchemaName() + "].[" + table.getTableName() + "]";
		}
	}

	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (RESERVED_WORDS.contains(sqlObjectName.toUpperCase())) {
			return ("O" + sqlObjectName.hashCode()).replaceAll("^[_-]", "O").replaceAll("-", "_");
		}
		return sqlObjectName;
	}

	@Override
	public String endSQLStatement() {
		return "";
	}

	@Override
	public String beginStringValue() {
		return " N'";
	}

	@Override
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
		int rowLimit = options.getRowLimit();
		Integer pageNumber = options.getPageIndex();
		if (rowLimit > 0 && supportsPagingNatively(options)) {
			long offset = pageNumber * rowLimit;
			return "OFFSET " + offset + " ROWS FETCH NEXT " + rowLimit + " ROWS ONLY";
		} else {
			return "";
		}
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
	 * While this seems useful, in fact it prevents checking for incorrect strings
	 * and breaks the industrial standard.
	 *
	 * @param firstSQLExpression the first string value to compare
	 * @param secondSQLExpression the second string value to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return super.doStringEqualsTransform(firstSQLExpression + "+'@'", secondSQLExpression + "+'@'");
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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

	/**
	 * Wraps the provided SQL snippet in a statement that the length of the value
	 * of the snippet.
	 *
	 * @param enclosedValue	enclosedValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL snippet
	 */
	@Override
	public String doStringLengthTransform(String enclosedValue) {
		return " CAST(" + getStringLengthFunctionName() + "( " + enclosedValue + " ) as NUMERIC(" + getNumericPrecision() + "," + getNumericScale() + "))";
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return " IDENTITY ";
	}

	@Override
	public boolean prefersLargeObjectsSetAsBase64String(DBLargeObject<?> lob) {
		return !(lob instanceof DBLargeBinary);
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream(DBLargeObject<?> lob) {
		return !(lob instanceof DBLargeBinary);
	}

	@Override
	protected String getCurrentDateOnlyFunctionName() {
		return " GETDATE";
	}

	@Override
	public String doBooleanToIntegerTransform(String booleanExpression) {
		return " case when (" + booleanExpression + ") then 1 when not (" + booleanExpression + ") then 0 else " + getNull() + " end ";
	}

	@Override
	public String doDateAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "DATEADD( SECOND, " + numberOfSeconds + "," + dateValue + ")";
	}

	@Override
	public String doDateAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "DATEADD( MINUTE, " + numberOfMinutes + "," + dateValue + ")";
	}

	@Override
	public String doDateAddDaysTransform(String dateValue, String numberOfDays) {
		return "DATEADD( DAY, " + numberOfDays + "," + dateValue + ")";
	}

	@Override
	public String doDateAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATEADD( HOUR, " + numberOfHours + "," + dateValue + ")";
	}

	@Override
	public String doDateAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD( WEEK, " + numberOfWeeks + "," + dateValue + ")";
	}

	@Override
	public String doDateAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD( MONTH, " + numberOfMonths + "," + dateValue + ")";
	}

	@Override
	public String doDateAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATEADD( YEAR, " + numberOfYears + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "DATEADD( SECOND, " + numberOfSeconds + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "DATEADD( MINUTE, " + numberOfMinutes + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddDaysTransform(String dateValue, String numberOfDays) {
		return "DATEADD( DAY, " + numberOfDays + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATEADD( HOUR, " + numberOfHours + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD( WEEK, " + numberOfWeeks + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD( MONTH, " + numberOfMonths + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddYearsTransform(String dateValue, String numberOfYears) {
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

//	@Override
//	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
//		return "(DATEDIFF(MILLISECOND, " + dateValue + "," + otherDateValue + "))";
//	}
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

	@Override
	public String doSecondAndSubsecondTransform(String dateExpression) {
		return "(" + doSubsecondTransform(dateExpression) + " + " + doSecondTransform(dateExpression) + ")";
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return "(DATEPART(MICROSECOND , " + dateExpression + ")/1000000.0000)";
	}

	@Override
	public String doInstantYearTransform(String dateExpression) {
		return "DATEPART(YEAR, " + dateExpression + ")";
	}

	@Override
	public String doInstantMonthTransform(String dateExpression) {
		return "DATEPART(MONTH, " + dateExpression + ")";
	}

	@Override
	public String doInstantDayTransform(String dateExpression) {
		return "DATEPART(DAY, " + dateExpression + ")";
	}

	@Override
	public String doInstantHourTransform(String dateExpression) {
		return "DATEPART(HOUR, " + dateExpression + ")";
	}

	@Override
	public String doInstantMinuteTransform(String dateExpression) {
		return "DATEPART(MINUTE, " + dateExpression + ")";
	}

	@Override
	public String doInstantSecondTransform(String dateExpression) {
		return "DATEPART(SECOND , " + dateExpression + ")";
	}

	@Override
	public String doInstantSubsecondTransform(String dateExpression) {
		return "(DATEPART(MILLISECOND , " + dateExpression + ")/1000.0000)";
	}

	@Override
	public boolean supportsComparingBooleanResults() {
		return false;
	}

	/**
	 * MS SQLServer does not support the LEASTOF operation natively.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return FALSE
	 */
	@Override
	protected boolean supportsGreatestOfNatively() {
		return false;
	}

	/**
	 * MS SQLServer does not support the grouping by columns that do not access
	 * table data.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL required to transform the number supplied into
	 * a character or String type.
	 */
	@Override
	protected String doNumberToStringTransformUnsafe(String numberExpression) {
		return "CONVERT(NVARCHAR(1000), " + numberExpression + ")";
	}

	/**
	 * Transforms a SQL snippet of a integer expression into a character
	 * expression for this database.
	 *
	 * @param numberExpression	numberExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL required to transform the number supplied into
	 * a character or String type.
	 */
	@Override
	protected String doIntegerToStringTransformUnsafe(String numberExpression) {
		return "CONVERT(NVARCHAR(1000), " + numberExpression + ")";
	}

	@Override
	public String beginWithClause() {
		return " WITH ";
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	@Override
	protected String getCurrentZonedDateTimeFunction() {
		return " SYSDATETIMEOFFSET() ";
	}

	@Override
	protected String getCurrentDateTimeFunction() {
		return " SYSDATETIME() ";
//		return " SYSDATETIMEOFFSET() ";
//		return " switchoffset("
//				+ "SYSDATETIME()" + ", '"
//				+ OffsetTime.now().format(DateTimeFormatter.ofPattern("ZZZZZ"))
//				+ "')";
	}

	@Override
	public String doCurrentUTCDateTimeTransform() {
		return " SYSUTCDATETIME()";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " datepart(dw,(" + dateSQL + "))";
	}

	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
		return " datepart(dw,(" + dateSQL + "))";
	}

	@Override
	public String doRoundTransform(String toSQLString) {
		return "ROUND(" + toSQLString + ", 0)";
	}

	@Override
	public String doRoundWithDecimalPlacesTransform(String number, String decimalPlaces) {
		return "ROUND(" + number + ", " + decimalPlaces + ")";
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

	@Override
	public String getArctan2FunctionName() {
		return "ATN2";
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return false;
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return "(CAST(0.0 as numeric(" + getNumericPrecision() + "," + getNumericScale() + "))+(CAST (" + stringResultContainingANumber + " as float)))";
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		if (columnExpression instanceof BooleanExpression) {
			final BooleanExpression boolExpr = (BooleanExpression) columnExpression;
			if (boolExpr.isWindowingFunction()) {
				return columnExpression;
			} else {
				return boolExpr.ifTrueFalseNull(1, 0, null).bracket();
			}
		} else if (columnExpression instanceof AbstractColumn) {
			Object col = ((AbstractColumn) columnExpression).getField();
			if (col != null && (col instanceof DBBoolean)) {
				final DBBoolean bool = (DBBoolean) col;
				final DBExpression[] exprns = bool.getColumnExpression();
				if (exprns.length > 0) {
					for (DBExpression expr : exprns) {/* TODO handle multiple expressions */
						if (expr instanceof BooleanExpression) {
							return ((BooleanExpression) expr).ifTrueFalseNull(1, 0, null).bracket();
						}
					}
				}
			}
		}
		return super.transformToStorableType(columnExpression);
	}

	@Override
	public DBExpression transformToSelectableType(DBExpression columnExpression) {
		if (columnExpression instanceof BooleanExpression) {
			final BooleanExpression boolExpr = (BooleanExpression) columnExpression;
			if (boolExpr.isWindowingFunction()) {
				return columnExpression;
			} else if (boolExpr.isBooleanStatement()) {
				return boolExpr.ifTrueFalseNull(1, 0, null).bracket();
			} else {
				return columnExpression;
			}
		} else if (columnExpression instanceof Spatial2DExpression) {
			return ((ExpressionHasStandardStringResult) columnExpression).stringResult();
		}
		return super.transformToSelectableType(columnExpression);
	}

	@Override
	public DBExpression transformToGroupableType(DBExpression expression) {
		if (expression instanceof BooleanExpression) {
			final BooleanExpression boolexpr = (BooleanExpression) expression;
			return boolexpr.ifTrueFalseNull(true, false, null);
		} else if (expression instanceof Spatial2DExpression) {
			return ((ExpressionHasStandardStringResult) expression).stringResult();
		} else {
			return super.transformToStorableType(expression);
		}
	}

	@Override
	public DBExpression transformToWhenableType(BooleanExpression test) {
		if (test.isBooleanStatement()) {
			return test;
		} else {
			return new BooleanExpression(test) {
				@Override
				public String toSQLString(DBDefinition db) {
					return "(" + super.toSQLString(db) + "=1)";
				}
			};
		}
	}

	@Override
	public DBExpression transformToSortableType(DBExpression columnExpression) {
		if (columnExpression instanceof BooleanExpression) {
			final BooleanExpression boolExpr = (BooleanExpression) columnExpression;
			if (boolExpr.isWindowingFunction()) {
				return columnExpression;
			} else {
				return boolExpr.ifTrueFalseNull(1, 0, null).bracket();
			}
		} else if (columnExpression instanceof AbstractColumn) {
			Object col = ((AbstractColumn) columnExpression).getField();
			if (col != null && (col instanceof DBBoolean)) {
				final DBBoolean bool = (DBBoolean) col;
				final DBExpression[] exprns = bool.getColumnExpression();
				if (exprns.length > 0) {
					for (DBExpression expr : exprns) {/* TODO handle multiple expressions */
						if (expr instanceof BooleanExpression) {
							return ((BooleanExpression) expr).ifTrueFalseNull(1, 0, null).bracket();
						} else {
							return super.transformToStorableType(columnExpression);
						}
					}
				} else {
					return super.transformToStorableType(columnExpression);
				}
			} else {
				return super.transformToStorableType(columnExpression);
			}
		} else {
			return super.transformToStorableType(columnExpression);
		}
		return super.transformToStorableType(columnExpression);
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return "(" + Point2DFunctions.EQUALS + "((" + firstPoint + "), (" + secondPoint + "))=1)";
	}

	@Override
	public String doPoint2DGetXTransform(String point2D) {
		return "(" + point2D + ").STX";
	}

	@Override
	public String doPoint2DGetYTransform(String point2D) {
		return "(" + point2D + ").STY";
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String point2D) {
		return "(" + point2D + ").STDimension()";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2D) {
		return "(" + point2D + ").STEnvelope()";
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DString) {
		return "CAST((" + point2DString + ").STAsText() as varchar(1000))";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "CAST((" + line2DSQL + ").STAsText() as nvarchar(2000))";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return "(" + toSQLString + ").STEnvelope()";
	}

	@Override
	public String doLine2DMeasurableDimensionsTransform(String toSQLString) {
		return super.doLine2DMeasurableDimensionsTransform(toSQLString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DEqualsTransform(String toSQLString, String toSQLString0) {
		return super.doLine2DEqualsTransform(toSQLString, toSQLString0); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return "((" + firstLine + ").STIntersects(" + secondLine + ")=1)";

	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return "(" + firstLine + ").STIntersection(" + secondLine + ")";
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return "(" + firstGeometry + ").STIntersection(" + secondGeometry + ")";
	}

	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString line) {
		return "geometry::STGeomFromText ('" + line.toText() + "',0).MakeValid().STUnion(geometry::STGeomFromText('" + line.toText() + "', 0).MakeValid().STStartPoint())";
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "geometry::STGeomFromText ('POINT (" + xValue + " " + yValue + ")',0)";
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		return "geometry::STGeomFromText ('" + point.toText() + "',0)";
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon2DInWKTFormat) {
		StringBuilder str = new StringBuilder();
		String separator = "";
		Coordinate[] coordinates = polygon2DInWKTFormat.getCoordinates();
		for (Coordinate coordinate : coordinates) {
			str.append(separator).append(coordinate.x).append(" ").append(coordinate.y);
			separator = ", ";
		}

		return "geometry::STGeomFromText('POLYGON ((" + str + "))', 0).MakeValid().STUnion(geometry::STGeomFromText('POLYGON ((" + str + "))', 0).MakeValid().STStartPoint())";
	}

	@Override
	public String transformCoordinateArrayToDatabasePolygon2DFormat(List<String> coordinateSQL) {

		StringBuilder str = new StringBuilder();
		String separator = "";
		for (String coordinate : coordinateSQL) {
			str.append(separator).append(coordinate);
			if (separator.equals(" ")) {
				separator = ",";
			} else {
				separator = " ";
			}
		}
//'POLYGON ((12 12, 13 12, 13 13, 12 13, 12 12))'
// the STUnion corrects for the unusual handedness of SQLServer
		return "geometry::STGeomFromText('POLYGON ((" + str + "))', 0).MakeValid().STUnion(geometry::STGeomFromText('POLYGON ((" + str + "))', 0).MakeValid().STStartPoint())";
	}

	@Override
	public String transformPoint2DArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		//PointFromText('POINT (0 0)') => POLYGON((0.0, 0.0), ... )
		StringBuilder str = new StringBuilder();
		String separator = "";
		for (String point : pointSQL) {
			final String coordsOnly = point.replaceAll("geometry::STGeomFromText \\('POINT \\(", "").replaceAll("\\)',0\\)", "");
			str.append(separator).append(coordsOnly);
			separator = ",";
		}

		return "geometry::STGeomFromText('POLYGON ((" + str + "))', 0).MakeValid().STUnion(geometry::STGeomFromText('POLYGON ((" + str + "))', 0).MakeValid().STStartPoint())";
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return "((" + polygonSQL + ").STAsText())";
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return "((" + firstGeometry + ").STIntersection(" + secondGeometry + "))";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
//		return "(" + firstGeometry + ") ?#  (" + secondGeometry + ")";
		return "((" + firstGeometry + ").STOverlaps(" + secondGeometry + ")=1)";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return "((" + firstGeometry + ").STIntersects(" + secondGeometry + ")=1)";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return "((" + firstGeometry + ").STTouches(" + secondGeometry + ")=1)";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String toSQLString) {
		return "((" + toSQLString + ").STArea())";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String toSQLString) {
		return "(" + toSQLString + ").STEnvelope()";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		//return "((" + firstGeometry + ").STEquals(" + secondGeometry + ")=1)";
		return "(" + Polygon2DFunctions.EQUALS + "((" + firstGeometry + "), (" + secondGeometry + "))=1)";
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
	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return "((" + firstGeometry + ").STContains(" + secondGeometry + ")=1)";
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String polygon2DSQL, String point2DSQL) {
		return "((" + polygon2DSQL + ").STContains(" + point2DSQL + ")=1)";
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
	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return "((" + firstGeometry + ").STDisjoint(" + secondGeometry + ")=1)";
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
	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		return "((" + firstGeometry + ").STWithin(" + secondGeometry + ")=1)";
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
	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String polygon2DSQL) {
		return "((" + polygon2DSQL + ").STDimension())";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return "((" + polygon2DSQL + ").STExteriorRing())";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return doPoint2DGetXTransform("((" + polygon2DSQL + ").STExteriorRing().STPointN(2))");
	}

	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return doPoint2DGetXTransform("((" + polygon2DSQL + ").STExteriorRing().STPointN(1))");
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return doPoint2DGetYTransform("((" + polygon2DSQL + ").STExteriorRing().STPointN(3))");
	}

	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return doPoint2DGetYTransform("((" + polygon2DSQL + ").STExteriorRing().STPointN(1))");
	}

	@Override
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineSegmentAsSQL) throws com.vividsolutions.jts.io.ParseException {
		return super.transformDatabaseLineSegment2DValueToJTSLineSegment(lineSegmentAsSQL);
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		LineString line = (new GeometryFactory()).createLineString(new Coordinate[]{lineSegment.getCoordinate(0), lineSegment.getCoordinate(1)});
		return transformLineStringIntoDatabaseLine2DFormat(line);
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return doLine2DIntersectsLine2DTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String toSQLString) {
		return doLine2DGetMaxXTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String toSQLString) {
		return doLine2DGetMinXTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String toSQLString) {
		return doLine2DGetMaxYTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String toSQLString) {
		return doLine2DGetMinYTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String toSQLString) {
		return doLine2DGetBoundingBoxTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DDimensionTransform(String toSQLString) {
		return doLine2DMeasurableDimensionsTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String toSQLString, String toSQLString0) {
		return doLine2DNotEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doLineSegment2DEqualsTransform(String toSQLString, String toSQLString0) {
		return doLine2DEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doLineSegment2DAsTextTransform(String toSQLString) {
		return doLine2DAsTextTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return doLine2DIntersectionPointWithLine2DTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		return "geometry::STGeomFromText ('" + points.toText() + "',0).MakeValid().STUnion(geometry::STGeomFromText('" + points.toText() + "', 0).MakeValid().STStartPoint())";
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		MultiPoint mpoint = null;
		WKTReader wktReader = new WKTReader();
		if (pointsAsString == null || pointsAsString.isEmpty()) {
			mpoint = (new GeometryFactory()).createMultiPoint(new Point[]{});
		} else {
			Geometry geometry = wktReader.read(pointsAsString);
			if (geometry.isEmpty()) {
				mpoint = (new GeometryFactory()).createMultiPoint(new Coordinate[]{});
			} else if (geometry instanceof MultiPoint) {
				mpoint = (MultiPoint) geometry;
			} else if (geometry instanceof Point) {
				Point point = (Point) geometry;
				mpoint = (new GeometryFactory()).createMultiPoint(new Point[]{point});
			} else {
				throw new IncorrectGeometryReturnedForDatatype(geometry, mpoint);
			}
		}
		return mpoint;
	}

	@Override
	public Point transformDatabasePoint2DValueToJTSPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		Point mpoint = null;
		WKTReader wktReader = new WKTReader();
		if (pointsAsString == null || pointsAsString.isEmpty()) {
			mpoint = (new GeometryFactory()).createPoint(new Coordinate());
		} else {
			Geometry geometry = wktReader.read(pointsAsString);
			if (geometry.isEmpty()) {
				mpoint = (new GeometryFactory()).createPoint(new Coordinate());
			} else if (geometry instanceof Point) {
				mpoint = (Point) geometry;
			} else {
				throw new IncorrectGeometryReturnedForDatatype(geometry, mpoint);
			}
		}
		return mpoint;
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return "(" + MultiPoint2DFunctions.EQUALS + "((" + first + "), (" + second + "))=1)";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return "(" + first + ").STPointN(" + doMultiPoint2DGetNumberOfPointsTransform(first) + " - (" + index + " -1))";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return "(" + first + ").STNumPoints()";
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String first) {
		return "(" + first + ").STDimension()";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return "(" + first + ").STEnvelope()";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return "(" + first + ").STAsText()";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return "geometry::STLineFromText('LINESTRING (' + replace(replace((SUBSTRING((" + first + ").ToString(),11,9999999)),'(','' ),')', '')+')',0)";
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String first) {
		return MultiPoint2DFunctions.MINY + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String first) {
		return MultiPoint2DFunctions.MINX + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String first) {
		return MultiPoint2DFunctions.MAXY + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String first) {
		return MultiPoint2DFunctions.MAXX + "(" + first + ")";
	}

	@Override
	public String getTrueValue() {
		return " 1 ";
	}

	@Override
	public String getFalseValue() {
		return " 0 ";
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.CHARSTREAM;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BLOB;
		} else {
			return super.preferredLargeObjectWriter(lob);
		}
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.STRING;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BLOB;
		} else {
			return super.preferredLargeObjectReader(lob);
		}
	}

	/**
	 * Return the function name for the RoundUp function.
	 *
	 * <p>
	 * For MS SQLServer this method returns <b>ceiling</b></p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	@Override
	public String getRoundUpFunctionName() {
		return "ceiling";
	}

	/**
	 * Return the function name for the Natural Logarithm function.
	 *
	 * <p>
	 * For SQLServer this method returns <b>log</b></p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	@Override
	public String getNaturalLogFunctionName() {
		return "log";
	}

	/**
	 * Return the function name for the Logarithm Base10 function.
	 *
	 * <p>
	 * By default this method returns <b>log10</b></p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	@Override
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
	 * This method DOES NOT use the SQLServer built-in function as it does not
	 * produce a different result for different rows in a single query.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return random number generating code
	 */
	@Override
	public String doRandomNumberTransform() {
		return " (ABS(cast(CHECKSUM(NewId())as BIGINT))/2147483648) ";
	}

	@Override
	public String doFindNumberInStringTransform(String toSQLString) {
		return MigrationFunctions.FINDFIRSTNUMBER + "(" + toSQLString + ')';
	}

	@Override
	public String doFindIntegerInStringTransform(String toSQLString) {
		return MigrationFunctions.FINDFIRSTINTEGER + "(" + toSQLString + ')';
	}

	@Override
	public Collection<? extends String> getInsertPreparation(DBRow table) {
		final ArrayList<String> strs = new ArrayList<String>();
		if (table.hasAutoIncrementField() && table.getAutoIncrementField().getQueryableDatatype().hasBeenSet()) {
			strs.add("SET IDENTITY_INSERT " + this.formatTableName(table) + " ON;");
		}
		return strs;
	}

	@Override
	public Collection<? extends String> getInsertCleanUp(DBRow table) {
		final ArrayList<String> strs = new ArrayList<String>();
		if (table.hasAutoIncrementField() && table.getAutoIncrementField().getQueryableDatatype().hasBeenSet()) {
			strs.add("SET IDENTITY_INSERT " + this.formatTableName(table) + " OFF;");
		}
		return strs;
	}

	@Override
	public String getAlterTableAddColumnSQL(DBRow existingTable, PropertyWrapper<?, ?, ?> columnPropertyWrapper) {
		return "ALTER TABLE " + formatTableName(existingTable) + " ADD " + getAddColumnColumnSQL(columnPropertyWrapper) + endSQLStatement();
	}

	@Override
	public boolean supportsNullsOrderingStandard() {
		return false;
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String referencedTable) {
		return "(STRING_AGG(" + accumulateColumn + ", " + doStringLiteralWrapping(separator) + "))";
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String orderByColumnName, String referencedTable) {
		return "(STRING_AGG(" + accumulateColumn + ", " + doStringLiteralWrapping(separator) + ") WITHIN GROUP (ORDER BY " + orderByColumnName + "))";
	}

	@Override
	public boolean requiresClosedPolygons() {
		return true;
	}

	@Override
	public boolean requiresOnClauseForAllJoins() {
		return true;
	}

	@Override
	public boolean requiresReversingLineStringsFromDatabase() {
		return true;
	}

	@Override
	public LineString transformDatabaseLine2DValueToJTSLineString(String lineStringAsSQL) throws com.vividsolutions.jts.io.ParseException {
		final GeometryFactory geom = new GeometryFactory();
		LineString lineString = geom.createLineString(new Coordinate[]{});
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(lineStringAsSQL);
		if (geometry.isEmpty()) {
			lineString = geom.createLineString(new Coordinate[]{});
		} else if (geometry instanceof LineString) {
			lineString = (LineString) geometry;
			Coordinate[] coords = lineString.getCoordinates();
			ArrayUtils.reverse(coords);
			lineString = geom.createLineString(coords);
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, lineString);
		}
		return lineString;
	}

	@Override
	public String getDefaultOrderingClause() {
		return "ORDER BY (SELECT NULL)";
	}

	@Override
	public boolean supportsDurationNatively() {
		return false;
	}

	@Override
	public boolean supportsDateRepeatDatatypeFunctions() {
		return false;
	}
	
}
