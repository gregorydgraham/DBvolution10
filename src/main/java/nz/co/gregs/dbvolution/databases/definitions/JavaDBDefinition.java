/*
 * Copyright 2014 gregorygraham.
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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;

/**
 * The DBDefinition to use for JavaDB databases.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class JavaDBDefinition extends DBDefinition {

	public static final long serialVersionUID = 1L;
	
	private static final String DB_DATE_FORMAT_STR = "yyyy-M-d HH:mm:ss.SSS";//2017-02-18 18:59:59.000 +10:00
	private final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static final String[] RESERVED_WORD_ARRAY = new String[]{};
	private static final List<String> RESERVED_WORDS = Arrays.asList(RESERVED_WORD_ARRAY);

	@Override
	public String getDateFormattedForQuery(Date date) {
//		yyyy-mm-dd hh[:mm[:ss
		return "TIMESTAMP('" + DATETIME_FORMAT.format(date) + "')";
	}

	@Override
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return "PARSEDATETIME("
				+ years
				+ "||'-'||" + months
				+ "||'-'||" + days
				+ "||' '||" + hours
				+ "||':'||" + minutes
				+ "||':'||(" + seconds+"+"+subsecond+")"
//				+ "||' '||" + timeZoneSign
//				+ "||" + timeZoneHourOffset
//				+ "||" + timeZoneMinuteOffSet
				+ ", '" + DB_DATE_FORMAT_STR + "')";
		//return "PARSEDATETIME('" + years + "','" + H2_DATE_FORMAT_STR + "')";
	}

	@Override
	protected String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBBoolean) {
			return "SMALLINT";
		} else if (qdt instanceof DBJavaObject) {
			return "BLOB";
		} else if (qdt instanceof DBDate) {
			return "TIMESTAMP";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt); //To change body of generated methods, choose Tools | Templates.
		}
	}

	@Override
	public String formatTableName(DBRow table
	) {
		final String sqlObjectName = table.getTableName();
		return formatNameForJavaDB(sqlObjectName);
	}

	@Override
	public String getPrimaryKeySequenceName(String table, String column
	) {
		return formatNameForJavaDB(super.getPrimaryKeySequenceName(table, column));
	}

	@Override
	public String getPrimaryKeyTriggerName(String table, String column
	) {
		return formatNameForJavaDB(super.getPrimaryKeyTriggerName(table, column));
	}

	@Override
	public String formatColumnName(String column
	) {
		return formatNameForJavaDB(super.formatColumnName(column));
	}

	private static String formatNameForJavaDB(final String sqlObjectName) {
		if (sqlObjectName.length() < 30 && !(RESERVED_WORDS.contains(sqlObjectName.toUpperCase()))) {
			return sqlObjectName.replaceAll("^[_-]", "O").replaceAll("-", "_");
		} else {
			return ("O" + sqlObjectName.hashCode()).replaceAll("^[_-]", "O").replaceAll("-", "_");
		}
	}

	@Override
	public String formatTableAlias(String tabRow) {
		return "\"" + tabRow.replaceAll("-", "_") + "\"";
	}

	@Override
	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return ("DB" + formattedName.hashCode()).replaceAll("-", "_") + "";
	}

	@Override
	public String beginTableAlias() {
		return " ";
	}

	@Override
	public String endInsertLine() {
		return "";
	}

	@Override
	public String endDeleteLine() {
		return "";
	}

	@Override
	public String endSQLStatement() {
		return "";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ (length.trim().isEmpty() ? "" : ", " + length)
				+ ") ";
	}

	@Override
	public boolean prefersLargeObjectsReadAsBLOB(DBLargeObject<?> lob) {
		return true;
	}

	@Override
	public boolean supportsPagingNatively(QueryOptions options) {
		return false;
	}

	@Override
	public String doTruncTransform(String realNumberExpression, String numberOfDecimalPlacesExpression) {
		return "(case when " + realNumberExpression + " >= 0 then floor(exp(" + numberOfDecimalPlacesExpression + " * ln(10)) * " + realNumberExpression + ") / exp(" + numberOfDecimalPlacesExpression + " * ln(10)) else ceil(exp(" + numberOfDecimalPlacesExpression + " * ln(10)) * " + realNumberExpression + ") / exp(" + numberOfDecimalPlacesExpression + " * ln(10)) end)";
	}

	@Override
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return " MOD(" + firstNumber + "," + secondNumber + ") ";
	}

	/**
	 * JavaDB does not support the GREATESTOF operation natively.
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
	 * JavaDB does not support the LEASTOF operation natively.
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

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "LOCATE(" + stringToFind + ", " + originalString + ")";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return "YEAR(" + dateExpression + ")";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return "MONTH(" + dateExpression + ")";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return "DAY(" + dateExpression + ")";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return "HOUR(" + dateExpression + ")";
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return "MINUTE(" + dateExpression + ")";
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return "SECOND(" + dateExpression + ")";
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return "(MILLISECOND(" + dateExpression + ")/1000.0000)";
	}

	@Override
	public String doDateAddSecondsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_SECOND, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doDateAddMinutesTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_MINUTE, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doDateAddHoursTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_HOUR, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doDateAddDaysTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_DAY, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doDateAddWeeksTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_WEEK, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doDateAddMonthsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_MONTH, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doDateAddYearsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_YEAR, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doInstantAddSecondsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_SECOND, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doInstantAddMinutesTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_MINUTE, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doInstantAddHoursTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_HOUR, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doInstantAddDaysTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_DAY, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doInstantAddWeeksTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_WEEK, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doInstantAddMonthsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_MONTH, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doInstantAddYearsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_YEAR, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doReplaceTransform(String withinString, String findString, String replaceString) {
		String startIndex = "Locate(" + findString + "," + withinString + ")";
		String length = "Length(" + findString + ")";
		return "(case when " + startIndex + "> 0 then SUBSTR(" + withinString + ", 1, " + startIndex + "-1)||" + replaceString + "||SUBSTR(" + withinString + ", " + startIndex + "+" + length + ") else " + withinString + " end)";
	}

	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return "(" + super.doStringEqualsTransform(firstSQLExpression, secondSQLExpression) + " AND LENGTH(" + firstSQLExpression + ") = LENGTH(" + secondSQLExpression + "))";
	}

	@Override
	protected String doNumberToStringTransformUnsafe(String numberExpression) {
		return "trim(cast(cast(" + numberExpression + " as char(38)) as varchar(1000)))";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return "cast(cast((cast( " + getCurrentDateOnlyFunctionName() + "  as VARCHAR(1000))||' 00:00:00') as VARCHAR(1000)) as TIMESTAMP) ";
	}

	@Override
	public boolean supportsStandardDeviationFunction() {
		return false;
	}

	@Override
	public String getOrderByDescending() {
		return " DESC ";
	}

	@Override
	public String getOrderByAscending() {
		return " ASC ";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		throw new UnsupportedOperationException("JavaDB does not support the DAYOFWEEK function");
	}

	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
		throw new UnsupportedOperationException("JavaDB does not support the DAYOFWEEK function");
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}

//	@Override
//	public String getLocalDateFormattedForQuery(LocalDate date) {
//		return "TIMESTAMP('" + DATETIME_FORMAT.format(date) + "')";
//	}
//
//	@Override
//	public String getLocalDateTimeFormattedForQuery(LocalDateTime date) {
//		return "TIMESTAMP('" + DATETIME_FORMAT.format(date) + "')";
//	}

	@Override
	public boolean supportsDateRepeatDatatypeFunctions() {
		return false;
	}
}
