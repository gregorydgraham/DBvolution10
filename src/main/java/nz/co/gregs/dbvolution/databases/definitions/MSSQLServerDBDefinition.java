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
	public boolean supportsXOROperator() {
		return true;
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
	protected boolean supportsLeastOfNatively() {
		return false;
	}

	@Override
	protected boolean supportsGreatestOfNatively() {
		return false;
	}

	@Override
	public boolean supportsPurelyFunctionalGroupByColumns() {
		return false;
	}
}
