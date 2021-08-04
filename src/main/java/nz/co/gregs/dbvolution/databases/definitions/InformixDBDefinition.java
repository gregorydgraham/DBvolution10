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

import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.InformixDB;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;

/**
 * Defines the features of the Informix database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link InformixDB} instances,
 * and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class InformixDBDefinition extends DBDefinition {

	public static final long serialVersionUID = 1L;
	
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	private static final String INFORMIX_DATE_FORMAT = "%Y-%m-%d %H:%M:%S%F3";
	private final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

	@Override
	public boolean prefersIndexBasedOrderByClause() {
		return true;
	}

	@Override
	public String getDateFormattedForQuery(Date date) {
		return "TO_DATE('" + dateFormat.format(date) + "','" + INFORMIX_DATE_FORMAT + "')";
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
				+ ", '" + INFORMIX_DATE_FORMAT + "')";
	}

	/**
	 *
	 * @param table table
	 * @param columnName columnName
	 * @return a string of the table and column name for the select clause
	 */
	@Override
	public String formatTableAndColumnName(DBRow table, String columnName) {
		return "" + formatTableName(table) + "." + formatColumnName(columnName) + "";
	}

	@Override
	public String getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		int rowLimit = options.getRowLimit();
		Integer pageNumber = options.getPageIndex();
		if (rowLimit < 1) {
			return "";
		} else {
			if (supportsPagingNatively(options)) {
				String rowLimitStr = " FIRST " + rowLimit + " ";
				long offset = pageNumber * rowLimit;
				String offsetStr = "";
				if (offset > 0L) {
					offsetStr = " SKIP " + offset;

				}
				return offsetStr + rowLimitStr;
			} else if (supportsRowLimitsNatively(options)) {
				return " FIRST " + rowLimit + " ";
			} else {
				return "";
			}
		}
	}

	@Override
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
		return "";
	}

	/**
	 * Provides the function name of the COALESCE, IFNULL, or NVL function.
	 *
	 * <p>
	 * Informix provides NVL only.
	 *
	 * @return "COALESCE"
	 */
	@Override
	public String getIfNullFunctionName() {
		return "NVL";
	}

	@Override
	protected String getCurrentTimeFunction() {
		throw new UnsupportedOperationException("Informix Does Not Support CurrentTime as a Function: Please use doCurrentTimeTransform() instead of getCurrentTimeFunction().");
	}

	@Override
	protected String getCurrentDateTimeFunction() {
		throw new UnsupportedOperationException("Informix Does Not Support CurrentDateTime as a Function: Please use doCurrentDateTimeTransform() instead of getCurrentDateTimeFunction().");
	}

	@Override
	protected String getCurrentDateOnlyFunctionName() {
		throw new UnsupportedOperationException("Informix Does Not Support CurrentDateOnly as a Function: Please use doCurrentDateOnlyTransform() instead of getCurrentDateOnlyFunctionName().");
	}

	@Override
	public String doCurrentTimeTransform() {
		return "(CURRENT HOUR TO FRACTION(3))";
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	@Override
	protected String getCurrentZonedDateTimeFunction() {
		return "(CURRENT)";
	}
	
	@Override
	public String doCurrentDateTimeTransform() {
		return "(CURRENT)";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return "(CURRENT YEAR TO DAY)";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return "DAY(" + dateExpression + ")";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return "MONTH(" + dateExpression + ")";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return "YEAR(" + dateExpression + ")";
	}

	@Override
	public boolean supportsPagingNatively(QueryOptions options) {
		return false;
	}

	@Override
	public boolean supportsGeneratedKeys() {
		return false;
	}

	@Override
	public String doDateAddYearsTransform(String dateValue, String numberOfYears) {
		return "((" + dateValue + ")+ (" + numberOfYears + ") UNITS YEAR)";
	}

	@Override
	public String doDateAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "((" + dateValue + ")+ (" + numberOfMonths + ") UNITS MONTH)";
	}

	@Override
	public String doDateAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "((" + dateValue + ")+ (" + numberOfWeeks + ") UNITS WEEK)";
	}

	@Override
	public String doDateAddHoursTransform(String dateValue, String numberOfHours) {
		return "((" + dateValue + ")+ (" + numberOfHours + ") UNITS HOUR)";
	}

	@Override
	public String doDateAddDaysTransform(String dateValue, String numberOfDays) {
		return "((" + dateValue + ")+ (" + numberOfDays + ") UNITS DAY)";
	}

	@Override
	public String doDateAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "((" + dateValue + ")+ (" + numberOfMinutes + ") UNITS MINUTE)";
	}

	@Override
	public String doDateAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "((" + dateValue + ")+ (" + numberOfSeconds + ") UNITS SECOND)";
	}

	@Override
	public String doInstantAddYearsTransform(String dateValue, String numberOfYears) {
		return "((" + dateValue + ")+ (" + numberOfYears + ") UNITS YEAR)";
	}

	@Override
	public String doInstantAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "((" + dateValue + ")+ (" + numberOfMonths + ") UNITS MONTH)";
	}

	@Override
	public String doInstantAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "((" + dateValue + ")+ (" + numberOfWeeks + ") UNITS WEEK)";
	}

	@Override
	public String doInstantAddHoursTransform(String dateValue, String numberOfHours) {
		return "((" + dateValue + ")+ (" + numberOfHours + ") UNITS HOUR)";
	}

	@Override
	public String doInstantAddDaysTransform(String dateValue, String numberOfDays) {
		return "((" + dateValue + ")+ (" + numberOfDays + ") UNITS DAY)";
	}

	@Override
	public String doInstantAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "((" + dateValue + ")+ (" + numberOfMinutes + ") UNITS MINUTE)";
	}

	@Override
	public String doInstantAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "((" + dateValue + ")+ (" + numberOfSeconds + ") UNITS SECOND)";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " (WEEKDAY(" + dateSQL + ")+1)";
	}

	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
		return " (WEEKDAY(" + dateSQL + ")+1)";
	}

	@Override
	public boolean willCloseConnectionOnStatementCancel() {
		return true;
	}

	@Override
	public boolean supportsStatementIsClosed() {
		return false;
	}

	@Override
	public boolean supportsDateRepeatDatatypeFunctions() {
		return false;
	}

	@Override
	public GroupByClauseMethod[] preferredGroupByClauseMethod() {
		return new GroupByClauseMethod[]{GroupByClauseMethod.INDEX};
	}
}
