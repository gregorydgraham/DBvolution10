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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class InformixDBDefinition extends DBDefinition {

	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	//TO_DATE("1998-07-07 10:24",   "%Y-%m-%d %H:%M")
	private static final String INFORMIX_DATE_FORMAT = "%Y-%m-%d %H:%M:%S%F3";
	private final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

	@Override
	public boolean prefersIndexBasedGroupByClause() {
		return true;
	}

	@Override
	public boolean prefersIndexBasedOrderByClause() {
		return true;
	}

	@Override
	public String getDateFormattedForQuery(Date date) {
		return "TO_DATE('" + dateFormat.format(date) + "','" + INFORMIX_DATE_FORMAT + "')";
	}

	/**
	 *
	 * @param table table
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a string of the table and column name for the select clause
	 */
	@Override
	public String formatTableAndColumnName(DBRow table, String columnName) {
		return "" + formatTableName(table) + "." + formatColumnName(columnName) + "";
	}

	@Override
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "((" + dateValue + ")+ (" + numberOfYears + ") UNITS YEAR)";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "((" + dateValue + ")+ (" + numberOfMonths + ") UNITS MONTH)";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "((" + dateValue + ")+ (" + numberOfWeeks + ") UNITS WEEK)";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "((" + dateValue + ")+ (" + numberOfHours + ") UNITS HOUR)";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "((" + dateValue + ")+ (" + numberOfDays + ") UNITS DAY)";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "((" + dateValue + ")+ (" + numberOfMinutes + ") UNITS MINUTE)";
	}

	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "((" + dateValue + ")+ (" + numberOfSeconds + ") UNITS SECOND)";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
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
}
