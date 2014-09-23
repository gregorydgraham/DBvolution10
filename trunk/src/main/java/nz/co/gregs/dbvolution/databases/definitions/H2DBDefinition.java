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

import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Defines the features of the H2 database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link H2DB} instances, and
 * you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class H2DBDefinition extends DBDefinition {

	private final String dateFormatStr = "yyyy-M-d hh:mm:ss";
	private final String h2DateFormatStr = "yyyy-M-d HH:mm:ss";
	private final SimpleDateFormat strToDateFormat = new SimpleDateFormat(dateFormatStr);

	@Override
	public String getDateFormattedForQuery(Date date) {
		if (date == null) {
			return getNull();
		}
		return "PARSEDATETIME('" + strToDateFormat.format(date) + "','" + h2DateFormatStr + "')";
	}

//	@Override
//	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
//		if (qdt instanceof DBDate){
//			return "TIMESTAMP WITH TIMEZONE";
//		}
//		return super.getSQLTypeOfDBDatatype(qdt);
//	}

	@Override
	public String formatTableName(DBRow table) {
		return table.getTableName().toUpperCase();
	}

	@Override
	public String formatColumnName(String columnName) {
		return columnName.toUpperCase();
	}

	@Override
	public String doAddDaysTransform(String dayValue, String numberOfDays) {
		return "DATEADD('day',"+numberOfDays+", "+dayValue+")";
	}

	@Override
	public String doAddSecondsTransform(String secondValue, String numberOfSeconds) {
		return "DATEADD('second',"+numberOfSeconds+","+secondValue+")";
	}

	@Override
	public String doAddMinutesTransform(String secondValue, String numberOfMinutes) {
		return "DATEADD('minute',"+numberOfMinutes+","+secondValue+")";
	}

	@Override
	public String doAddHoursTransform(String hourValue, String numberOfSeconds) {
		return "DATEADD('hour',"+numberOfSeconds+","+hourValue+")";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD('week',"+numberOfWeeks+","+dateValue+")";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD('month',"+numberOfMonths+","+dateValue+")";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATEADD('year',"+numberOfYears+","+dateValue+")";
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * @return the H@ implementation subtracts the time zone from the current timestamp
	 */
	@Override
	public String getCurrentTimestampFunction() {
		SimpleDateFormat format = new SimpleDateFormat("Z");
		long rawTimezone = Long.parseLong(format.format(new Date()).replaceAll("\\+", ""));
		long timezone = rawTimezone/100+((rawTimezone%100)*(100/60));
		return " DATEADD('hour',-1* "+timezone+",CURRENT_TIMESTAMP )";
	}
}
