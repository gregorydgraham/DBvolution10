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
import nz.co.gregs.dbvolution.databases.H2DB;
import static nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition.INTERVAL_CREATION_FUNCTION;
import static nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition.INTERVAL_DATEADDITION_FUNCTION;
import static nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition.INTERVAL_DATESUBTRACTION_FUNCTION;
import static nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition.INTERVAL_GREATERTHANEQUALS_FUNCTION;
import static nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition.INTERVAL_GREATERTHAN_FUNCTION;
import static nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition.INTERVAL_LESSTHANEQUALS_FUNCTION;
import static nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition.INTERVAL_LESSTHAN_FUNCTION;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInterval;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.datatypes.IntervalImpl;
import org.joda.time.Period;

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

	private final String dateFormatStr = "yyyy-M-d HH:mm:ss Z";
	private final String h2DateFormatStr = "yyyy-M-d HH:mm:ss Z";
	private final SimpleDateFormat strToDateFormat = new SimpleDateFormat(dateFormatStr);
	public static String INTERVAL_CREATION_FUNCTION = "DBV_INTERVAL_CREATE";
	public static String INTERVAL_EQUALS_FUNCTION = "DBV_INTERVAL_EQUALS";
	public static String INTERVAL_LESSTHAN_FUNCTION = "DBV_INTERVAL_LESSTHAN";
	public static String INTERVAL_LESSTHANEQUALS_FUNCTION = "DBV_INTERVAL_LESSTHANEQUALS";
	public static String INTERVAL_GREATERTHAN_FUNCTION = "DBV_INTERVAL_GREATERTHAN";
	public static String INTERVAL_GREATERTHANEQUALS_FUNCTION = "DBV_INTERVAL_GREATERTHANEQUALS";
	public static String INTERVAL_DATEADDITION_FUNCTION = "DBV_INTERVAL_DATEPLUSINTERVAL";
	public static String INTERVAL_DATESUBTRACTION_FUNCTION = "DBV_INTERVAL_DATEMINUS";


	@Override
	public String getDateFormattedForQuery(Date date) {
		if (date == null) {
			return getNull();
		}
		return "PARSEDATETIME('" + strToDateFormat.format(date) + "','" + h2DateFormatStr + "')";
	}

	@Override
	public String formatTableName(DBRow table) {
		return table.getTableName().toUpperCase();
	}

	@Override
	public String formatColumnName(String columnName) {
		return columnName.toUpperCase();
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBInterval) {
			return " DBV_INTERVAL ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public String doAddDaysTransform(String dayValue, String numberOfDays) {
		return "DATEADD('day'," + numberOfDays + ", " + dayValue + ")";
	}

	@Override
	public String doAddSecondsTransform(String secondValue, String numberOfSeconds) {
		return "DATEADD('second'," + numberOfSeconds + "," + secondValue + ")";
	}

	@Override
	public String doAddMinutesTransform(String secondValue, String numberOfMinutes) {
		return "DATEADD('minute'," + numberOfMinutes + "," + secondValue + ")";
	}

	@Override
	public String doAddHoursTransform(String hourValue, String numberOfSeconds) {
		return "DATEADD('hour'," + numberOfSeconds + "," + hourValue + ")";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD('WEEK'," + numberOfWeeks + "," + dateValue + ")";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD('month'," + numberOfMonths + "," + dateValue + ")";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATEADD('year'," + numberOfYears + "," + dateValue + ")";
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * @return the H@ implementation subtracts the time zone from the current
	 * timestamp
	 */
	@Override
	protected String getCurrentDateTimeFunction() {
//		SimpleDateFormat format = new SimpleDateFormat("Z");
//		long rawTimezone = Long.parseLong(format.format(new Date()).replaceAll("\\+", ""));
//		long timezone = rawTimezone/100+((rawTimezone%100)*(100/60));
//		return " DATEADD('hour',-1* "+timezone+",CURRENT_TIMESTAMP )";
		return " CURRENT_TIMESTAMP ";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " DAY_OF_WEEK("+dateSQL+")";
	}
	
	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		StringBuilder str = new StringBuilder();
		str.append("(");
		String separator = "";
		if (bools.length == 0) {
			return "()";
		} else if (bools.length == 1) {
			return "(" + bools[0] + ",)";
		} else {
			for (Boolean bool : bools) {
				str.append(separator).append(bool.toString().toUpperCase());
				separator = ",";
			}
			str.append(")");
			return str.toString();
		}
	}

	@Override
	public String transformPeriodIntoInterval(Period interval) {
		return "'"+IntervalImpl.getIntervalString(interval)+"'";
	}

	@Override
	public Period parseIntervalFromGetString(String intervalStr) {
		return IntervalImpl.parseIntervalFromGetString(intervalStr);
	}

	@Override
	public String doDateMinusTransformation(String leftHandSide, String rightHandSide) {
		return " "+INTERVAL_CREATION_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateIntervalAdditionTransform(String leftHandSide, String rightHandSide) {
		return " "+INTERVAL_DATEADDITION_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateIntervalSubtractionTransform(String leftHandSide, String rightHandSide) {
		return " "+INTERVAL_DATESUBTRACTION_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doIntervalEqualsTransform(String leftHandSide, String rightHandSide) {
		return " "+INTERVAL_EQUALS_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doIntervalLessThanTransform(String leftHandSide, String rightHandSide) {
		return INTERVAL_LESSTHAN_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doIntervalLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return INTERVAL_LESSTHANEQUALS_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doIntervalGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return INTERVAL_GREATERTHAN_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doIntervalGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return INTERVAL_GREATERTHANEQUALS_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

}
