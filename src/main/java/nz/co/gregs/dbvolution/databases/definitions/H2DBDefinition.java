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
import nz.co.gregs.dbvolution.datatypes.DBDateRepeat;
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

	private final String dateFormatStr = "yyyy-M-d HH:mm:ss Z";
	private final String h2DateFormatStr = "yyyy-M-d HH:mm:ss Z";
	private final SimpleDateFormat strToDateFormat = new SimpleDateFormat(dateFormatStr);
	public static String DATEREPEAT_CREATION_FUNCTION = "DBV_DATEREPEAT_CREATE";
	public static String DATEREPEAT_EQUALS_FUNCTION = "DBV_DATEREPEAT_EQUALS";
	public static String DATEREPEAT_LESSTHAN_FUNCTION = "DBV_DATEREPEAT_LESSTHAN";
	public static String DATEREPEAT_LESSTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_LESSTHANEQUALS";
	public static String DATEREPEAT_GREATERTHAN_FUNCTION = "DBV_DATEREPEAT_GREATERTHAN";
	public static String DATEREPEAT_GREATERTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_GREATERTHANEQUALS";
	public static String DATEREPEAT_DATEADDITION_FUNCTION = "DBV_DATEREPEAT_DATEPLUSDATEREPEAT";
	public static String DATEREPEAT_DATESUBTRACTION_FUNCTION = "DBV_DATEREPEAT_DATEMINUSDATEREPEAT";
	
	public static String DATEREPEAT_YEAR_PART_FUNCTION = "DBV_DATEREPEAT_YEAR_PART";
	public static String DATEREPEAT_MONTH_PART_FUNCTION = "DBV_DATEREPEAT_MONTH_PART";
	public static String DATEREPEAT_DAY_PART_FUNCTION = "DBV_DATEREPEAT_DAY_PART";
	public static String DATEREPEAT_HOUR_PART_FUNCTION = "DBV_DATEREPEAT_HOUR_PART";
	public static String DATEREPEAT_MINUTE_PART_FUNCTION = "DBV_DATEREPEAT_MINUTE_PART";
	public static String DATEREPEAT_SECOND_PART_FUNCTION = "DBV_DATEREPEAT_SECOND_PART";
	public static String DATEREPEAT_MILLISECOND_PART_FUNCTION = "DBV_DATEREPEAT_MILLI_PART";


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
		if (qdt instanceof DBDateRepeat) {
			return " DBV_DATEREPEAT ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public String doAddDaysTransform(String dayValue, String numberOfDays) {
		return "DATEADD('day'," + numberOfDays + ", " + dayValue + ")";
	}

	@Override
	public String doAddMillisecondsTransform(String secondValue, String numberOfSeconds) {
		return "DATEADD('millisecond'," + numberOfSeconds + "," + secondValue + ")";
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

//	@Override
//	public String transformPeriodIntoDateRepeat(Period interval) {
//		return "'"+DateRepeatImpl.getIntervalString(interval)+"'";
//	}

//	@Override
//	public Period parseDateRepeatFromGetString(String intervalStr) {
//		return DateRepeatImpl.parseDateRepeatFromGetString(intervalStr);
//	}

	@Override
	public String doDateMinusToDateRepeatTransformation(String leftHandSide, String rightHandSide) {
		return " "+DATEREPEAT_CREATION_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDatePlusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " "+DATEREPEAT_DATEADDITION_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateMinusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " "+DATEREPEAT_DATESUBTRACTION_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateRepeatEqualsTransform(String leftHandSide, String rightHandSide) {
		return " "+DATEREPEAT_EQUALS_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateRepeatLessThanTransform(String leftHandSide, String rightHandSide) {
		return DATEREPEAT_LESSTHAN_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateRepeatLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DATEREPEAT_LESSTHANEQUALS_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateRepeatGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return DATEREPEAT_GREATERTHAN_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateRepeatGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DATEREPEAT_GREATERTHANEQUALS_FUNCTION+"("+leftHandSide +", "+rightHandSide+")";
	}

	@Override
	public String doDateRepeatGetYearsTransform(String intervalStr) {
		return DATEREPEAT_YEAR_PART_FUNCTION+"("+intervalStr +")";
	}
	@Override
	public String doDateRepeatGetMonthsTransform(String intervalStr) {
		return DATEREPEAT_MONTH_PART_FUNCTION+"("+intervalStr +")";
	}

	@Override
	public String doDateRepeatGetDaysTransform(String intervalStr) {
		return DATEREPEAT_DAY_PART_FUNCTION+"("+intervalStr +")";
	}

	@Override
	public String doDateRepeatGetHoursTransform(String intervalStr) {
		return DATEREPEAT_HOUR_PART_FUNCTION+"("+intervalStr +")";
	}

	@Override
	public String doDateRepeatGetMinutesTransform(String intervalStr) {
		return DATEREPEAT_MINUTE_PART_FUNCTION+"("+intervalStr +")";
	}

	@Override
	public String doDateRepeatGetSecondsTransform(String intervalStr) {
		return DATEREPEAT_SECOND_PART_FUNCTION+"("+intervalStr +")";
	}

	@Override
	public String doDateRepeatGetMillisecondsTransform(String intervalStr) {
		return DATEREPEAT_MILLISECOND_PART_FUNCTION+"("+intervalStr +")";
	}
}
