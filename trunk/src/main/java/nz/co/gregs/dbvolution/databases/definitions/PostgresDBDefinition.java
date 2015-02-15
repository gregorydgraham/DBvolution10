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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.PostgresDB;
import nz.co.gregs.dbvolution.databases.PostgresDBOverSSL;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBByteArray;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Defines the features of the PostgreSQL database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link PostgresDB} and
 * {@link PostgresDBOverSSL} instances, and you should not need to use it
 * directly.
 *
 * @author Gregory Graham
 */
public class PostgresDBDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");

	private static final String[] reservedWordsArray = new String[]{"LIMIT", "END"};
	private static final List<String> reservedWords = Arrays.asList(reservedWordsArray);


	@Override
	public String getDropDatabase(String databaseName) throws UnsupportedOperationException {
		return "DROP DATABASE IF EXISTS '"+databaseName+"';";
	}

	@Override
	public String getDropTableStart() {
		return super.getDropTableStart() + " IF EXISTS "; //To change body of generated methods, choose Tools | Templates.
	}
	
	@Override
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return primaryKeyColumnName.toLowerCase();
	}
	
	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (!(reservedWords.contains(sqlObjectName.toUpperCase()))) {
			return super.formatNameForDatabase(sqlObjectName);
		} else {
			return formatNameForDatabase("p" + super.formatNameForDatabase(sqlObjectName));
		}
	}
	
	@Override
	public String getDateFormattedForQuery(Date date) {
		return "('" + DATETIME_FORMAT.format(date) + "'::timestamp)";
	}

	@Override
	public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBByteArray) {
			return " BYTEA ";
		} else if (qdt instanceof DBLargeObject) {
			return " BYTEA ";
		} else if (qdt instanceof DBBoolean) {
			return " BOOLEAN ";
		} else  if (qdt instanceof DBBooleanArray) {
			return " BOOL[] ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public Object getOrderByDirectionClause(Boolean sortOrder) {
		if (sortOrder == null) {
			return "";
		} else if (sortOrder) {
			return " ASC NULLS FIRST ";
		} else {
			return " DESC NULLS LAST ";
		}
	}

	@Override
	public String doTruncTransform(String firstString, String secondString) {
		return getTruncFunctionName() + "((" + firstString + ")::numeric, " + secondString + ")";
	}

	@Override
	public String doBitsToIntegerTransform(String columnName) {
		return columnName + "::integer";
	}

	@Override
	public String doIntegerToBitTransform(String columnName) {
		return columnName + "::bit";
	}

	@Override
	public String doBitsValueTransform(boolean[] boolArray) {
		String boolStr = "";
		for (boolean c : boolArray) {
			if (c) {
				boolStr += "1";
			} else {
				boolStr += "0";
			}
		}
		return "B'" + boolStr + "'";
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return "";
	}

	@Override
	protected boolean hasSpecialAutoIncrementType() {
		return true;
	}

	@Override
	protected String getSpecialAutoIncrementType() {
		return " SERIAL ";
	}

	@Override
	public boolean supportsModulusFunction() {
		return false;
	}

	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + "+ (" + numberOfSeconds + ")*INTERVAL '1 SECOND' )";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "(" + dateValue + "+ (" + numberOfMinutes + ")*INTERVAL '1 MINUTE' )";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "(" + dateValue + "+ (" + numberOfDays + ")*INTERVAL '1 DAY' )";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "(" + dateValue + "+ (" + numberOfHours + ")*INTERVAL '1 HOUR')";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 WEEK')";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 MONTH')";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 YEAR')";
	}

	@Override
	public String doBooleanValueTransform(Boolean boolValue) {
		return "" + (boolValue ? 1 : 0) + "::bool";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return super.getCurrentDateOnlyFunctionName(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(DAY from (" + otherDateValue + ")-(" + dateValue + ")))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	private String doAgeTransformation(String dateValue, String otherDateValue) {
		return "age((" + dateValue + "), (" + otherDateValue + "))";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND(EXTRACT(YEAR FROM " + doAgeTransformation(dateValue, otherDateValue) + ") * 12 + EXTRACT(MONTH FROM " + doAgeTransformation(dateValue, otherDateValue) + ")*-1)";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(YEAR FROM " + doAgeTransformation(dateValue, otherDateValue) + ")*-1)";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + ")) / -3600)";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + ")) / -60)";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + "))*-1)";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " (EXTRACT(DOW FROM ("+dateSQL+"))+1)";
	}

	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		StringBuilder str = new StringBuilder();
		str.append("'{");
		String separator = "";
		if (bools.length == 0) {
			return "'{}'";
		} else if (bools.length == 1) {
			return "'{" + bools[0] + "}'";
		} else {
			for (Boolean bool : bools) {
				str.append(separator).append(bool?1:0);
				separator =",";
			}
			str.append("}'");
			return str.toString();
		}
	}
	
	@Override
	public String doGeometryIntersectionTransform(String firstGeometry, String secondGeometry) {
		return "ST_Intersects(" + firstGeometry + ", " + secondGeometry + ")";
	}

}
