/*
 * Copyright 2014 Gregory Graham.
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
import java.util.Date;
import nz.co.gregs.dbvolution.databases.NuoDB;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Defines the features of the NuoDB database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link NuoDB} instances, and
 * you should not need to use it directly.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class NuoDBDefinition extends DBDefinition {

	private final DateFormat DATETIME_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");

	@Override
	@SuppressWarnings("deprecation")
	public String getDateFormattedForQuery(Date date) {

		return " DATE_FROM_STR('" + DATETIME_FORMAT.format(date) + "', 'dd/MM/yyyy HH:mm:ss.SSS') ";

	}

	@Override
	protected String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBBoolean) {
			return " boolean ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDate) {
			return " TIMESTAMP(0) ";
		} else if (qdt instanceof DBJavaObject) {
			return " BLOB ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public String doTruncTransform(String firstString, String secondString) {
		//A1-MOD(A1,1*(A1/ABS(A1)))
//		return ""+firstString+"-MOD("+firstString+",1*("+firstString+"/ABS("+firstString+")))";
		return "(((CAST(((" + firstString + ")>0) AS INTEGER))-0.5)*2)*floor(abs(" + firstString + "))";
	}

	@Override
	public boolean supportsExpFunction() {
		return false;
	}

	@Override
	public String getCurrentUserFunctionName() {
		return "USER()";
	}

	@Override
	public boolean supportsStandardDeviationFunction() {
		return false;
	}

	/**
	 * NuoDB follows the standard, unlike anyone else, and pads the short string
	 * with spaces before comparing.
	 *
	 * <p>
	 * This effectively means strings are trimmed during comparisons whether you
	 * like it or not.
	 *
	 * <p>
	 * While this seems useful, in fact it prevents checking for incorrect strings
	 * and breaks the industrial standard.
	 *
	 * @param firstSQLExpression firstSQLExpression
	 * @param secondSQLExpression secondSQLExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return "(" + firstSQLExpression + "||'@') = (" + secondSQLExpression + "||'@')";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATE_ADD(" + dateValue + ", INTERVAL ((" + numberOfHours + ")*60*60) SECOND )";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "DATE_ADD(" + dateValue + ", INTERVAL ((" + numberOfMinutes + ")*60) SECOND )";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return getCurrentDateOnlyFunctionName().trim();
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND(CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND((CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP))/30.43)";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND((CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP))/365.25)";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND((CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP))*24)";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND((CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP))*24*60)";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND((CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP))*24*60*60)";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " DAYOFWEEK(" + dateSQL + ")";
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

	@Override
	public String doNumberEqualsTransform(String leftHandSide, String rightHandSide) {
		return "((" + super.doNumberEqualsTransform(leftHandSide, rightHandSide) + ")=true)";
	}

	@Override
	public boolean supportsRecursiveQueriesNatively() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}
}
