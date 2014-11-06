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
 * @author Gregory Graham
 */
public class NuoDBDefinition extends DBDefinition{

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

	@Override
	@SuppressWarnings("deprecation")
	public String getDateFormattedForQuery(Date date) {

		return " DATE_FROM_STR('" + DATETIME_FORMAT.format(date) + "', 'dd/MM/yyyy HH:mm:ss') ";

	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBBoolean) {
			return " boolean ";
		} else if (qdt instanceof DBDate) {
			return " TIMESTAMP(0) ";
		} else if (qdt instanceof DBJavaObject) {
			return " BLOB ";
		} else {
			return qdt.getSQLDatatype();
		}
	}

	@Override
	public String doTruncTransform(String firstString, String secondString) {
		//A1-MOD(A1,1*(A1/ABS(A1)))
//		return ""+firstString+"-MOD("+firstString+",1*("+firstString+"/ABS("+firstString+")))";
		return "(((CAST((("+firstString+")>0) AS INTEGER))-0.5)*2)*floor(abs("+firstString+"))";
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
	 * God-awful hack to get past a bug in NuoDB LTRIM.
	 * 
	 * <p>
	 * To be removed as soon as NuoDB fixes the bug.
	 * 
	 * @param toSQLString
	 * @return a hack masquerading as SQL.
	 * @deprecated 
	 */
	@Override
	@Deprecated
	public String doLeftTrimTransform(String toSQLString) {
		return " (("+toSQLString+") not like '% ') and LTRIM("+toSQLString+")";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATE_ADD("+dateValue+", INTERVAL (("+numberOfHours+")*60*60) SECOND )";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "DATE_ADD("+dateValue+", INTERVAL (("+numberOfMinutes+")*60) SECOND )";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return getCurrentDateOnlyFunctionName().trim();
	}
	
	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(DAY FROM (CAST("+otherDateValue+" AS TIMESTAMP) - CAST("+dateValue+" AS TIMESTAMP))))"; 
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "("+doDayDifferenceTransform(dateValue, otherDateValue)+"/7)"; 
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "MONTHS_BETWEEN("+otherDateValue+","+dateValue+")"; 
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(MONTHS_BETWEEN("+otherDateValue+","+dateValue+")/12)"; 
	}

	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(HOUR FROM (CAST("+otherDateValue+" AS TIMESTAMP) - CAST("+dateValue+" AS TIMESTAMP)))"+
				"+("+doDayDifferenceTransform(dateValue, otherDateValue)+"*24))"; 
	}

	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(MINUTE FROM (CAST("+otherDateValue+" AS TIMESTAMP) - CAST("+dateValue+" AS TIMESTAMP)))"+
				"+("+doHourDifferenceTransform(dateValue, otherDateValue)+"*60))";
	}

	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(SECOND FROM (CAST("+otherDateValue+" AS TIMESTAMP) - CAST("+dateValue+" AS TIMESTAMP)))"+
				"+("+doMinuteDifferenceTransform(dateValue, otherDateValue)+"*60))";
	}


	
}
