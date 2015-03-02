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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.internal.datatypes.DateRepeatImpl;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.joda.time.Period;

/**
 * Defines the features of the SQLite database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link SQLiteDB} instances,
 * and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class SQLiteDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static String DATEREPEAT_CREATION_FUNCTION = "DBV_DATEREPEAT_CREATE";
	public static String DATEREPEAT_EQUALS_FUNCTION = "DBV_DATEREPEAT_EQUALS";
	public static String DATEREPEAT_LESSTHAN_FUNCTION = "DBV_DATEREPEAT_LESSTHAN";
	public static String DATEREPEAT_LESSTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_LESSTHANEQUALS";
	public static String DATEREPEAT_GREATERTHAN_FUNCTION = "DBV_DATEREPEAT_GREATERTHAN";
	public static String DATEREPEAT_GREATERTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_GREATERTHANEQUALS";
	public static String DATEREPEAT_DATEADDITION_FUNCTION = "DBV_DATEREPEAT_DATEADD";
	public static String DATEREPEAT_DATESUBTRACTION_FUNCTION = "DBV_DATEREPEAT_DATEMINUS";
	
	public static String DATEREPEAT_YEAR_PART_FUNCTION = "DBV_DATEREPEAT_YEAR_PART";
	public static String DATEREPEAT_MONTH_PART_FUNCTION = "DBV_DATEREPEAT_MONTH_PART";
	public static String DATEREPEAT_DAY_PART_FUNCTION = "DBV_DATEREPEAT_DAY_PART";
	public static String DATEREPEAT_HOUR_PART_FUNCTION = "DBV_DATEREPEAT_HOUR_PART";
	public static String DATEREPEAT_MINUTE_PART_FUNCTION = "DBV_DATEREPEAT_MINUTE_PART";
	public static String DATEREPEAT_SECOND_PART_FUNCTION = "DBV_DATEREPEAT_SECOND_PART";
	public static String DATEREPEAT_MILLISECOND_PART_FUNCTION = "DBV_DATEREPEAT_MILLI_PART";

	@Override
	public String getDateFormattedForQuery(Date date) {
		return " DATETIME('" + DATETIME_FORMAT.format(date) + "') ";
	}

	@Override
	public boolean supportsGeneratedKeys() {
		return false;
	}

	@Override
	public String formatTableName(DBRow table) {
		return super.formatTableName(table).toUpperCase();
	}

	@Override
	public String getDropTableStart() {
		return super.getDropTableStart() + " IF EXISTS "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean prefersTrailingPrimaryKeyDefinition() {
		return false;
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return " PRIMARY KEY AUTOINCREMENT ";
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBLargeObject) {
			return " TEXT ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDate) {
			return " DATETIME ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public void sanityCheckDBTableField(DBTableField dbTableField) {
		if (dbTableField.isPrimaryKey && dbTableField.columnType.equals(DBInteger.class)) {
			dbTableField.isAutoIncrement = true;
		}
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream() {
		return true;
	}

	@Override
	public boolean prefersLargeObjectsSetAsBase64String() {
		return true;
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ ","
				+ length
				+ ") ";
	}

	@Override
	protected String getCurrentDateOnlyFunctionName() {
		return "DATETIME";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return " DATETIME('now','localtime') ";
	}

	@Override
	protected String getCurrentDateTimeFunction() {
		return " DATETIME('now','localtime') ";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String getTruncFunctionName() {
		// TRUNC is defined in SQLiteDB as a user defined function.
		return "TRUNC";
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "LOCATION_OF(" + originalString + ", " + stringToFind + ")";
	}

	@Override
	public String getCurrentUserFunctionName() {
		return "CURRENT_USER()";
	}

	@Override
	public String getStandardDeviationFunctionName() {
		return "STDEV";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return " (CAST(strftime('%m', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return " (CAST(strftime('%Y', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return " (CAST(strftime('%d', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return " (CAST(strftime('%H', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return " (CAST(strftime('%M', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return " (CAST(strftime('%S', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String getGreatestOfFunctionName() {
		return " MAX "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getLeastOfFunctionName() {
		return " MIN "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return true;
	}

	@Override
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		return DATETIME_FORMAT.parse(getStringDate);
	}

	public String formatDateForGetString(Date date) throws ParseException {
		return DATETIME_FORMAT.format(date);
	}

	@Override
	public boolean supportsRetrievingLastInsertedRowViaSQL() {
		return true;
	}

	@Override
	public String getRetrieveLastInsertedRowSQL() {
		return "select last_insert_rowid();";
	}

	/**
	 * Indicates whether the database supports the modulus function.
	 *
	 * @return the default implementation returns TRUE.
	 */
	@Override
	public boolean supportsModulusFunction() {
		return false;
	}

	@Override
	public String doAddMillisecondsTransform(String dateValue, String numberOfSeconds) {
		return "datetime(" + dateValue + ", (" + numberOfSeconds + ")||' MILLISECOND' )";
	}

	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "datetime(" + dateValue + ", (" + numberOfSeconds + ")||' SECOND')";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "datetime(" + dateValue + ", (" + numberOfMinutes + ")||' minute')";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "datetime(" + dateValue + ", (" + numberOfHours + ")||' hour')";
	}
	
	@Override
	public String doAddDaysTransform(String dayValue, String numberOfDays) {
		return "datetime(" + dayValue + ", (" + numberOfDays + ")||' days')";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "datetime(" + dateValue + ", (7*(" + numberOfWeeks + "))||' days')";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "datetime(" + dateValue + ", (" + numberOfMonths + ")||' month')";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "datetime(" + dateValue + ", (" + numberOfYears + ")||' year')";
	}
	
	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(julianday("+otherDateValue+") - julianday("+dateValue+"))"; 
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "("+doDayDifferenceTransform(dateValue, otherDateValue)+"/7)"; 
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "(strftime('%m',"+otherDateValue+")+12*strftime('%Y',"+otherDateValue+")) - (strftime('%m',"+dateValue+")+12*strftime('%Y',"+dateValue+"))"; 
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(strftime('%Y',"+otherDateValue+")) - (strftime('%Y',"+dateValue+"))"; 
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(cast((strftime('%s',"+otherDateValue+")-strftime('%s',"+dateValue+")) AS real)/60/60)"; 
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(cast((strftime('%s',"+otherDateValue+")-strftime('%s',"+dateValue+")) AS real)/60)"; 
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "cast((strftime('%s',"+otherDateValue+")-strftime('%s',"+dateValue+")) AS real)"; 
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " (cast(STRFTIME('%w', ("+dateSQL+")) AS real)+1)";
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

	@Override
	public String getAlterTableAddForeignKeyStatement(DBRow newTableRow, PropertyWrapper field) {
		if (field.isForeignKey()) {
			return "ALTER TABLE " + this.formatTableName(newTableRow) + " ADD " + field.columnName() + " REFERENCES " + field.referencedTableName() + "(" + field.referencedColumnName() + ") ";
		
		}
		return "";
	}

	@Override
	public String transformPeriodIntoDateRepeat(Period interval) {
		return "'"+DateRepeatImpl.getDateRepeatString(interval)+"'";
	}

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
		return "("+leftHandSide +" = "+rightHandSide+")";
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

	@Override
	public boolean supportsArcSineFunction() {
		return false;
	}
}
