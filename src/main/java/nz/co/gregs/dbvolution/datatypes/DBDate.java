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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.DateColumn;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.operators.DBGreaterThanOperator;
import nz.co.gregs.dbvolution.operators.DBGreaterThanOrEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBLessThanOperator;
import nz.co.gregs.dbvolution.operators.DBLessThanOrEqualOperator;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.utility.comparators.ComparableComparator;

/**
 * Encapsulates database values that are Dates.
 *
 * <p>
 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate}, or
 * {@link DBInstant}.</p>
 *
 * <p>
 * Use DBDate when the column is a date datatype, even in databases where the
 * native date type is a String (i.e. {@link SQLiteDB}).
 *
 * <p>
 * Generally DBDate is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBDate myBoolColumn = new DBDate();}
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBDate extends QueryableDatatype<Date> implements DateResult {

	private static final long serialVersionUID = 1L;
	private final SimpleDateFormat toStringFormat = new SimpleDateFormat("yyyy-MM-dd KK:mm:ss.SSSa ZZZZ");

	/**
	 * The default constructor for DBDate.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * Creates an unset undefined DBDate object.
	 *
	 */
	public DBDate() {
		super();
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * @param date	date
	 */
	public DBDate(Date date) {
		super(date);
	}

	/**
	 * Creates a column expression with a date result from the expression
	 * provided.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param dateExpression	dateExpression
	 */
	public DBDate(DateExpression dateExpression) {
		super(dateExpression);
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 *
	 */
	DBDate(Timestamp timestamp) {
		super(timestamp);
		if (timestamp == null) {
			this.setToNull();
		} else {
			Date date = new Date();
			date.setTime(timestamp.getTime());
			setLiteralValue(date);
		}
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * <p>
	 * The string is parsed using {@link Date#parse(java.lang.String) } so please
	 * ensure your string matches the requirements of that method.
	 *
	 *
	 */
	@SuppressWarnings("deprecation")
	DBDate(String dateAsAString) {
		final long dateLong = Date.parse(dateAsAString);
		Date dateValue = new Date();
		dateValue.setTime(dateLong);
		setLiteralValue(dateValue);
	}

	/**
	 * Returns the set value of this DBDate as a Java Date instance.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @return the value as a Java Date.
	 */
	public Date dateValue() {
		if (getLiteralValue() instanceof Date) {
			return getLiteralValue();
		} else {
			return null;
		}
	}

	void setValue(DBDate newLiteralValue) {
		setValue(newLiteralValue.getLiteralValue());
	}

	/**
	 * Sets the value of this QDT to the Java Date provided.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @param date	date
	 */
	@Override
	public void setValue(Date date) {
		super.setLiteralValue(date);
	}

	/**
	 * Sets the value of this QDT to the date and time now.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 */
	public void setValueToNow() {
		super.setValue(new Date());
	}

	/**
	 * Sets the value of this QDT to the dateStr provided.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The date String will be parsed by {@link Date#parse(java.lang.String) }
	 * so please confirms to the requirements of that method.
	 *
	 * @param dateStr	dateStr
	 */
	@SuppressWarnings("deprecation")
	public void setValue(String dateStr) {
		final long dateLong = Date.parse(dateStr);
		Date date = new Date();
		date.setTime(dateLong);
		setValue(date);
	}

	@Override
	public String getSQLDatatype() {
		return "TIMESTAMP";
	}

	/**
	 * Returns the string value of the DBDate.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @return a string version of the current value of this DBDate
	 */
	@Override
	public String toString() {
		if (this.isNull() || dateValue() == null) {
			return "";
		}
		return toStringFormat.format(dateValue());
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		return db.getDateFormattedForQuery(dateValue());
	}

	@Override
	protected Date getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		Date dbValue;
		if (defn.prefersDatesReadAsStrings()) {
			dbValue = setByGetString(defn, resultSet, fullColumnName);
		} else {
			dbValue = setByGetDate(defn, resultSet, fullColumnName);
		}
		return dbValue;
	}

	private Date setByGetString(DBDefinition database, ResultSet resultSet, String fullColumnName) {
		String string = null;
		try {
			string = resultSet.getString(fullColumnName);
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to get Date from String:" + sqlex.getLocalizedMessage(), sqlex);
		}
		if (string == null || string.isEmpty()) {
			return null;
		} else {
			try {
				return new Date(database.parseDateFromGetString(string).getTime());
			} catch (DateTimeParseException ex) {
				throw new DBRuntimeException("Unable To Parse Date: " + string, ex);
			}
		}
	}

	private Date setByGetDate(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		Date dbValue = null;
		try {
			Date dateValue = resultSet.getDate(fullColumnName);
			if (resultSet.wasNull()) {
				dbValue = null;
			} else {
				// Some drivers interpret getDate as meaning return only the date without the time
				// so we should check both the date and the timestamp find the latest time.
				final long timestamp = resultSet.getTimestamp(fullColumnName).getTime();
				Date timestampValue = new Date(timestamp);
				if (timestampValue.after(dateValue)) {
					dbValue = timestampValue;
				} else {
					dbValue = dateValue;
				}
			}
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to set Date by getting Date: " + sqlex.getLocalizedMessage(), sqlex);
		}
		return dbValue;
	}

	@Override
	public DBDate copy() {
		return (DBDate) super.copy(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Date getValue() {
		return dateValue();
	}

	@Override
	public DBDate getQueryableDatatypeForExpressionValue() {
		return new DBDate();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<>();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Date... permitted) {
		this.setOperator(new DBPermittedValuesOperator<Date>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(Date... excluded) {
		this.setOperator(new DBPermittedValuesOperator<Date>(excluded));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRange(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeOperator<Date>(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeInclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5,...
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1,...
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeExclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e excludedRange(1,3) will return
	 * everything except 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will return ..., -1, 0.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will return 5, 6, 7, 8 etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeOperator<Date>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e excludedRangeInclusive(1,3) will return
	 * ..., -1, 0, 4, 5, ... .
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will return ..., -1, 0.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will return 6, 7, 8, 9,... etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeInclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e excludedRangeExclusive(1,3) will return
	 * ... -1, 0, 1, 3, 4,... but exclude 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will return ..., -1 ,0 ,1 .
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will return 5,6,7,8...
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeExclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(DateExpression... permitted) {
		this.setOperator(new DBPermittedValuesOperator<DateExpression>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(DateExpression... excluded) {
		this.setOperator(new DBPermittedValuesOperator<DateExpression>(excluded));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRange(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<DateExpression>(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeInclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeExclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e excludedRange(1,3) will return
	 * everything except 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will return 5, 6, 7, 8, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<DateExpression>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e excludedRangeInclusive(1,3) will return
	 * everything except 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will return ..., -1, 0.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will return 6, 7, 8, 9, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeInclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e excludedRangeExclusive(1,3) will return
	 * everything except 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will return 5, 6, 7, 8,, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeExclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Used internally to decide whether the required query needs to include NULL
	 * values.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * @return whether the query expression needs to test for NULL.
	 */
	@Override
	public boolean getIncludesNull() {
		return dateValue() == null;
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("DBDate does not support setValueFromStandardStringEncoding(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public DateColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new DateColumn(row, this);
	}

	@Override
	public StringExpression stringResult() {
		return new DateExpression(this).stringResult();
	}

	public void excludeNotNull() {
		this.permittedValues((Date) null);
	}

	public void excludeNull() {
		this.excludedValues((Date) null);
	}

	public void permitOnlyNull() {
		excludeNotNull();
	}

	public void permitOnlyNotNull() {
		excludeNull();
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBDate) setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:</p>
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(DateExpression.currentDate())
	 * .setDefaultUpdateValue(DateExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBDate setDefaultInsertValue(Date value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Sets the value to be inserted when no value has been set to the current
	 * database date.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:</p>
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(DateExpression.currentDate())
	 * .setDefaultUpdateValue(DateExpression.currentDate());
	 * </pre>
	 *
	 * @return This QDT
	 */
	public synchronized DBDate setDefaultInsertValueToCurrentDate() {
		super.setDefaultInsertValue(DateExpression.currentDate());
		return this;
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBDate) setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultInsertValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultInsertValue(new Date()) is probably NOT what you
	 * want, setDefaultInsertValue(DateExpression.currentDate()) will produce a
	 * correct creation date value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(DateExpression.currentDate())
	 * .setDefaultUpdateValue(DateExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBDate setDefaultInsertValue(DateResult value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBDate)  setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultUpdateValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultUpdateValue(new Date()) is probably NOT what you
	 * want, setDefaultUpdateValue(DateExpression.currentDate()) will produce a
	 * correct update time value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(DateExpression.currentDate())
	 * .setDefaultUpdateValue(DateExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBDate setDefaultUpdateValue(Date value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	/**
	 * Sets the value to be used during an update when no value has been set,
	 * using
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBDate)  setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(DateExpression.currentDate())
	 * .setDefaultUpdateValue(DateExpression.currentDate());
	 * </pre>
	 *
	 * @return This QDT
	 */
	public synchronized DBDate setDefaultUpdateValueToCurrentDate() {
		super.setDefaultUpdateValue(DateExpression.currentDate());
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBDate)  setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * DEPRECATED: please change to {@link DBLocalDateTime}, {@link DBLocalDate},
	 * or {@link DBInstant}.</p>
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(DateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(DateExpression.currentDate())
	 * .setDefaultUpdateValue(DateExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBDate setDefaultUpdateValue(DateResult value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	public void permitOnlyPastAndPresent() {
		this.setOperator(new DBLessThanOrEqualOperator(DateExpression.currentDate()));
	}

	public void permitOnlyPresentAndFuture() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(DateExpression.currentDate()));
	}

	public void permitOnlyPast() {
		this.setOperator(new DBLessThanOperator(DateExpression.currentDate()));
	}

	public void permitOnlyFuture() {
		this.setOperator(new DBGreaterThanOperator(DateExpression.currentDate()));
	}

	public void permitOnlyPastAndPresentByDateOnly() {
		this.setOperator(new DBLessThanOrEqualOperator(DateExpression.currentDateOnly()));
	}

	public void permitOnlyPresentAndFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(DateExpression.currentDateOnly()));
	}

	public void permitOnlyPastByDateOnly() {
		this.setOperator(new DBLessThanOperator(DateExpression.currentDateOnly()));
	}

	public void permitOnlyFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOperator(DateExpression.currentDateOnly()));
	}

	@Override
	public Comparator<Date> getComparator() {
		return ComparableComparator.forClass(Date.class);
	}
}
