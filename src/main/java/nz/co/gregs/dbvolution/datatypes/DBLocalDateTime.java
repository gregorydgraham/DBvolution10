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
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.LocalDateTimeColumn;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.LocalDateTimeExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.operators.DBGreaterThanOperator;
import nz.co.gregs.dbvolution.operators.DBGreaterThanOrEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBLessThanOperator;
import nz.co.gregs.dbvolution.operators.DBLessThanOrEqualOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.LocalDateTimeResult;
import nz.co.gregs.dbvolution.utility.comparators.ComparableComparator;

/**
 * Encapsulates database values that are Dates.
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
public class DBLocalDateTime extends QueryableDatatype<LocalDateTime> implements LocalDateTimeResult {

	private static final long serialVersionUID = 1L;
//	private final SimpleDateFormat toStringFormat = new SimpleDateFormat("yyyy-MM-dd KK:mm:ss.SSSa ZZZZ");
//	private final transient DateTimeFormatter toStringFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss.SSS ZZZZ");

	/**
	 * The default constructor for DBDate.
	 *
	 * <p>
	 * Creates an unset undefined DBDate object.
	 *
	 */
	public DBLocalDateTime() {
		super();
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * @param date	date
	 */
	public DBLocalDateTime(LocalDateTime date) {
		super(date);
	}

	/**
	 * Creates a column expression with a date result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param dateExpression	dateExpression
	 */
	public DBLocalDateTime(LocalDateTimeExpression dateExpression) {
		super(dateExpression);
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 *
	 */
	DBLocalDateTime(Timestamp timestamp) {
		super(timestamp == null ? null : timestamp.toLocalDateTime());
		if (timestamp == null) {
			this.setToNull();
		} else {
			setLiteralValue(timestamp.toLocalDateTime());
		}
	}

	/**
	 * Creates a DBLocalDateTime with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * <p>
	 * The string is parsed using {@link LocalDateTime#parse(java.lang.String) }
	 * so please ensure your string matches the requirements of that method.
	 *
	 *
	 */
	DBLocalDateTime(String dateAsAString) {
		setLiteralValue(LocalDateTime.parse(dateAsAString, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
	}

	/**
	 * Returns the set value of this DBDate as a Java LocalDateTime instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java LocalDateTime.
	 */
	public LocalDateTime localDateTimeValue() {
		if (getLiteralValue() instanceof LocalDateTime) {
			return getLiteralValue();
		} else {
			return null;
		}
	}

	/**
	 * Returns the set value of this DBDate as a Java LocalDate instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java LocalDateTime.
	 */
	public LocalDate localDateValue() {
		if (getLiteralValue() instanceof LocalDateTime) {
			return getLiteralValue().toLocalDate();
		} else {
			return null;
		}
	}

	/**
	 * Returns the set value of this DBDate as a Java LocalDate instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java LocalDateTime.
	 */
	public LocalTime localTimeValue() {
		if (getLiteralValue() instanceof LocalDateTime) {
			return getLiteralValue().toLocalTime();
		} else {
			return null;
		}
	}

	void setValue(DBLocalDateTime newLiteralValue) {
		setValue(newLiteralValue.getLiteralValue());
	}

	/**
	 * Sets the value of this QDT to the Java LocalDateTime provided.
	 *
	 * @param date	date
	 */
	@Override
	public void setValue(LocalDateTime date) {
		super.setLiteralValue(date);
	}

	/**
	 * Sets the value of this QDT to the Java LocalDateTime provided.
	 *
	 * @param date	date
	 */
	public void setValue(Date date) {
		super.setLiteralValue(LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));
	}

	/**
	 * Sets the value of this QDT to the date and time now.
	 *
	 */
	public void setValueToNow() {
		super.setValue(LocalDateTime.now());
	}

	/**
	 * Sets the value of this QDT to the dateStr provided.
	 *
	 * <p>
	 * The date String will be parsed by {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME
	 * }
	 * so please conform to the requirements of that pattern.
	 *
	 * @param dateStr	dateStr
	 */
	public void setValue(String dateStr) {
		setValue(dateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
	}

	/**
	 * Sets the value of this QDT to the dateStr provided.
	 *
	 * <p>
	 * The date String will be parsed by {@link LocalDateTime#parse(java.lang.CharSequence, java.time.format.DateTimeFormatter)
	 * }
	 * so please confirms to the requirements of that method.
	 *
	 * @param dateStr	dateStr
	 * @param format the format to parse the string with
	 */
	public void setValue(String dateStr, DateTimeFormatter format) {
		setValue(LocalDateTime.parse(dateStr, format));
	}

	@Override
	public String getSQLDatatype() {
		return "TIMESTAMP";
	}

	/**
	 * Returns the string value of the DBDate.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a string version of the current value of this DBDate
	 */
	@Override
	public String toString() {
		if (this.isNull() || getValue() == null) {
			return "<NULL>";
		}
		return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(getValue());
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		return db.getLocalDateTimeFormattedForQuery(getValue());
	}

	@Override
	protected LocalDateTime getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		LocalDateTime dbValue;
		if (defn.prefersDatesReadAsStrings()) {
			dbValue = setByGetString(defn, resultSet, fullColumnName);
		} else {
			dbValue = setByGetDate(defn, resultSet, fullColumnName);
		}
		if (dbValue != null && defn.hasLocalDateTimeOffset()) {
			dbValue = dbValue.plusHours(defn.getLocalDateTimeOffsetHours()).plusMinutes(defn.getLocalDateTimeOffsetMinutes());
		}
		return dbValue;
	}

	private LocalDateTime setByGetString(DBDefinition database, ResultSet resultSet, String fullColumnName) {
		String string = null;
		try {
			string = resultSet.getString(fullColumnName);
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to get LocalDateTime from String:" + sqlex.getLocalizedMessage(), sqlex);
		}
		if (string == null || string.isEmpty()) {
			return null;
		} else {
			return database.parseLocalDateTimeFromGetString(string);
		}
	}

	private LocalDateTime setByGetDate(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		LocalDateTime dbValue = null;
		try {
			final Timestamp timestamp = resultSet.getTimestamp(fullColumnName);

			if (resultSet.wasNull()) {
				dbValue = null;
			} else {
				LocalDateTime localDateTime = timestamp.toLocalDateTime();
				dbValue = localDateTime;
			}
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to set LocalDateTime by getting LocalDateTime: " + sqlex.getLocalizedMessage(), sqlex);
		}
		return dbValue;
	}

	@Override
	public DBLocalDateTime copy() {
		return (DBLocalDateTime) super.copy(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public LocalDateTime getValue() {
		return localDateTimeValue();
	}

	@Override
	public DBLocalDateTime getQueryableDatatypeForExpressionValue() {
		return new DBLocalDateTime();
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
	 * @param permitted	permitted
	 */
	public void permittedValues(LocalDateTime... permitted) {
		this.setOperator(new DBPermittedValuesOperator<LocalDateTime>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(LocalDateTime... excluded) {
		this.setOperator(new DBPermittedValuesOperator<LocalDateTime>(excluded));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
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
	public void permittedRange(LocalDateTime lowerBound, LocalDateTime upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDateTime>(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
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
	public void permittedRangeInclusive(LocalDateTime lowerBound, LocalDateTime upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
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
	public void permittedRangeExclusive(LocalDateTime lowerBound, LocalDateTime upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
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
	public void excludedRange(LocalDateTime lowerBound, LocalDateTime upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDateTime>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
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
	public void excludedRangeInclusive(LocalDateTime lowerBound, LocalDateTime upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
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
	public void excludedRangeExclusive(LocalDateTime lowerBound, LocalDateTime upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(LocalDateTimeExpression... permitted) {
		this.setOperator(new DBPermittedValuesOperator<LocalDateTimeExpression>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(LocalDateTimeExpression... excluded) {
		this.setOperator(new DBPermittedValuesOperator<LocalDateTimeExpression>(excluded));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
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
	public void permittedRange(LocalDateTimeExpression lowerBound, LocalDateTimeExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDateTimeExpression>(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
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
	public void permittedRangeInclusive(LocalDateTimeExpression lowerBound, LocalDateTimeExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
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
	public void permittedRangeExclusive(LocalDateTimeExpression lowerBound, LocalDateTimeExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
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
	public void excludedRange(LocalDateTimeExpression lowerBound, LocalDateTimeExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDateTimeExpression>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
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
	public void excludedRangeInclusive(LocalDateTimeExpression lowerBound, LocalDateTimeExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
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
	public void excludedRangeExclusive(LocalDateTimeExpression lowerBound, LocalDateTimeExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Used internally to decide whether the required query needs to include NULL
	 * values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return whether the query expression needs to test for NULL.
	 */
	@Override
	public boolean getIncludesNull() {
		return getValue() == null;
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("DBLocalDateTime Does Not Have An Accepted Standard String"); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public LocalDateTimeColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new LocalDateTimeColumn(row, this);
	}

	@Override
	public StringExpression stringResult() {
		return new LocalDateTimeExpression(this).stringResult();
	}

	public void excludeNotNull() {
		this.permittedValues((LocalDateTime) null);
	}

	public void excludeNull() {
		this.excludedValues((LocalDateTime) null);
	}

	public void permitOnlyNull() {
		excludeNotNull();
	}

	public void permitOnlyNotNull() {
		excludeNull();
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.time.LocalDateTime)  setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(LocalDateTimeExpression.currentDate())
	 * .setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBLocalDateTime setDefaultInsertValue(LocalDateTime value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.time.LocalDateTime)  setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultInsertValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultInsertValue(new LocalDateTime()) is probably NOT
	 * what you want, setDefaultInsertValue(LocalDateTimeExpression.currentDate())
	 * will produce a correct creation date value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(LocalDateTimeExpression.currentDate())
	 * .setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBLocalDateTime setDefaultInsertValue(LocalDateTimeResult value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.time.LocalDateTime)   setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultUpdateValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultUpdateValue(new LocalDateTime()) is probably NOT
	 * what you want, setDefaultUpdateValue(LocalDateTimeExpression.currentDate())
	 * will produce a correct update time value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(LocalDateTimeExpression.currentDate())
	 * .setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBLocalDateTime setDefaultUpdateValue(LocalDateTime value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.time.LocalDateTime)   setValue(...)}, for the QDT.
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
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(LocalDateTimeExpression.currentDate())
	 * .setDefaultUpdateValue(LocalDateTimeExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBLocalDateTime setDefaultUpdateValue(LocalDateTimeResult value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	public void permitOnlyPastAndPresent() {
		this.setOperator(new DBLessThanOrEqualOperator(LocalDateTimeExpression.currentLocalDateTime()));
	}

	public void permitOnlyPresentAndFuture() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(LocalDateTimeExpression.currentLocalDateTime()));
	}

	public void permitOnlyPast() {
		this.setOperator(new DBLessThanOperator(LocalDateTimeExpression.currentLocalDateTime()));
	}

	public void permitOnlyFuture() {
		this.setOperator(new DBGreaterThanOperator(LocalDateTimeExpression.currentLocalDateTime()));
	}

	public void permitOnlyPastAndPresentByDateOnly() {
		this.setOperator(new DBLessThanOrEqualOperator(LocalDateTimeExpression.currentLocalDate()));
	}

	public void permitOnlyPresentAndFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(LocalDateTimeExpression.currentLocalDate()));
	}

	public void permitOnlyPastByDateOnly() {
		this.setOperator(new DBLessThanOperator(LocalDateTimeExpression.currentLocalDate()));
	}

	public void permitOnlyFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOperator(LocalDateTimeExpression.currentLocalDate()));
	}

	public DBLocalDateTime setDefaultInsertValueToNow() {
		setDefaultInsertValue(LocalDateTime.now());
		return this;
	}

	public DBLocalDateTime setDefaultUpdateValueToNow() {
		setDefaultUpdateValue(LocalDateTime.now());
		return this;
	}

	@Override
	public Comparator<LocalDateTime> getComparator() {
		return ComparableComparator.forClass(LocalDateTime.class);
	}
}
