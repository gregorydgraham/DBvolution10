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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.LocalDateTimeColumn;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.DateExpression;
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
	private final DateTimeFormatter toStringFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss.SSS ZZZZ");

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
	 * Creates a DBDate with the value provided.
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
	DBLocalDateTime(String dateAsAString) {
//		System.out.println("DATEASSTRING: "+dateAsAString);
//		System.out.println("ISO_DATE FORMAT: 2011-12-03T10:15:30");
		setLiteralValue(LocalDateTime.parse(dateAsAString, DateTimeFormatter.ISO_DATE_TIME));
	}

	/**
	 * Returns the set value of this DBDate as a Java Date instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java Date.
	 */
	public LocalDateTime localDateTimeValue() {
		if (getLiteralValue() instanceof LocalDateTime) {
			return getLiteralValue();
		} else {
			return null;
		}
	}

	void setValue(DBLocalDateTime newLiteralValue) {
		setValue(newLiteralValue.getLiteralValue());
	}

	/**
	 * Sets the value of this QDT to the Java Date provided.
	 *
	 * @param date	date
	 */
	@Override
	public void setValue(LocalDateTime date) {
		super.setLiteralValue(date);
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
	 * The date String will be parsed by {@link Date#parse(java.lang.String) }
	 * so please confirms to the requirements of that method.
	 *
	 * @param dateStr	dateStr
	 */
	@SuppressWarnings("deprecation")
	public void setValue(String dateStr) {
		setValue(dateStr, DateTimeFormatter.ISO_DATE_TIME);
	}

	/**
	 * Sets the value of this QDT to the dateStr provided.
	 *
	 * <p>
	 * The date String will be parsed by {@link Date#parse(java.lang.String) }
	 * so please confirms to the requirements of that method.
	 *
	 * @param dateStr	dateStr
	 * @param format the format to parse the string with
	 */
	@SuppressWarnings("deprecation")
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
			return "";
		}
		return toStringFormat.format(getValue());
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
		return dbValue;
	}

	private LocalDateTime setByGetString(DBDefinition database, ResultSet resultSet, String fullColumnName) {
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
				return database.parseLocalDateTimeFromGetString(string);
			} catch (ParseException ex) {
				throw new DBRuntimeException("Unable To Parse Date: " + string, ex);
			}
		}
	}

	private LocalDateTime setByGetDate(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		LocalDateTime dbValue = null;
		try {
			final java.sql.Date date = resultSet.getDate(fullColumnName);

			if (resultSet.wasNull()) {
				dbValue = null;
			} else {
				final Timestamp timestamp = resultSet.getTimestamp(fullColumnName);
				final LocalDateTime utcVersion = timestamp.toLocalDateTime();
				final LocalDateTime localDateTime = utcVersion;
				LocalDateTime timestampValue = localDateTime;
				dbValue = timestampValue;
			}
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to set Date by getting Date: " + sqlex.getLocalizedMessage(), sqlex);
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
	public void permittedValues(Date... permitted) {
		this.setOperator(new DBPermittedValuesOperator<Date>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
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
	 * @param permitted	permitted
	 */
	public void permittedValues(DateExpression... permitted) {
		this.setOperator(new DBPermittedValuesOperator<DateExpression>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
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
	 * {@link #setValue(java.time.LocalDateTime)  setValue(...)},
	 * for the QDT.
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
	public synchronized DBLocalDateTime setDefaultInsertValue(LocalDateTime value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.time.LocalDateTime)  setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultInsertValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.  In particular, setDefaultInsertValue(new Date()) is probably NOT what you want, setDefaultInsertValue(DateExpression.currentDate()) will produce a correct creation date value.</p>
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
	public synchronized DBLocalDateTime setDefaultInsertValue(LocalDateTimeResult value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.time.LocalDateTime)   setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultUpdateValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.  In particular, setDefaultUpdateValue(new Date()) is probably NOT what you want, setDefaultUpdateValue(DateExpression.currentDate()) will produce a correct update time value.</p>
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
	public synchronized DBLocalDateTime setDefaultUpdateValue(LocalDateTime value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.time.LocalDateTime)   setValue(...)},
	 * for the QDT.
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
	public synchronized DBLocalDateTime setDefaultUpdateValue(LocalDateTimeResult value) {
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
}