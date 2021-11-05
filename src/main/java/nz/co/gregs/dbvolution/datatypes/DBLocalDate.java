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

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.LocalDateColumn;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.LocalDateExpression;
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
import nz.co.gregs.dbvolution.results.LocalDateResult;
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
public class DBLocalDate extends QueryableDatatype<LocalDate> implements LocalDateResult {

	private static final long serialVersionUID = 1L;
	private final DateTimeFormatter toStringFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	/**
	 * The default constructor for DBDate.
	 *
	 * <p>
	 * Creates an unset undefined DBDate object.
	 *
	 */
	public DBLocalDate() {
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
	public DBLocalDate(LocalDate date) {
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
	public DBLocalDate(LocalDateExpression dateExpression) {
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
	DBLocalDate(Timestamp timestamp) {
		super(timestamp == null ? null : timestamp.toLocalDateTime().toLocalDate());
		if (timestamp == null) {
			this.setToNull();
		} else {
			setLiteralValue(timestamp.toLocalDateTime().toLocalDate());
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
	 * The string is parsed using {@link LocalDate#parse(java.lang.String) } so
	 * please ensure your string matches the requirements of that method.
	 *
	 *
	 */
	@SuppressWarnings("deprecation")
	DBLocalDate(String dateAsAString) {
		final LocalDate dateValue = LocalDate.parse(dateAsAString);
		setLiteralValue(dateValue);
	}

	/**
	 * Returns the set value of this DBDate as a Java LocalDate instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java LocalDate.
	 */
	public LocalDate localDateValue() {
		if (getLiteralValue() instanceof LocalDate) {
			return getLiteralValue();
		} else {
			return null;
		}
	}

	void setValue(DBLocalDate newLiteralValue) {
		setValue(newLiteralValue.getLiteralValue());
	}

	/**
	 * Sets the value of this QDT to the Java LocalDate provided.
	 *
	 * @param date	date
	 */
	@Override
	public void setValue(LocalDate date) {
		super.setLiteralValue(date);
	}

	/**
	 * Sets the value of this QDT to the date and time now.
	 *
	 */
	public void setValueToNow() {
		super.setValue(LocalDate.now());
	}

	/**
	 * Sets the value of this QDT to the dateStr provided.
	 *
	 * <p>
	 * The date String will be parsed by {@link LocalDate#parse(java.lang.CharSequence)
	 * }
	 * so please confirms to the requirements of that method.
	 *
	 * @param dateStr	dateStr
	 */
	@SuppressWarnings("deprecation")
	public void setValue(String dateStr) {
		final LocalDate date = LocalDate.parse(dateStr);
		setValue(date);
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
		if (this.isNull() || localDateValue() == null) {
			return "<NULL>";
		}
		return DateTimeFormatter.ISO_LOCAL_DATE.format(localDateValue());
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		return db.getLocalDateFormattedForQuery(localDateValue());
	}

	@Override
	protected LocalDate getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		LocalDate dbValue;
		if (defn.prefersDatesReadAsStrings()) {
			dbValue = setByGetString(defn, resultSet, fullColumnName);
		} else {
			dbValue = setByGetDate(defn, resultSet, fullColumnName);
		}
		return dbValue;
	}

	private LocalDate setByGetString(DBDefinition database, ResultSet resultSet, String fullColumnName) {
		String string = null;
		try {
			string = resultSet.getString(fullColumnName);
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to get LocalDate from String:" + sqlex.getLocalizedMessage(), sqlex);
		}
		if (string == null || string.isEmpty()) {
			return null;
		} else {
			return database.parseLocalDateFromGetString(string);
		}
	}

	private LocalDate setByGetDate(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		LocalDate dbValue = null;
		try {
			final Date date = resultSet.getDate(fullColumnName);
			if (resultSet.wasNull()) {
				dbValue = null;
			} else {
				LocalDate dateValue = date.toLocalDate();
				// Some drivers interpret getDate as meaning return only the date without the time
				// so we should check both the date and the timestamp find the latest time.
				final Timestamp timestamp = resultSet.getTimestamp(fullColumnName);
				LocalDate timestampValue = timestamp.toLocalDateTime().toLocalDate();
				if (timestampValue.isAfter(dateValue)) {
					dbValue = timestampValue;
				} else {
					dbValue = dateValue;
				}
			}
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to set LocalDate by getting LocalDate: " + sqlex.getLocalizedMessage(), sqlex);
		}
		return dbValue;
	}

	@Override
	public DBLocalDate copy() {
		return (DBLocalDate) super.copy();
	}

	@Override
	public LocalDate getValue() {
		return localDateValue();
	}

	@Override
	public DBLocalDate getQueryableDatatypeForExpressionValue() {
		return new DBLocalDate();
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
	public void permittedValues(LocalDate... permitted) {
		this.setOperator(new DBPermittedValuesOperator<LocalDate>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(LocalDate... excluded) {
		this.setOperator(new DBPermittedValuesOperator<LocalDate>(excluded));
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
	public void permittedRange(LocalDate lowerBound, LocalDate upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDate>(lowerBound, upperBound));
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
	public void permittedRangeInclusive(LocalDate lowerBound, LocalDate upperBound) {
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
	public void permittedRangeExclusive(LocalDate lowerBound, LocalDate upperBound) {
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
	public void excludedRange(LocalDate lowerBound, LocalDate upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDate>(lowerBound, upperBound));
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
	public void excludedRangeInclusive(LocalDate lowerBound, LocalDate upperBound) {
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
	public void excludedRangeExclusive(LocalDate lowerBound, LocalDate upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(LocalDateExpression... permitted) {
		this.setOperator(new DBPermittedValuesOperator<LocalDateExpression>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(LocalDateExpression... excluded) {
		this.setOperator(new DBPermittedValuesOperator<LocalDateExpression>(excluded));
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
	public void permittedRange(LocalDateExpression lowerBound, LocalDateExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDateExpression>(lowerBound, upperBound));
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
	public void permittedRangeInclusive(LocalDateExpression lowerBound, LocalDateExpression upperBound) {
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
	public void permittedRangeExclusive(LocalDateExpression lowerBound, LocalDateExpression upperBound) {
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
	public void excludedRange(LocalDateExpression lowerBound, LocalDateExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<LocalDateExpression>(lowerBound, upperBound));
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
	public void excludedRangeInclusive(LocalDateExpression lowerBound, LocalDateExpression upperBound) {
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
	public void excludedRangeExclusive(LocalDateExpression lowerBound, LocalDateExpression upperBound) {
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
		return localDateValue() == null;
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("DBLocalDate Does Not Have An Accepted Standard String"); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public LocalDateColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new LocalDateColumn(row, this);
	}

	@Override
	public StringExpression stringResult() {
		return new LocalDateExpression(this).stringResult();
	}

	public void excludeNotNull() {
		this.permittedValues((LocalDate) null);
	}

	public void excludeNull() {
		this.excludedValues((LocalDate) null);
	}

	public void permitOnlyNull() {
		excludeNotNull();
	}

	public void permitOnlyNotNull() {
		excludeNull();
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.time.LocalDate) setValue(...)}, for the QDT.
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
	 * public DBDate creationDate = new DBDate().setDefaultInsertValue(LocalDateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate updateDate = new DBDate().setDefaultUpdateValue(LocalDateExpression.currentDate());
	 *
	 * &#64;DBColumn
	 * public DBDate creationOrUpdateDate = new DBDate()
	 * .setDefaultInsertValue(LocalDateExpression.currentDate())
	 * .setDefaultUpdateValue(LocalDateExpression.currentDate());
	 * </pre>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBLocalDate setDefaultInsertValue(LocalDate value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBLocalDate) setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultInsertValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultInsertValue(new LocalDate()) is probably NOT what
	 * you want, setDefaultInsertValue(LocalDateExpression.currentLocalDate())
	 * will produce a correct creation date value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBLocalDate creationDate = new DBLocalDate().setDefaultInsertValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate updateDate = new DBLocalDate().setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate creationOrUpdateDate = new DBLocalDate()
	 * .setDefaultInsertValue(LocalDateExpression.currentLocalDate())
	 * .setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 * </pre>
	 *
	 * @return This QDT
	 */
	public synchronized DBLocalDate setDefaultInsertValueToCurrentLocalDate() {
		super.setDefaultInsertValue(LocalDateExpression.currentLocalDate());
		return this;
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBLocalDate) setValue(...)},
	 * for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultInsertValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultInsertValue(new LocalDate()) is probably NOT what
	 * you want, setDefaultInsertValue(LocalDateExpression.currentLocalDate())
	 * will produce a correct creation date value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBLocalDate creationDate = new DBLocalDate().setDefaultInsertValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate updateDate = new DBLocalDate().setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate creationOrUpdateDate = new DBLocalDate()
	 * .setDefaultInsertValue(LocalDateExpression.currentLocalDate())
	 * .setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 * </pre>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBLocalDate setDefaultInsertValue(LocalDateResult value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.time.LocalDate)  setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultUpdateValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultUpdateValue(new LocalDate()) is probably NOT what
	 * you want, setDefaultUpdateValue(LocalDateExpression.currentDate()) will
	 * produce a correct update time value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBLocalDate creationDate = new DBLocalDate().setDefaultInsertValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate updateDate = new DBLocalDate().setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate creationOrUpdateDate = new DBLocalDate()
	 * .setDefaultInsertValue(LocalDateExpression.currentLocalDate())
	 * .setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 * </pre>
	 *
	 * @return This QDT
	 */
	public synchronized DBLocalDate setDefaultUpdateValueToCurrentLocalDate() {
		super.setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.time.LocalDate)  setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultUpdateValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultUpdateValue(new LocalDate()) is probably NOT what
	 * you want, setDefaultUpdateValue(LocalDateExpression.currentDate()) will
	 * produce a correct update time value.</p>
	 *
	 * <p>
	 * Correct usages for standard date defaults:
	 *
	 * <pre>
	 * &#64;DBColumn
	 * public DBLocalDate creationDate = new DBLocalDate().setDefaultInsertValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate updateDate = new DBLocalDate().setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate creationOrUpdateDate = new DBLocalDate()
	 * .setDefaultInsertValue(LocalDateExpression.currentLocalDate())
	 * .setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 * </pre>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBLocalDate setDefaultUpdateValue(LocalDate value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.time.LocalDate) setValue(...)}, for the QDT.
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
	 * public DBLocalDate creationDate = new DBLocalDate().setDefaultInsertValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate updateDate = new DBLocalDate().setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 *
	 * &#64;DBColumn
	 * public DBLocalDate creationOrUpdateDate = new DBLocalDate()
	 * .setDefaultInsertValue(LocalDateExpression.currentLocalDate())
	 * .setDefaultUpdateValue(LocalDateExpression.currentLocalDate());
	 * </pre>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBLocalDate setDefaultUpdateValue(LocalDateResult value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	public void permitOnlyPastAndPresent() {
		this.setOperator(new DBLessThanOrEqualOperator(LocalDateExpression.currentDate()));
	}

	public void permitOnlyPresentAndFuture() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(LocalDateExpression.currentDate()));
	}

	public void permitOnlyPast() {
		this.setOperator(new DBLessThanOperator(LocalDateExpression.currentDate()));
	}

	public void permitOnlyFuture() {
		this.setOperator(new DBGreaterThanOperator(LocalDateExpression.currentDate()));
	}

	public void permitOnlyPastAndPresentByDateOnly() {
		this.setOperator(new DBLessThanOrEqualOperator(LocalDateExpression.currentDate()));
	}

	public void permitOnlyPresentAndFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(LocalDateExpression.currentLocalDate()));
	}

	public void permitOnlyPastByDateOnly() {
		this.setOperator(new DBLessThanOperator(LocalDateExpression.now()));
	}

	public void permitOnlyFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOperator(LocalDateExpression.today()));
	}

	@Override
	public Comparator<LocalDate> getComparator() {
		return ComparableComparator.forClass(LocalDate.class);
	}
}
