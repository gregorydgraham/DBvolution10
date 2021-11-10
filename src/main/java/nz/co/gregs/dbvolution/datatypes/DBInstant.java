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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.InstantColumn;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.InstantExpression;
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
import nz.co.gregs.dbvolution.results.InstantResult;
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
public class DBInstant extends QueryableDatatype<Instant> implements InstantResult {

	private static final long serialVersionUID = 1L;
//	private final SimpleDateFormat toStringFormat = new SimpleDateFormat("yyyy-MM-dd KK:mm:ss.SSSa ZZZZ");
//	private final DateTimeFormatter toStringFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd kk:mm:ss.SSS ZZZZ");

	/**
	 * The default constructor for DBDate.
	 *
	 * <p>
	 * Creates an unset undefined DBDate object.
	 *
	 */
	public DBInstant() {
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
	public DBInstant(Instant date) {
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
	public DBInstant(InstantExpression dateExpression) {
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
	DBInstant(Timestamp timestamp) {
		super(timestamp == null ? null : timestamp.toInstant());
		if (timestamp == null) {
			this.setToNull();
		} else {
			setLiteralValue(timestamp.toInstant());
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
	 * The string is parsed using {@link Instant#parse(java.lang.CharSequence) }
	 * so please ensure your string matches the requirements of that method.
	 *
	 *
	 */
	DBInstant(String dateAsAString) {
		setLiteralValue(Instant.parse(dateAsAString.subSequence(0, dateAsAString.length())));
	}

	/**
	 * Returns the set value of this DBDate as a Java Instant instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java Instant.
	 */
	public Instant instantValue() {
		if (getLiteralValue() instanceof Instant) {
			return getLiteralValue();
		} else {
			return null;
		}
	}

	/**
	 * Returns the set value of this DBDate as a Java LocalDateTime instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java Instant.
	 */
	public LocalDateTime localDateTimeValue() {
		if (getLiteralValue() instanceof Instant) {
			return getLiteralValue().atZone(ZoneId.systemDefault()).toLocalDateTime();
		} else {
			return null;
		}
	}

	/**
	 * Returns the set value of this DBDate as a Java LocalDateTime instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java Instant.
	 */
	public LocalDate localDateValue() {
		if (getLiteralValue() instanceof Instant) {
			return getLiteralValue().atZone(ZoneId.systemDefault()).toLocalDate();
		} else {
			return null;
		}
	}

	/**
	 * Returns the set value of this DBInstant as a Java ZonedDateTime instance.
	 *
	 * @param zoneId the Zone ID to represent the instant at.
	 * @return the value as a Java Instant.
	 */
	public ZonedDateTime zonedDateTimeValue(ZoneId zoneId) {
		if (getLiteralValue() instanceof Instant) {
			return getLiteralValue().atZone(zoneId);
		} else {
			return null;
		}
	}

	/**
	 * Returns the set value of this DBDate as a Java ZonedDateTime instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value as a Java Instant.
	 */
	public ZonedDateTime utcDateTimeValue() {
		if (getLiteralValue() instanceof Instant) {
			return getLiteralValue().atZone(ZoneOffset.UTC);
		} else {
			return null;
		}
	}

	void setValue(DBInstant newLiteralValue) {
		setValue(newLiteralValue.getLiteralValue());
	}

	/**
	 * Sets the value of this QDT to the Java Instant provided.
	 *
	 * @param date	date
	 */
	@Override
	public void setValue(Instant date) {
		super.setLiteralValue(date);
	}

	/**
	 * Sets the value of this QDT to the Java Instant provided.
	 *
	 * <p>
	 * The local date and time is moved to UTC before storing.</p>
	 *
	 * @param date	date
	 */
	public void setValue(GregorianCalendar date) {
		super.setLiteralValue(date.toZonedDateTime().withZoneSameLocal(ZoneOffset.UTC).toInstant());
	}

	/**
	 * Sets the value of this QDT to the Java Instant provided.
	 *
	 * <p>
	 * The local date and time is moved to UTC before storing.</p>
	 *
	 * @param date	date
	 */
	public void setValue(Date date) {
		super.setLiteralValue(date.toInstant());
	}

	/**
	 * Sets the value of this QDT to the Java LocalDateTime provided.
	 *
	 * <p>
	 * The local date and time is moved to UTC before storing.</p>
	 *
	 * @param date	date
	 */
	public void setValue(LocalDateTime date) {
		super.setLiteralValue(asInstant(date));
	}

	/**
	 * Sets the value of this QDT to the Java Instant provided.
	 *
	 * <p>
	 * The zoned date and time is stored as the equivalent UTC instant before
	 * storing.</p>
	 *
	 * @param date	date
	 */
	public void setValue(ZonedDateTime date) {
		super.setLiteralValue(date.toInstant());
	}

	/**
	 * Sets the value of this QDT to the date and time now.
	 *
	 */
	public void setValueToNow() {
		super.setValue(Instant.now());
	}

	/**
	 * Sets the value of this QDT to the dateStr provided.
	 *
	 * <p>
	 * The date String will be parsed by {@link Instant#parse(java.lang.CharSequence)
	 * }. so please confirms to the requirements of that method.
	 *
	 * @param dateStr	dateStr
	 */
	public void setValue(String dateStr) {
		setValue(Instant.parse(dateStr.subSequence(0, dateStr.length())));
	}

	@Override
	public String getSQLDatatype() {
		return "TIMESTAMP WITH TIME ZONE";
	}

	/**
	 * Returns the string value of the DBInstant.
	 *
	 * @return a string version of the current value of this DBDate
	 */
	@Override
	public String toString() {
		if (this.isNull() || getValue() == null) {
			return "<NULL>";
		}
		return DateTimeFormatter.ISO_INSTANT.format(getValue().atZone(ZoneId.of("Z")));
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		return db.getInstantFormattedForQuery(getValue());
	}

	@Override
	protected Instant getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		Instant dbValue;
		if (defn.prefersInstantsReadAsStrings()) {
			dbValue = setByGetString(defn, resultSet, fullColumnName);
		} else {
			dbValue = setByGetTimestamp(defn, resultSet, fullColumnName);
		}
		return dbValue;
	}

	private Instant setByGetString(DBDefinition database, ResultSet resultSet, String fullColumnName) {
		String string = null;
		try {
			string = resultSet.getString(fullColumnName);
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to get Instant from String:" + sqlex.getLocalizedMessage(), sqlex);
		}
		if (string == null || string.isEmpty()) {
			return null;
		} else {
			return database.parseInstantFromGetString(string);
		}
	}

	private Instant setByGetTimestamp(DBDefinition defn, ResultSet resultSet, String fullColumnName) {
		Instant dbValue = null;
		try {
			final Timestamp timestamp = resultSet.getTimestamp(fullColumnName);
			if (resultSet.wasNull()) {
				dbValue = null;
			} else {
				Instant utcVersion;
				if (defn.supportsTimeZones()) {
					utcVersion = timestamp.toInstant();
				} else {
					utcVersion = timestamp.toLocalDateTime().atZone(ZoneOffset.UTC).toInstant();
				}
				dbValue = utcVersion;
			}
		} catch (SQLException sqlex) {
			throw new DBRuntimeException("Unable to set Instant by getting Instant: " + sqlex.getLocalizedMessage(), sqlex);
		}
		return dbValue;
	}

	@Override
	public DBInstant copy() {
		return (DBInstant) super.copy(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Instant getValue() {
		return instantValue();
	}

	@Override
	public DBInstant getQueryableDatatypeForExpressionValue() {
		return new DBInstant();
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
	public void permittedValues(Instant... permitted) {
		this.setOperator(new DBPermittedValuesOperator<Instant>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(LocalDateTime... permitted) {
		List<Instant> collect = asInstantList(permitted);
		this.setOperator(new DBPermittedValuesOperator<Instant>(collect));
	}

	private List<Instant> asInstantList(LocalDateTime[] permitted) {
		return Arrays.asList(permitted).stream().map((t) -> asInstant(t)).collect(Collectors.toList());
	}

	private static Instant asInstant(LocalDateTime t) {
		return t.toInstant(ZoneOffset.UTC);
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(Instant... excluded) {
		this.setOperator(new DBPermittedValuesOperator<Instant>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(LocalDateTime... excluded) {
		List<Instant> list = asInstantList(excluded);
		this.setOperator(new DBPermittedValuesOperator<Instant>(list));
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
	public void permittedRange(Instant lowerBound, Instant upperBound) {
		setOperator(new DBPermittedRangeOperator<Instant>(lowerBound, upperBound));
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
		setOperator(new DBPermittedRangeOperator<Instant>(asInstant(lowerBound), asInstant(upperBound)));
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
	public void permittedRangeInclusive(Instant lowerBound, Instant upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
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
		setOperator(new DBPermittedRangeInclusiveOperator(asInstant(lowerBound), asInstant(upperBound)));
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
	public void permittedRangeExclusive(Instant lowerBound, Instant upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
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
		setOperator(new DBPermittedRangeExclusiveOperator(asInstant(lowerBound), asInstant(upperBound)));
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
	public void excludedRange(Instant lowerBound, Instant upperBound) {
		setOperator(new DBPermittedRangeOperator<Instant>(lowerBound, upperBound));
		negateOperator();
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
		setOperator(new DBPermittedRangeOperator<Instant>(asInstant(lowerBound), asInstant(upperBound)));
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
	public void excludedRangeInclusive(Instant lowerBound, Instant upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
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
		setOperator(new DBPermittedRangeInclusiveOperator(asInstant(lowerBound), asInstant(upperBound)));
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
	public void excludedRangeExclusive(Instant lowerBound, Instant upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
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
		setOperator(new DBPermittedRangeExclusiveOperator(asInstant(lowerBound), asInstant(upperBound)));
		negateOperator();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(InstantExpression... permitted) {
		this.setOperator(new DBPermittedValuesOperator<InstantExpression>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(InstantExpression... excluded) {
		this.setOperator(new DBPermittedValuesOperator<InstantExpression>(excluded));
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
	public void permittedRange(InstantExpression lowerBound, InstantExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<InstantExpression>(lowerBound, upperBound));
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
	public void permittedRangeInclusive(InstantExpression lowerBound, InstantExpression upperBound) {
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
	public void permittedRangeExclusive(InstantExpression lowerBound, InstantExpression upperBound) {
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
	public void excludedRange(InstantExpression lowerBound, InstantExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<InstantExpression>(lowerBound, upperBound));
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
	public void excludedRangeInclusive(InstantExpression lowerBound, InstantExpression upperBound) {
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
	public void excludedRangeExclusive(InstantExpression lowerBound, InstantExpression upperBound) {
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
		throw new UnsupportedOperationException("DBInstant Does Not Have An Accepted Standard String"); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public InstantColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new InstantColumn(row, this);
	}

	@Override
	public StringExpression stringResult() {
		return new InstantExpression(this).stringResult();
	}

	public void excludeNotNull() {
		this.permittedValues((Instant) null);
	}

	public void excludeNull() {
		this.excludedValues((Instant) null);
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
	public synchronized DBInstant setDefaultInsertValue(Instant value) {
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
	 * In particular, setDefaultInsertValue(new Instant()) is probably NOT what
	 * you want, setDefaultInsertValue(DateExpression.currentDate()) will produce
	 * a correct creation date value.</p>
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
	public synchronized DBInstant setDefaultInsertValue(InstantResult value) {
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
	 * In particular, setDefaultUpdateValue(new Instant()) is probably NOT what
	 * you want, setDefaultUpdateValue(DateExpression.currentDate()) will produce
	 * a correct update time value.</p>
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
	public synchronized DBInstant setDefaultUpdateValue(Instant value) {
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
	public synchronized DBInstant setDefaultUpdateValue(InstantResult value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	public void permitOnlyPastAndPresent() {
		this.setOperator(new DBLessThanOrEqualOperator(InstantExpression.now()));
	}

	public void permitOnlyPresentAndFuture() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(InstantExpression.now()));
	}

	public void permitOnlyPast() {
		this.setOperator(new DBLessThanOperator(InstantExpression.currentDate()));
	}

	public void permitOnlyFuture() {
		this.setOperator(new DBGreaterThanOperator(InstantExpression.currentDate()));
	}

	public void permitOnlyPastAndPresentByDateOnly() {
		this.setOperator(new DBLessThanOrEqualOperator(InstantExpression.currentDateOnly()));
	}

	public void permitOnlyPresentAndFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOrEqualsOperator(InstantExpression.currentDateOnly()));
	}

	public void permitOnlyPastByDateOnly() {
		this.setOperator(new DBLessThanOperator(InstantExpression.currentDateOnly()));
	}

	public void permitOnlyFutureByDateOnly() {
		this.setOperator(new DBGreaterThanOperator(InstantExpression.currentDateOnly()));
	}

	public DBInstant setDefaultInsertValueToNow() {
		super.setDefaultInsertValue(Instant.now());
		return this;
	}

	public DBInstant setDefaultUpdateValueToNow() {
		super.setDefaultUpdateValue(Instant.now());
		return this;
	}

	@Override
	public Comparator<Instant> getComparator() {
		return ComparableComparator.forClass(Instant.class);
	}
}
