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
package nz.co.gregs.dbvolution.expressions;

import java.lang.reflect.InvocationTargetException;
import java.time.*;
import java.util.*;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.DateRepeatResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.LocalDateTimeResult;
import org.joda.time.Period;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionRequiresOrderBy;
import nz.co.gregs.separatedstring.SeparatedStringBuilder;

/**
 * LocalDateTimeExpression implements standard functions that produce a
 * LocalDateTime or Time result.
 *
 * <p>
 * LocalDateTime and Time are considered synonymous with timestamp as that
 * appears to be the standard usage by developers. So every date has a time
 * component and every time has a date component. {@link DBLocalDate} implements
 * a time-less date for DBvolution but is considered a DBLocalDateTime with a
 * time of Midnight for LocalDateTimeExpression purposes.
 *
 * <p>
 * Most query requirements are provided by {@link QueryableDatatype}s like
 * {@link DBString} or {@link DBInteger} but expressions can provide more
 * functions or more precise control.
 *
 * <p>
 * Use a LocalDateTimeExpression to produce a date from an existing column,
 * expression or value and perform date arithmetic.
 *
 * <p>
 * Generally you get a LocalDateTimeExpression from a column or value using
 * {@link LocalDateTimeExpression#value(java.util.LocalDateTime) } or
 * {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBLocalDateTime)}.
 *
 * @author Gregory Graham
 */
public class LocalDateTimeExpression extends RangeExpression<LocalDateTime, LocalDateTimeResult, DBLocalDateTime> implements LocalDateTimeResult {

	private final static long serialVersionUID = 1l;

	/**
	 * The integer used to represent the index for Sunday
	 */
	static final public Number SUNDAY = 1;
	/**
	 * The integer used to represent the index for Monday
	 */
	static final public Number MONDAY = 2;
	/**
	 * The integer used to represent the index for Tuesday
	 */
	static final public Number TUESDAY = 3;
	/**
	 * The integer used to represent the index for Wednesday
	 */
	static final public Number WEDNESDAY = 4;
	/**
	 * The integer used to represent the index for Thursday
	 */
	static final public Number THURSDAY = 5;
	/**
	 * The integer used to represent the index for Friday
	 */
	static final public Number FRIDAY = 6;
	/**
	 * The integer used to represent the index for Saturday
	 */
	static final public Number SATURDAY = 7;

	public static LocalDateTimeExpression newLocalDateTime(DateExpression dateExpression) {
		return new LocalDateTimeExpression(dateExpression);
	}

	/**
	 * Default Constructor
	 */
	protected LocalDateTimeExpression() {
		super();
	}

	/**
	 * Create a LocalDateTimeExpression based on an existing
	 * {@link LocalDateTimeResult}.
	 *
	 * <p>
	 * {@link LocalDateTimeResult} is generally a LocalDateTimeExpression but it
	 * may also be a {@link DBLocalDateTime} or {@link DBLocalDate}.
	 *
	 * @param dateVariable a date expression or QueryableDatatype
	 */
	public LocalDateTimeExpression(LocalDateTimeResult dateVariable) {
		super(dateVariable);
	}

	/**
	 * Create a LocalDateTimeExpression based on an existing
	 * {@link LocalDateTimeResult}.
	 *
	 * <p>
	 * {@link LocalDateTimeResult} is generally a LocalDateTimeExpression but it
	 * may also be a {@link DBLocalDateTime} or {@link DBLocalDate}.
	 *
	 * @param variable a date expression or QueryableDatatype
	 */
	protected LocalDateTimeExpression(AnyResult<?> variable) {
		super(variable);
	}

	/**
	 * Create a LocalDateTimeExpression based on an existing LocalDateTime.
	 *
	 * <p>
	 * This performs a similar function to {@link LocalDateTimeExpression#value(java.util.LocalDateTime)
	 * }.
	 *
	 * @param date the date to be used in this expression
	 */
	public LocalDateTimeExpression(LocalDateTime date) {
		super(new DBLocalDateTime(date));
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return getInnerResult().toSQLString(db);
	}

	@Override
	public LocalDateTimeExpression copy() {
		return isNullSafetyTerminator() ? nullLocalDateTime() : new LocalDateTimeExpression((AnyResult<?>) this.getInnerResult().copy());
	}

	@Override
	public final LocalDateTimeExpression nullExpression() {
		return new LocalDateTimeNullExpression();
	}

	/**
	 * Creates a date expression that returns only the date part of current date
	 * on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current day, according to the
	 * database, with the time set to Midnight.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a date expression of only the date part of the current database
	 * timestamp.
	 */
	public static LocalDateTimeExpression currentLocalDate() {
		return new CurrentLocalDateLocalDateTimeExpression();
	}

	/**
	 * Creates a date expression that returns the current date on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current day and time according to
	 * the database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a date expression of the current database timestamp.
	 */
	public static LocalDateTimeExpression currentLocalDateTime() {
		return new LocalDateTimeCurrentLocalDateTimeExpression();
	}

	/**
	 * Creates a date expression that returns the current time on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current time, according to the
	 * database, with the date set to database's zero date.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a date expression of only the time part of the current database
	 * timestamp.
	 */
	public static LocalDateTimeExpression currentTime() {
		return new LocalDateTimeExpression(
				new LocalDateTimeCurrentTimeExpression());
	}

	/**
	 * Creates a date expression that returns the current time on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current time, according to the
	 * database, with the date set to database's zero date.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a date expression of only the time part of the current database
	 * timestamp.
	 */
	public static LocalDateTimeExpression now() {
		return LocalDateTimeExpression.currentLocalDateTime();
	}

	/**
	 * Creates an SQL expression that returns the year part of this date
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the year of this date expression as a number.
	 */
	public IntegerExpression year() {
		return new LocalDateTimeYearExpression(this).integerPart();
	}

	/**
	 * Creates an SQL expression that tests the year part of this date expression.
	 *
	 * @param yearRequired the year to used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the year is the same as the
	 * example supplied.
	 */
	public BooleanExpression yearIs(Number yearRequired) {
		return this.year().is(yearRequired);
	}

	/**
	 * Creates an SQL expression that tests the year part of this date expression.
	 *
	 * @param yearRequired the year to used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the year is the same as the
	 * example supplied.
	 */
	public BooleanExpression yearIs(Long yearRequired) {
		return this.year().is(yearRequired);
	}

	/**
	 * Creates an SQL expression that tests the year part of this date expression.
	 *
	 * @param yearRequired the year to used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the year is the same as the
	 * example supplied.
	 */
	public BooleanExpression yearIs(Integer yearRequired) {
		return this.year().is(yearRequired);
	}

	/**
	 * Creates an SQL expression that tests the year part of this date expression.
	 *
	 * @param yearRequired the year to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the year is the same as the
	 * example supplied.
	 */
	public BooleanExpression yearIs(NumberResult yearRequired) {
		return this.year().is(yearRequired);
	}

	/**
	 * Creates an SQL expression that tests the year part of this date expression.
	 *
	 * @param yearRequired the year to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the year is the same as the
	 * example supplied.
	 */
	public BooleanExpression yearIs(IntegerResult yearRequired) {
		return this.year().is(yearRequired);
	}

	/**
	 * Creates an SQL expression that returns the month part of this date
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the month of this date expression as a number.
	 */
	public IntegerExpression month() {
		return new LocalDateTimeMonthExpression(this).integerPart();
	}

	/**
	 * Creates an SQL expression that tests the month part of this date
	 * expression.
	 *
	 * @param monthRequired the month to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the month is the same as the
	 * example supplied.
	 */
	public BooleanExpression monthIs(Number monthRequired) {
		return this.month().is(monthRequired);
	}

	/**
	 * Creates an SQL expression that tests the month part of this date
	 * expression.
	 *
	 * @param monthRequired the month to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the month is the same as the
	 * example supplied.
	 */
	public BooleanExpression monthIs(Long monthRequired) {
		return this.month().is(monthRequired);
	}

	/**
	 * Creates an SQL expression that tests the month part of this date
	 * expression.
	 *
	 * @param monthRequired the month to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the month is the same as the
	 * example supplied.
	 */
	public BooleanExpression monthIs(Integer monthRequired) {
		return this.month().is(monthRequired);
	}

	/**
	 * Creates an SQL expression that tests the month part of this date
	 * expression.
	 *
	 * @param monthRequired the month to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the month is the same as the
	 * example supplied.
	 */
	public BooleanExpression monthIs(NumberResult monthRequired) {
		return this.month().is(monthRequired);
	}

	/**
	 * Creates an SQL expression that tests the month part of this date
	 * expression.
	 *
	 * @param monthRequired the month to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the month is the same as the
	 * example supplied.
	 */
	public BooleanExpression monthIs(IntegerResult monthRequired) {
		return this.month().is(monthRequired);
	}

	/**
	 * Returns the day part of the date.
	 *
	 * <p>
	 * Day in this sense is the number of the day within the month: that is the 25
	 * part of Monday 25th of August 2014
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression that will provide the day of this date.
	 */
	public IntegerExpression day() {
		return new LocalDateTimeDayExpression(this).integerResult();
	}

	/**
	 * Creates an SQL expression that tests the day part of this date expression.
	 *
	 * @param dayRequired the day to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the day is the same as the
	 * example supplied.
	 */
	public BooleanExpression dayIs(Number dayRequired) {
		return this.day().is(dayRequired);
	}

	/**
	 * Creates an SQL expression that tests the day part of this date expression.
	 *
	 * @param dayRequired the day to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the day is the same as the
	 * example supplied.
	 */
	public BooleanExpression dayIs(Long dayRequired) {
		return this.day().is(dayRequired);
	}

	/**
	 * Creates an SQL expression that tests the day part of this date expression.
	 *
	 * @param dayRequired the day to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the day is the same as the
	 * example supplied.
	 */
	public BooleanExpression dayIs(Integer dayRequired) {
		return this.day().is(dayRequired);
	}

	/**
	 * Creates an SQL expression that tests the day part of this date expression.
	 *
	 * @param dayRequired the day to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the day is the same as the
	 * example supplied.
	 */
	public BooleanExpression dayIs(NumberResult dayRequired) {
		return this.day().is(dayRequired);
	}

	/**
	 * Creates an SQL expression that tests the day part of this date expression.
	 *
	 * @param dayRequired the day to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the day is the same as the
	 * example supplied.
	 */
	public BooleanExpression dayIs(IntegerResult dayRequired) {
		return this.day().is(dayRequired);
	}

	/**
	 * Creates an SQL expression that returns the hour part of this date
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the hour of this date expression as a number.
	 */
	public IntegerExpression hour() {
		return new LocalDateTimeHourExpression(this).integerResult();
	}

	/**
	 * Creates an SQL expression that tests the hour part of this date expression.
	 *
	 * @param hourRequired the hour to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the hour is the same as the
	 * example supplied.
	 */
	public BooleanExpression hourIs(Number hourRequired) {
		return this.hour().is(hourRequired);
	}

	/**
	 * Creates an SQL expression that tests the hour part of this date expression.
	 *
	 * @param hourRequired the hour to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the hour is the same as the
	 * example supplied.
	 */
	public BooleanExpression hourIs(Long hourRequired) {
		return this.hour().is(hourRequired);
	}

	/**
	 * Creates an SQL expression that tests the hour part of this date expression.
	 *
	 * @param hourRequired the hour to be used in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the hour is the same as the
	 * example supplied.
	 */
	public BooleanExpression hourIs(Integer hourRequired) {
		return this.hour().is(hourRequired);
	}

	/**
	 * Creates an SQL expression that tests the hour part of this date expression.
	 *
	 * @param hourRequired the hour to be compared to.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the hour is the same as the
	 * example supplied.
	 */
	public BooleanExpression hourIs(IntegerResult hourRequired) {
		return this.hour().is(hourRequired);
	}

	/**
	 * Creates an SQL expression that tests the hour part of this date expression.
	 *
	 * @param hourRequired the hour to be compared to.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the hour is the same as the
	 * example supplied.
	 */
	public BooleanExpression hourIs(NumberResult hourRequired) {
		return this.hour().is(hourRequired);
	}

	/**
	 * Creates an SQL expression that returns the minute part of this date
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the minute of this date expression as a number.
	 */
	public IntegerExpression minute() {
		return (new LocalDateTimeMinuteExpression(this)).integerResult();
	}

	/**
	 * Creates an SQL expression that tests the minute part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute to be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the minute is the same as the
	 * example supplied.
	 */
	public BooleanExpression minuteIs(Number minuteRequired) {
		return this.minute().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the minute part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute to be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the minute is the same as the
	 * example supplied.
	 */
	public BooleanExpression minuteIs(Long minuteRequired) {
		return this.minute().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the minute part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute to be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the minute is the same as the
	 * example supplied.
	 */
	public BooleanExpression minuteIs(Integer minuteRequired) {
		return this.minute().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the minute part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute to be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the minute is the same as the
	 * example supplied.
	 */
	public BooleanExpression minuteIs(NumberResult minuteRequired) {
		return this.minute().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the minute part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute to be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the minute is the same as the
	 * example supplied.
	 */
	public BooleanExpression minuteIs(IntegerResult minuteRequired) {
		return this.minute().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that returns the second part of this date
	 * expression.
	 *
	 * <p>
	 * Contains only whole seconds, use {@link #subsecond()} to retrieve the
	 * fractional part.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the second of this date expression as a number.
	 */
	public IntegerExpression second() {
		return new LocalDateTimeSecondExpression(this).integerPart();
	}

	/**
	 * Creates an SQL expression that returns the second and subsecond parts of
	 * this date expression.
	 *
	 * <p>
	 * Contains both the integer seconds and fractional seconds, use
	 * {@link #subsecond()} to retrieve the fractional part.
	 *
	 * @return the second of this date expression as a number.
	 */
	public NumberExpression secondAndSubSecond() {
		return new LocalDateTimeSecondAndSubsecondExpression(this);
	}

	/**
	 * Creates an SQL expression that returns the fractions of a second part of
	 * this date expression.
	 *
	 * <p>
	 * Contains only the fractional part of the seconds, that is always between 0
	 * and 1, use {@link #second()} to retrieve the integer part.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the second of this date expression as a number.
	 */
	public NumberExpression subsecond() {
		return new NumberExpression(
				new LocalDateTimeSubsecondExpression(this));
	}

	/**
	 * Creates an SQL expression that tests the second part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the second is the same as the
	 * example supplied.
	 */
	public BooleanExpression secondIs(Number minuteRequired) {
		return this.second().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the second part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the second is the same as the
	 * example supplied.
	 */
	public BooleanExpression secondIs(Long minuteRequired) {
		return this.second().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the second part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the second is the same as the
	 * example supplied.
	 */
	public BooleanExpression secondIs(Integer minuteRequired) {
		return this.second().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the second part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute that the expression must match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the second is the same as the
	 * example supplied.
	 */
	public BooleanExpression secondIs(NumberResult minuteRequired) {
		return this.second().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that tests the second part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute that the expression must match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that is TRUE if the second is the same as the
	 * example supplied.
	 */
	public BooleanExpression secondIs(IntegerResult minuteRequired) {
		return this.second().is(minuteRequired);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * <p>
	 * Be careful when using this expression as dates have lots of fields and it
	 * is easy to miss a similar date.
	 *
	 * @param date the date the expression must match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the date and this
	 * LocalDateTimeExpression.
	 */
	@Override
	public BooleanExpression is(LocalDateTime date) {
		return is(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * @param dateExpression the date the expression must match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the LocalDateTimeResult and this
	 * LocalDateTimeExpression.
	 */
	@Override
	public BooleanExpression is(LocalDateTimeResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new LocalDateTimeIsExpression(this, dateExpression));
		if (isExpr.getIncludesNull()) {
			return BooleanExpression.isNull(this);
		} else {
			return isExpr;
		}
	}

	/**
	 * Creates an SQL expression that test whether this date expression is NOT
	 * equal to the supplied date.
	 *
	 * @param date the date the expression must not match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the LocalDateTimeResult and this
	 * LocalDateTimeExpression.
	 */
	@Override
	public BooleanExpression isNot(LocalDateTime date) {
		return this.isNot(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is NOT
	 * equal to the supplied date.
	 *
	 * @param dateExpression the date the expression must not match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the LocalDateTimeResult and this
	 * LocalDateTimeExpression.
	 */
	@Override
	public BooleanExpression isNot(LocalDateTimeResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new LocalDateTimeIsNotExpression(this, dateExpression));
		if (isExpr.getIncludesNull()) {
			return BooleanExpression.isNotNull(this);
		} else {
			return isExpr;
		}
	}

	/**
	 * Returns FALSE if this expression evaluates to NULL, otherwise TRUE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	/**
	 * Returns TRUE if this expression evaluates to NULL, otherwise FALSE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(LocalDateTimeResult lowerBound, LocalDateTimeResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(LocalDateTime lowerBound, LocalDateTimeResult upperBound) {
		return super.isBetween(lowerBound, upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(LocalDateTimeResult lowerBound, LocalDateTime upperBound) {
		return super.isBetween(lowerBound, upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(LocalDateTime lowerBound, LocalDateTime upperBound) {
		return super.isBetween(lowerBound, upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(LocalDateTimeResult lowerBound, LocalDateTimeResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(LocalDateTime lowerBound, LocalDateTimeResult upperBound) {
		return super.isBetweenInclusive(lowerBound, upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(LocalDateTimeResult lowerBound, LocalDateTime upperBound) {
		return super.isBetweenInclusive(lowerBound, upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(LocalDateTime lowerBound, LocalDateTime upperBound) {
		return super.isBetweenInclusive(lowerBound, upperBound);
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
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(LocalDateTimeResult lowerBound, LocalDateTimeResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
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
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(LocalDateTime lowerBound, LocalDateTimeResult upperBound) {
		return super.isBetweenExclusive(lowerBound, upperBound);
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
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(LocalDateTimeResult lowerBound, LocalDateTime upperBound) {
		return super.isBetweenExclusive(lowerBound, upperBound);
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
	 * @param lowerBound the lower bound that the expression must exceed
	 * @param upperBound the upper bound that the expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(LocalDateTime lowerBound, LocalDateTime upperBound) {
		return super.isBetweenExclusive(lowerBound, upperBound);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than to the supplied date.
	 *
	 * @param date the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThan(LocalDateTime date) {
		return super.isLessThan(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than to the supplied date.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThan(LocalDateTimeResult dateExpression) {
		return new BooleanExpression(new LocalDateTimeIsLessThanExpression(this, dateExpression));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than to the supplied date.
	 *
	 * @param date the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isEarlierThan(LocalDateTime date) {
		return isEarlierThan(new LocalDateTimeExpression(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than to the supplied date.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isEarlierThan(LocalDateTimeResult dateExpression) {
		return this.isLessThan(dateExpression);
	}

	/**
	 * Create a DateRepeat value representing the difference between this date
	 * expression and the one provided
	 *
	 * @param date the other date which defines this DateRepeat
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateRepeat expression
	 */
	public DateRepeatExpression getDateRepeatFrom(LocalDateTime date) {
		return getDateRepeatFrom(value(date));
	}

	/**
	 * Create a DateRepeat value representing the difference between this date
	 * expression and the one provided
	 *
	 * @param dateExpression the other date which defines this DateRepeat
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return DateRepeat expression
	 */
	public DateRepeatExpression getDateRepeatFrom(LocalDateTimeResult dateExpression) {
		return new DateRepeatExpression(new LocalDateTimeGetDateRepeatFromExpression(this, dateExpression));
	}

	/**
	 * Subtract the period/duration provided from this date expression to get an
	 * offset date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTime expression
	 */
	public LocalDateTimeExpression minus(Period interval) {
		return minus(DateRepeatExpression.value(interval));
	}

	/**
	 * Subtract the period/duration provided from this date expression to get an
	 * offset date.
	 *
	 * @param intervalExpression the amount of time this date needs to be offset
	 * by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTime expression
	 */
	public LocalDateTimeExpression minus(DateRepeatResult intervalExpression) {
		return new LocalDateTimeExpression(new LocalDateTimeMinusDateRepeatExpression(this, intervalExpression));
	}

	/**
	 * Add the period/duration provided from this date expression to get an offset
	 * date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTime expression
	 */
	public LocalDateTimeExpression plus(Period interval) {
		return plus(DateRepeatExpression.value(interval));
	}

	/**
	 * Add the period/duration provided from this date expression to get an offset
	 * date.
	 *
	 * @param intervalExpression the amount of time this date needs to be offset
	 * by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTime expression
	 */
	public LocalDateTimeExpression plus(DateRepeatResult intervalExpression) {
		return new LocalDateTimeExpression(new LocalDateTimePlusDateRepeatExpression(this, intervalExpression));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied date.
	 *
	 * @param date the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(LocalDateTime date) {
		return super.isLessThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied LocalDateTimeResult.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(LocalDateTimeResult dateExpression) {
		return new BooleanExpression(new LocalDateTimeIsLessThanOrEqualExpression(this, dateExpression));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied date.
	 *
	 * @param date the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will evaluate to a greater than operation
	 */
	@Override
	public BooleanExpression isGreaterThan(LocalDateTime date) {
		return super.isGreaterThan(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied LocalDateTimeResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThan(LocalDateTimeResult dateExpression) {
		return new BooleanExpression(new LocalDateTimeIsGreaterThanExpression(this, dateExpression));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied date.
	 *
	 * @param date the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will evaluate to a greater than operation
	 */
	public BooleanExpression isLaterThan(LocalDateTime date) {
		return isGreaterThan(new LocalDateTimeExpression(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied LocalDateTimeResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLaterThan(LocalDateTimeResult dateExpression) {
		return isGreaterThan(dateExpression);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied LocalDateTime.
	 *
	 * @param date the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(LocalDateTime date) {
		return super.isGreaterThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied LocalDateTimeResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(LocalDateTimeResult dateExpression) {
		return new BooleanExpression(new LocalDateTimeIsGreaterThanOrEqualExpression(this, dateExpression));
	}

	/**
	 * Like LESSTHAN_OR_EQUAL but only includes the EQUAL values if the fallback
	 * matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(LocalDateTime value, BooleanExpression fallBackWhenEquals) {
		return super.isLessThan(value, fallBackWhenEquals);
	}

	/**
	 * Like GREATERTHAN_OR_EQUAL but only includes the EQUAL values if the
	 * fallback matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(LocalDateTime value, BooleanExpression fallBackWhenEquals) {
		return super.isGreaterThan(value, fallBackWhenEquals);
	}

	/**
	 * Like LESSTHAN_OR_EQUAL but only includes the EQUAL values if the fallback
	 * matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(LocalDateTimeResult value, BooleanExpression fallBackWhenEquals) {
		return this.isLessThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Like GREATERTHAN_OR_EQUAL but only includes the EQUAL values if the
	 * fallback matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(LocalDateTimeResult value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of LocalDateTimes.
	 *
	 * <p>
	 * Be careful when using this expression as dates have lots of fields and it
	 * is easy to miss a similar date.
	 *
	 * @param possibleValues allowed values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
//	@Override
//	public BooleanExpression isIn(LocalDateTime... possibleValues) {
//		List<LocalDateTimeExpression> possVals = new ArrayList<LocalDateTimeExpression>();
//		for (LocalDateTime num : possibleValues) {
//			possVals.add(value(num));
//		}
//		return isIn(possVals.toArray(new LocalDateTimeExpression[]{}));
//	}
	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of LocalDateTimes.
	 *
	 * <p>
	 * Be careful when using this expression as dates have lots of fields and it
	 * is easy to miss a similar date.
	 *
	 * @param possibleValues allowed values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isIn(Collection<? extends LocalDateTimeResult> possibleValues) {
		//List<LocalDateTimeExpression> possVals = new ArrayList<LocalDateTimeExpression>();
		//for (LocalDateTime num : possibleValues) {
		//	possVals.add(value(num));
		//}
		return isIn(possibleValues.toArray(new LocalDateTimeResult[]{}));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of LocalDateTimeResults.
	 *
	 * <p>
	 * Be careful when using this expression as dates have lots of fields and it
	 * is easy to miss a similar date.
	 *
	 * @param possibleValues allowed values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isInCollection(Collection<LocalDateTimeResult> possibleValues) {
		BooleanExpression isInExpr = new BooleanExpression(new LocalDateTimeIsInExpression(this, possibleValues));
		if (isInExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isInExpr);
		} else {
			return isInExpr;
		}
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of LocalDateTimeResults.
	 *
	 * <p>
	 * Be careful when using this expression as dates have lots of fields and it
	 * is easy to miss a similar date.
	 *
	 * @param possibleValues allowed values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isNotInCollection(Collection<LocalDateTimeResult> possibleValues) {
		BooleanExpression isNotInExpr = new BooleanExpression(new LocalDateTimeIsNotInExpression(this, possibleValues));
		if (isNotInExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isNotInExpr);
		} else {
			return isNotInExpr;
		}
	}

	/**
	 * Creates and expression that replaces a NULL result with the supplied date.
	 *
	 * <p>
	 * This is a way of handling dates that should have a value but don't.
	 *
	 * @param alternative use this value if the expression evaluates to NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public LocalDateTimeExpression ifDBNull(LocalDateTime alternative) {
		return ifDBNull(value(alternative));
//		return new LocalDateTimeExpression(
//				new LocalDateTimeExpression.LocalDateTimeLocalDateTimeFunctionWithLocalDateTimeResult(this, new LocalDateTimeExpression(alternative)) {
//			@Override
//			protected String getFunctionName(DBDefinition db) {
//				return db.getIfNullFunctionName();
//			}
//
//			@Override
//			public boolean getIncludesNull() {
//				return false;
//			}
//		});
	}

	/**
	 * Creates and expression that replaces a NULL result with the supplied
	 * LocalDateTimeResult.
	 *
	 * <p>
	 * This is a way of handling dates that should have a value but don't.
	 *
	 * @param alternative use this value if the expression evaluates to NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public LocalDateTimeExpression ifDBNull(LocalDateTimeResult alternative) {
		return new LocalDateTimeExpression(
				new LocalDateTimeIfDBNullExpression(this, alternative));
	}

	/**
	 * Aggregates the dates found in a query to find the maximum date in the
	 * selection.
	 *
	 * <p>
	 * For use in expression columns and {@link DBReport}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public LocalDateTimeMaxExpression max() {
		return new LocalDateTimeMaxExpression(this);
	}

	/**
	 * Aggregates the dates found in a query to find the minimum date in the
	 * selection.
	 *
	 * <p>
	 * For use in expression columns and {@link DBReport}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public LocalDateTimeMinExpression min() {
		return new LocalDateTimeMinExpression(this);
	}

	@Override
	public DBLocalDateTime getQueryableDatatypeForExpressionValue() {
		return new DBLocalDateTime();
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of seconds to the date
	 * expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addSeconds(int secondsToAdd) {
		return this.addSeconds(value(secondsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of seconds to the date
	 * expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addSeconds(NumberExpression secondsToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddSecondsExpression(this, secondsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of seconds to the date
	 * expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addSeconds(IntegerExpression secondsToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddIntegerSecondsExpression(this, secondsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of minutes to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addMinutes(int minutesToAdd) {
		return this.addMinutes(value(minutesToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of minutes to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addMinutes(IntegerExpression minutesToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddIntegerMinutesExpression(this, minutesToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addDays(Integer daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addDays(Long daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addDays(Number daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addDays(IntegerExpression daysToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddIntegerDaysExpression(this, daysToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addDays(NumberExpression daysToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddDaysExpression(this, daysToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of hours to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addHours(int hoursToAdd) {
		return this.addHours(value(hoursToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of hours to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addHours(IntegerExpression hoursToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddIntegerHoursExpression(this, hoursToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of weeks to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addWeeks(int weeksToAdd) {
		return this.addWeeks(value(weeksToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of weeks to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addWeeks(IntegerExpression weeksToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddIntegerWeeksExpression(this, weeksToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addMonths(Number monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addMonths(Integer monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addMonths(Long monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addMonths(IntegerExpression monthsToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddIntegerMonthsExpression(this, monthsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addMonths(NumberExpression monthsToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddMonthsExpression(this, monthsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of years to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addYears(int yearsToAdd) {
		return this.addYears(value(yearsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: add the supplied number of years to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDateTimeExpression
	 */
	public LocalDateTimeExpression addYears(IntegerExpression yearsToAdd) {
		return new LocalDateTimeExpression(
				new LocalDateTimeAddIntegerYearsExpression(this, yearsToAdd));
	}

	/**
	 * LocalDateTime Arithmetic: get the days between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression daysFrom(LocalDateTime dateToCompareTo) {
		return daysFrom(LocalDateTimeExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the days between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression daysFrom(LocalDateTimeResult dateToCompareTo) {
		return new NumberExpression(
				new LocalDateTimeDaysFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the weeks between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression weeksFrom(LocalDateTime dateToCompareTo) {
		return weeksFrom(LocalDateTimeExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the weeks between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression weeksFrom(LocalDateTimeExpression dateToCompareTo) {
		return new NumberExpression(
				new LocalDateTimeWeeksFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the months between the date expression and
	 * the supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression monthsFrom(LocalDateTime dateToCompareTo) {
		return monthsFrom(LocalDateTimeExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the months between the date expression and
	 * the supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression monthsFrom(LocalDateTimeResult dateToCompareTo) {
		return new NumberExpression(
				new LocalDateTimeMonthsFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the years between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression yearsFrom(LocalDateTime dateToCompareTo) {
		return yearsFrom(LocalDateTimeExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the years between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression yearsFrom(LocalDateTimeResult dateToCompareTo) {
		return new NumberExpression(
				new LocalDateTimeYearsFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the Hours between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression hoursFrom(LocalDateTime dateToCompareTo) {
		return hoursFrom(LocalDateTimeExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the Hours between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression hoursFrom(LocalDateTimeResult dateToCompareTo) {
		return new NumberExpression(
				new LocalDateTimeHoursFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the minutes between the date expression and
	 * the supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression minutesFrom(LocalDateTime dateToCompareTo) {
		return minutesFrom(LocalDateTimeExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the days between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression minutesFrom(LocalDateTimeResult dateToCompareTo) {
		return new NumberExpression(
				new LocalDateTimeMinutesFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the seconds between the date expression and
	 * the supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression secondsFrom(LocalDateTime dateToCompareTo) {
		return secondsFrom(LocalDateTimeExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDateTime Arithmetic: get the days between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression secondsFrom(LocalDateTimeResult dateToCompareTo) {
		return new NumberExpression(
				new LocalDateTimeSecondsFromExpression(this, dateToCompareTo));
	}

	/**
	 * Derive the first day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a LocalDateTime expression
	 */
	public LocalDateTimeExpression firstOfMonth() {
		return this.addDays(this.day().minus(1).bracket().times(-1).integerResult());
	}

	/**
	 * Derive the last day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a LocalDateTime expression
	 */
	public LocalDateTimeExpression endOfMonth() {
		return new LocalDateTimeEndOfMonthExpression(this);
	}

	/**
	 * Return the index of the day of the week that this date expression refers
	 * to.
	 *
	 * Refer to {@link #SUNDAY},  {@link #MONDAY}, etc
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an index of the day of the week.
	 */
	public NumberExpression dayOfWeek() {
		return new NumberExpression(
				new LocalDateTimeDayOfWeekExpression(this));
	}

	/**
	 * Considering the first and second dates as end point for a time period and
	 * similarly for the third and fourth, tests whether the 2 time periods
	 * overlap.
	 *
	 * @param firstStartTime the beginning of the first interval
	 * @param firstEndTime the end of the first interval
	 * @param secondStartTime the beginning of the second interval
	 * @param secondEndtime the end of the second interval
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression
	 */
	public static BooleanExpression overlaps(LocalDateTime firstStartTime, LocalDateTime firstEndTime, LocalDateTime secondStartTime, LocalDateTime secondEndtime) {
		return LocalDateTimeExpression.overlaps(LocalDateTimeExpression.value(firstStartTime), LocalDateTimeExpression.value(firstEndTime),
				LocalDateTimeExpression.value(secondStartTime), LocalDateTimeExpression.value(secondEndtime)
		);
	}

	/**
	 * Considering the first and second dates as end point for a time period and
	 * similarly for the third and fourth, tests whether the 2 time periods
	 * overlap.
	 *
	 * @param firstStartTime the beginning of the first interval
	 * @param firstEndTime the end of the first interval
	 * @param secondStartTime the beginning of the second interval
	 * @param secondEndtime the end of the second interval
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression
	 */
	public static BooleanExpression overlaps(LocalDateTimeResult firstStartTime, LocalDateTimeResult firstEndTime, LocalDateTimeResult secondStartTime, LocalDateTimeResult secondEndtime) {
		return LocalDateTimeExpression.overlaps(new LocalDateTimeExpression(firstStartTime), new LocalDateTimeExpression(firstEndTime),
				secondStartTime, secondEndtime
		);
	}

	/**
	 * Considering the first and second dates as end point for a time period and
	 * similarly for the third and fourth, tests whether the 2 time periods
	 * overlap.
	 *
	 * @param firstStartTime the beginning of the first interval
	 * @param firstEndTime the end of the first interval
	 * @param secondStartTime the beginning of the second interval
	 * @param secondEndtime the end of the second interval
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Boolean expression
	 */
	public static BooleanExpression overlaps(LocalDateTimeExpression firstStartTime, LocalDateTimeExpression firstEndTime, LocalDateTimeResult secondStartTime, LocalDateTimeResult secondEndtime) {
		return BooleanExpression.anyOf(
				firstStartTime.isBetween(
						leastOf(secondStartTime, secondEndtime),
						greatestOf(secondStartTime, secondEndtime)),
				firstEndTime.isBetween(
						leastOf(secondStartTime, secondEndtime),
						greatestOf(secondStartTime, secondEndtime)
				)
		);
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.</p>
	 *
	 * @param possibleValue the first possible value
	 * @param possibleValue2 the second possible value
	 * @param possibleValues all other possible values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static LocalDateTimeExpression leastOf(LocalDateTime possibleValue, LocalDateTime possibleValue2, LocalDateTime... possibleValues) {
		ArrayList<LocalDateTimeExpression> possVals = new ArrayList<LocalDateTimeExpression>();
		possVals.add(value(possibleValue));
		possVals.add(value(possibleValue2));
		for (LocalDateTime num : possibleValues) {
			possVals.add(value(num));
		}
		return internalLeastOf(possVals.toArray(new LocalDateTimeExpression[]{}));
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static LocalDateTimeExpression leastOf(Collection<? extends LocalDateTimeResult> possibleValues) {
		if (possibleValues.size() > 1) {
			ArrayList<LocalDateTimeExpression> possVals = new ArrayList<LocalDateTimeExpression>();
			possibleValues.forEach((num) -> {
				possVals.add(new LocalDateTimeExpression(num));
			});
			return internalLeastOf(possVals.toArray(new LocalDateTimeExpression[]{}));
		} else if (possibleValues.size() == 1) {
			return new LocalDateTimeExpression(possibleValues.toArray(new LocalDateTimeResult[]{})[0]);
		} else {
			return nullLocalDateTime();
		}
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValue the first possible value
	 * @param possibleValue2 the second possible value
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static LocalDateTimeExpression leastOf(LocalDateTimeResult possibleValue, LocalDateTimeResult possibleValue2, LocalDateTimeResult... possibleValues) {
		List<LocalDateTimeResult> vals = new ArrayList<>();
		vals.add(possibleValue);
		vals.add(possibleValue2);
		vals.addAll(Arrays.asList(possibleValues));
		return internalLeastOf(vals.toArray(new LocalDateTimeResult[]{}));
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	protected static LocalDateTimeExpression internalLeastOf(LocalDateTimeResult... possibleValues) {
		LocalDateTimeExpression leastExpr
				= new LocalDateTimeExpression(new LocalDateTimeLeastOfExpression(possibleValues));
		return leastExpr;
	}

	/**
	 * Returns the largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValue the first possible value
	 * @param possibleValue2 the second possible value
	 * @param possibleValues all other possible values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the largest value from the list.
	 */
	public static LocalDateTimeExpression greatestOf(LocalDateTime possibleValue, LocalDateTime possibleValue2, LocalDateTime... possibleValues) {
		ArrayList<LocalDateTimeExpression> possVals = new ArrayList<LocalDateTimeExpression>();
		possVals.add(value(possibleValue));
		possVals.add(value(possibleValue2));
		for (LocalDateTime num : possibleValues) {
			possVals.add(value(num));
		}
		return internalGreatestOf(possVals.toArray(new LocalDateTimeExpression[]{}));
	}

	/**
	 * Returns the largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the largest of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the largest value from the list.
	 */
	public static LocalDateTimeExpression greatestOf(Collection<? extends LocalDateTimeResult> possibleValues) {
		if (possibleValues.size() > 1) {
			ArrayList<LocalDateTimeExpression> possVals = new ArrayList<LocalDateTimeExpression>();
			for (LocalDateTimeResult num : possibleValues) {
				possVals.add(new LocalDateTimeExpression(num));
			}
			return internalGreatestOf(possVals.toArray(new LocalDateTimeExpression[]{}));
		} else if (possibleValues.size() == 1) {
			return new LocalDateTimeExpression(possibleValues.toArray(new LocalDateTimeResult[]{})[0]);
		} else {
			return nullLocalDateTime();
		}
	}

	/**
	 * Returns the largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValue the first possible value
	 * @param possibleValue2 the second possible value
	 * @param possibleValues all other possible values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the largest value from the list.
	 */
	public static LocalDateTimeExpression greatestOf(LocalDateTimeResult possibleValue, LocalDateTimeResult possibleValue2, LocalDateTimeResult... possibleValues) {
		List<LocalDateTimeResult> vals = new ArrayList<>();
		vals.add(possibleValue);
		vals.add(possibleValue2);
		vals.addAll(Arrays.asList(possibleValues));
		return internalGreatestOf(vals.toArray(new LocalDateTimeResult[]{}));
	}

	/**
	 * Returns the largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the largest of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the largest value from the list.
	 */
	protected static LocalDateTimeExpression internalGreatestOf(LocalDateTimeResult... possibleValues) {
		LocalDateTimeExpression greatestOf
				= new LocalDateTimeExpression(new LocalDateTimeGreatestOfExpression(possibleValues));
		return greatestOf;
	}

	@Override
	public DBLocalDateTime asExpressionColumn() {
		return new DBLocalDateTime(this);
	}

	/**
	 * Returns the date as an ISO 8601 formatted string NOT including time zone.
	 *
	 * <p>
	 * ISO 8601 form at is YYYY-MM-DDTHH:mm:ss.sss</p>
	 *
	 * <p>
	 * May not be zero padded but the format is still unambiguous.</p>
	 *
	 * @return a ISO formatted version of this date
	 */
	@Override
	public StringExpression stringResult() {
		return this.stringResultISOFormat();
	}

	/**
	 * Returns the date as an ISO 8601 formatted string NOT including time zone.
	 *
	 * <p>
	 * ISO 8601 form at is YYYY-MM-DDTHH:mm:ss.sss</p>
	 *
	 * <p>
	 * May not be zero padded but the format is still unambiguous.</p>
	 *
	 * @return a ISO formatted version of this date
	 */
	public StringExpression stringResultISOFormat() {
		StringExpression isoFormatLocalDateTimeTime = value("")
				.append(this.year())
				.append("-")
				.append(this.month())
				.append("-")
				.append(this.day())
				.append("T")
				.append(this.hour())
				.append(":")
				.append(this.minute())
				.append(":")
				.append(this.second())
				.append(".")
				.append(this.subsecond());
		return isoFormatLocalDateTimeTime;
	}

	/**
	 * Returns the date as a USA formatted string NOT including time zone.
	 *
	 * <p>
	 * USA format is MM-DD-YYYY HH:mm:ss.sss</p>
	 *
	 * <p>
	 * May not be zero padded but the format is still unambiguous.</p>
	 *
	 * @return a USA formatted version of this date
	 */
	public StringExpression stringResultUSAFormat() {
		StringExpression usaFormatLocalDateTimeTime = value("")
				.append(this.month())
				.append("-")
				.append(this.day())
				.append("-")
				.append(this.year())
				.append(" ")
				.append(this.hour())
				.append(":")
				.append(this.minute())
				.append(":")
				.append(this.second())
				.append(".")
				.append(this.subsecond());
		return usaFormatLocalDateTimeTime;
	}

	/**
	 * Returns the date as formatted string NOT including time zone.
	 *
	 * <p>
	 * Common format is DD-MM-YYYY HH:mm:ss.sss</p>
	 *
	 * <p>
	 * May not be zero padded but the format is still unambiguous.</p>
	 *
	 * @return a formatted version of this date using the format commonly used
	 * around the world
	 */
	public StringExpression stringResultCommonFormat() {
		StringExpression commonFormatLocalDateTimeTime = value("")
				.append(this.day())
				.append("-")
				.append(this.month())
				.append("-")
				.append(this.year())
				.append(" ")
				.append(this.hour())
				.append(":")
				.append(this.minute())
				.append(":")
				.append(this.second())
				.append(".")
				.append(this.subsecond());
		return commonFormatLocalDateTimeTime;
	}

	@Override
	public LocalDateTimeExpression expression(LocalDateTime value) {
		return new LocalDateTimeExpression(value);
	}

	@Override
	public LocalDateTimeExpression expression(LocalDateTimeResult value) {
		return new LocalDateTimeExpression(value);
	}

	@Override
	public LocalDateTimeResult expression(DBLocalDateTime value) {
		return new LocalDateTimeExpression(value);
	}

	public LocalDateTimeExpression toLocalDateTime() {
		return new LocalDateTimeExpression(this);
	}

	public LocalDateTimeExpression setYear(int i) {
		return this.addYears(IntegerExpression.value(i).minus(this.year().integerResult()));
	}

	public LocalDateTimeExpression setMonth(Month month) {
		return setMonth(month.getValue());
	}

	public LocalDateTimeExpression setMonth(int i) {
		return this.addMonths(IntegerExpression.value(i).minus(this.month().integerResult()));
	}

	public LocalDateTimeExpression setDay(int i) {
		return this.addDays(IntegerExpression.value(i).minus(this.day().integerResult()));
	}

	public LocalDateTimeExpression setHour(int i) {
		return this.addHours(IntegerExpression.value(i).minus(this.hour().integerResult()));
	}

	public LocalDateTimeExpression setMinute(int i) {
		return this.addMinutes(IntegerExpression.value(i).minus(this.minute().integerResult()));
	}

	public LocalDateTimeExpression setSecond(int i) {
		return this.addSeconds(IntegerExpression.value(i).minus(this.second().integerResult()));
	}

	public LocalDateTimeExpression setYear(IntegerExpression i) {
		return this.addYears(i.minus(this.year().integerResult()));
	}

	public LocalDateTimeExpression setMonth(IntegerExpression i) {
		return this.addMonths(i.minus(this.month().integerResult()));
	}

	public LocalDateTimeExpression setDay(IntegerExpression i) {
		return this.addDays(i.minus(this.day().integerResult()));
	}

	public LocalDateTimeExpression setHour(IntegerExpression i) {
		return this.addHours(i.minus(this.hour().integerResult()));
	}

	public LocalDateTimeExpression setMinute(IntegerExpression i) {
		return this.addMinutes(i.minus(this.minute().integerResult()));
	}

	public LocalDateTimeExpression setSecond(IntegerExpression i) {
		return this.addSeconds(i.minus(this.second().integerResult()));
	}

	private static abstract class FunctionWithLocalDateTimeResult extends LocalDateTimeExpression implements CanBeWindowingFunctionWithFrame<LocalDateTimeExpression> {

		private static final long serialVersionUID = 1L;

		FunctionWithLocalDateTimeResult() {
		}

		protected String getFunctionName(DBDefinition db) {
			return "";
		}

		protected String beforeValue(DBDefinition db) {
			return " " + getFunctionName(db) + "";
		}

		protected String afterValue(DBDefinition db) {
			return " ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + this.afterValue(db);
		}

		@Override
		public LocalDateTimeExpression.FunctionWithLocalDateTimeResult copy() {
			LocalDateTimeExpression.FunctionWithLocalDateTimeResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return new HashSet<DBRow>();
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			return true;
		}

		@Override
		public WindowFunctionFramable<LocalDateTimeExpression> over() {
			return new WindowFunctionFramable<LocalDateTimeExpression>(new LocalDateTimeExpression(this));
		}
	}

	private static abstract class LocalDateTimeExpressionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		LocalDateTimeExpressionWithNumberResult() {
			super();
		}

		LocalDateTimeExpressionWithNumberResult(LocalDateTimeExpression only) {
			super(only);
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public abstract String toSQLString(DBDefinition db);
	}

	private static abstract class LocalDateTimeLocalDateTimeExpressionWithBooleanResult extends BooleanExpression implements CanBeWindowingFunctionWithFrame<BooleanExpression> {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression first;
		protected LocalDateTimeExpression second;
		private boolean requiresNullProtection = false;

		LocalDateTimeLocalDateTimeExpressionWithBooleanResult(LocalDateTimeExpression first, LocalDateTimeResult second) {
			this.first = first;
			this.second = new LocalDateTimeExpression(second);
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
		}

		@Override
		public LocalDateTimeLocalDateTimeExpressionWithBooleanResult copy() {
			LocalDateTimeLocalDateTimeExpressionWithBooleanResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			if (second != null) {
				hashSet.addAll(second.getTablesInvolved());
			}
			return hashSet;
		}

		protected abstract String getEquationOperator(DBDefinition db);

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}

		@Override
		public WindowFunctionFramable<BooleanExpression> over() {
			return new WindowFunctionFramable<BooleanExpression>(new BooleanExpression(first));
		}
	}

	private static abstract class LocalDateTimeLocalDateTimeExpressionWithDateRepeatResult extends DateRepeatExpression implements CanBeWindowingFunctionWithFrame<DateRepeatExpression> {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression first;
		protected LocalDateTimeExpression second;
		private boolean requiresNullProtection = false;

		LocalDateTimeLocalDateTimeExpressionWithDateRepeatResult(LocalDateTimeExpression first, LocalDateTimeResult second) {
			this.first = first;
			this.second = new LocalDateTimeExpression(second);
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public LocalDateTimeLocalDateTimeExpressionWithDateRepeatResult copy() {
			LocalDateTimeLocalDateTimeExpressionWithDateRepeatResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst().copy();
			newInstance.second = getSecond().copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (getFirst() != null) {
				hashSet.addAll(getFirst().getTablesInvolved());
			}
			if (getSecond() != null) {
				hashSet.addAll(getSecond().getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator() || getSecond().isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		public LocalDateTimeExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		public LocalDateTimeExpression getSecond() {
			return second;
		}

		@Override
		public WindowFunctionFramable<DateRepeatExpression> over() {
			return new WindowFunctionFramable<DateRepeatExpression>(new DateRepeatExpression(this));
		}
	}

	private static abstract class LocalDateTimeDateRepeatArithmeticLocalDateTimeResult extends LocalDateTimeExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression first;
		protected DateRepeatExpression second;
		private boolean requiresNullProtection = false;

		LocalDateTimeDateRepeatArithmeticLocalDateTimeResult(LocalDateTimeExpression first, DateRepeatResult second) {
			this.first = first;
			this.second = new DateRepeatExpression(second);
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.doExpressionTransformation(db);
		}

		@Override
		public LocalDateTimeDateRepeatArithmeticLocalDateTimeResult copy() {
			LocalDateTimeDateRepeatArithmeticLocalDateTimeResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst().copy();
			newInstance.second = getSecond().copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (getFirst() != null) {
				hashSet.addAll(getFirst().getTablesInvolved());
			}
			if (getSecond() != null) {
				hashSet.addAll(getSecond().getTablesInvolved());
			}
			return hashSet;
		}

		protected abstract String doExpressionTransformation(DBDefinition db);

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator() || getSecond().isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		public LocalDateTimeExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		public DateRepeatExpression getSecond() {
			return second;
		}
	}

	private static abstract class LocalDateTimeArrayFunctionWithLocalDateTimeResult extends LocalDateTimeExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression column;
		protected final List<LocalDateTimeResult> values = new ArrayList<LocalDateTimeResult>();
		boolean nullProtectionRequired = false;

		LocalDateTimeArrayFunctionWithLocalDateTimeResult() {
		}

		LocalDateTimeArrayFunctionWithLocalDateTimeResult(LocalDateTimeResult[] rightHandSide) {
			for (LocalDateTimeResult dateResult : rightHandSide) {
				if (dateResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (dateResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					}
					values.add(dateResult);
				}
			}
		}

		protected String getFunctionName(DBDefinition db) {
			return "";
		}

		protected String beforeValue(DBDefinition db) {
			return "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			StringBuilder builder = new StringBuilder();
			builder
					.append(this.getFunctionName(db))
					.append(this.beforeValue(db));
			String separator = "";
			for (LocalDateTimeResult val : values) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public LocalDateTimeArrayFunctionWithLocalDateTimeResult copy() {
			LocalDateTimeArrayFunctionWithLocalDateTimeResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			for (LocalDateTimeResult value : this.values) {
				newInstance.values.add(value.copy());
			}

			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			for (LocalDateTimeResult second : values) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false;
			if (column != null) {
				result = column.isAggregator();
			}
			for (LocalDateTimeResult numer : values) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (column == null && values.isEmpty()) {
				return true;
			} else {
				boolean result = column == null ? true : column.isPurelyFunctional();
				for (LocalDateTimeResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class LocalDateTimeLocalDateTimeResultFunctionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private LocalDateTimeExpression column;
		private List<LocalDateTimeResult> values = new ArrayList<LocalDateTimeResult>();
		boolean nullProtectionRequired = false;

		LocalDateTimeLocalDateTimeResultFunctionWithBooleanResult() {
		}

		LocalDateTimeLocalDateTimeResultFunctionWithBooleanResult(LocalDateTimeExpression leftHandSide, Collection<LocalDateTimeResult> rightHandSide) {
			this.column = leftHandSide;
			for (LocalDateTimeResult dateResult : rightHandSide) {
				if (dateResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (dateResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					} else {
						values.add(dateResult);
					}
				}
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		protected String getFunctionName(DBDefinition db) {
			return "";
		}

		protected String beforeValue(DBDefinition db) {
			return "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			StringBuilder builder = new StringBuilder();
			builder
					.append(getColumn().toSQLString(db))
					.append(this.getFunctionName(db))
					.append(this.beforeValue(db));
			String separator = "";
			for (LocalDateTimeResult val : getValues()) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public LocalDateTimeLocalDateTimeResultFunctionWithBooleanResult copy() {
			LocalDateTimeLocalDateTimeResultFunctionWithBooleanResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.getColumn().copy();
			for (LocalDateTimeResult value : this.getValues()) {
				newInstance.getValues().add(value.copy());
			}

			Collections.copy(this.getValues(), newInstance.getValues());
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (getColumn() != null) {
				hashSet.addAll(getColumn().getTablesInvolved());
			}
			for (LocalDateTimeResult val : getValues()) {
				if (val != null) {
					hashSet.addAll(val.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false || getColumn().isAggregator();
			for (LocalDateTimeResult dater : getValues()) {
				result = result || dater.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the column
		 */
		protected LocalDateTimeExpression getColumn() {
			return column;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the values
		 */
		protected List<LocalDateTimeResult> getValues() {
			return values;
		}
	}

	private static abstract class LocalDateTimeLocalDateTimeFunctionWithLocalDateTimeResult extends LocalDateTimeExpression {

		private static final long serialVersionUID = 1L;

		private LocalDateTimeExpression first;
		private LocalDateTimeResult second;

		LocalDateTimeLocalDateTimeFunctionWithLocalDateTimeResult(LocalDateTimeExpression first) {
			this.first = first;
			this.second = null;
		}

		LocalDateTimeLocalDateTimeFunctionWithLocalDateTimeResult(LocalDateTimeExpression first, LocalDateTimeResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public abstract String toSQLString(DBDefinition db);

		@Override
		public LocalDateTimeLocalDateTimeFunctionWithLocalDateTimeResult copy() {
			LocalDateTimeLocalDateTimeFunctionWithLocalDateTimeResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst().copy();
			newInstance.second = getSecond().copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (getFirst() != null) {
				hashSet.addAll(getFirst().getTablesInvolved());
			}
			if (getSecond() != null) {
				hashSet.addAll(getSecond().getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator() || getSecond().isAggregator();
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		protected LocalDateTimeExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected LocalDateTimeResult getSecond() {
			return second;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else if (first == null) {
				return second.isPurelyFunctional();
			} else if (second == null) {
				return first.isPurelyFunctional();
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class LocalDateTimeFunctionWithLocalDateTimeResult extends LocalDateTimeExpression {

		private static final long serialVersionUID = 1L;

		LocalDateTimeFunctionWithLocalDateTimeResult() {
			super();
		}

		LocalDateTimeFunctionWithLocalDateTimeResult(LocalDateTimeExpression only) {
			super(only);
		}

		protected String getFunctionName(DBDefinition db) {
			return "";
		}

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (getInnerResult() == null ? "" : getInnerResult().toSQLString(db)) + this.afterValue(db);
		}
	}

	private static abstract class LocalDateTimeIntegerExpressionWithLocalDateTimeResult extends LocalDateTimeExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression first;
		protected IntegerExpression second;

		LocalDateTimeIntegerExpressionWithLocalDateTimeResult() {
			this.first = null;
			this.second = null;
		}

		LocalDateTimeIntegerExpressionWithLocalDateTimeResult(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			this.first = dateExp;
			this.second = numbExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public LocalDateTimeIntegerExpressionWithLocalDateTimeResult copy() {
			LocalDateTimeIntegerExpressionWithLocalDateTimeResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			final Set<DBRow> tablesInvolved = first.getTablesInvolved();
			tablesInvolved.addAll(second.getTablesInvolved());
			return tablesInvolved;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else if (first == null) {
				return second.isPurelyFunctional();
			} else if (second == null) {
				return first.isPurelyFunctional();
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class LocalDateTimeNumberExpressionWithLocalDateTimeResult extends LocalDateTimeExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression first;
		protected NumberExpression second;

		LocalDateTimeNumberExpressionWithLocalDateTimeResult() {
			this.first = null;
			this.second = null;
		}

		LocalDateTimeNumberExpressionWithLocalDateTimeResult(LocalDateTimeExpression dateExp, NumberExpression numbExp) {
			this.first = dateExp;
			this.second = numbExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			final Set<DBRow> tablesInvolved = first.getTablesInvolved();
			tablesInvolved.addAll(second.getTablesInvolved());
			return tablesInvolved;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else if (first == null) {
				return second.isPurelyFunctional();
			} else if (second == null) {
				return first.isPurelyFunctional();
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class LocalDateTimeLocalDateTimeFunctionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression first;
		protected LocalDateTimeResult second;

		LocalDateTimeLocalDateTimeFunctionWithNumberResult() {
			this.first = null;
			this.second = null;
		}

		LocalDateTimeLocalDateTimeFunctionWithNumberResult(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			this.first = dateExp;
			this.second = otherLocalDateTimeExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public LocalDateTimeLocalDateTimeFunctionWithNumberResult copy() {
			LocalDateTimeLocalDateTimeFunctionWithNumberResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			final Set<DBRow> tablesInvolved = first.getTablesInvolved();
			tablesInvolved.addAll(second.getTablesInvolved());
			return tablesInvolved;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else if (first == null) {
				return second.isPurelyFunctional();
			} else if (second == null) {
				return first.isPurelyFunctional();
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static class LocalDateTimeNullExpression extends LocalDateTimeExpression {

		public LocalDateTimeNullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public LocalDateTimeNullExpression copy() {
			return new LocalDateTimeNullExpression();
		}
	}

	protected static class CurrentLocalDateLocalDateTimeExpression extends FunctionWithLocalDateTimeResult {

		public CurrentLocalDateLocalDateTimeExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentDateOnlyTransform();
		}

		@Override
		public DBLocalDateTime getQueryableDatatypeForExpressionValue() {
			return new DBLocalDateTime();
		}

		@Override
		public CurrentLocalDateLocalDateTimeExpression copy() {
			return new CurrentLocalDateLocalDateTimeExpression();
		}

	}

	protected static class LocalDateTimeCurrentLocalDateTimeExpression extends FunctionWithLocalDateTimeResult {

		public LocalDateTimeCurrentLocalDateTimeExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition defn) {
			final DoCurrentDateTransformationExpression doCurrentDateTransformationExpression = new DoCurrentDateTransformationExpression();
			if (defn.requiresAddingTimeZoneToCurrentLocalDateTime()) {
				ZoneOffset offset = OffsetDateTime.now().getOffset();
				int totalSeconds = offset.getTotalSeconds();
				return doCurrentDateTransformationExpression.addSeconds(totalSeconds).toSQLString(defn);
			} else {
				return doCurrentDateTransformationExpression.toSQLString(defn);
			}
		}

		@Override
		public LocalDateTimeCurrentLocalDateTimeExpression copy() {
			return new LocalDateTimeCurrentLocalDateTimeExpression();
		}

		private static class DoCurrentDateTransformationExpression extends LocalDateTimeExpression {

			public DoCurrentDateTransformationExpression() {
			}
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition defn) {
				return defn.doCurrentDateTimeTransform();
			}

			@Override
			public DoCurrentDateTransformationExpression copy() {
				return new DoCurrentDateTransformationExpression();
			}
		}

	}

	protected static class LocalDateTimeCurrentTimeExpression extends FunctionWithLocalDateTimeResult {

		public LocalDateTimeCurrentTimeExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentTimeTransform();
		}

		@Override
		public CurrentLocalDateLocalDateTimeExpression copy() {
			return new CurrentLocalDateLocalDateTimeExpression();
		}

	}

	protected static class LocalDateTimeYearExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeYearExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doYearTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeYearExpression copy() {
			return new LocalDateTimeYearExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

	}

	protected static class LocalDateTimeMonthExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeMonthExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMonthTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeMonthExpression copy() {
			return new LocalDateTimeMonthExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

	}

	protected static class LocalDateTimeDayExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeDayExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDayTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeDayExpression copy() {
			return new LocalDateTimeDayExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

	}

	protected static class LocalDateTimeHourExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeHourExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doHourTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeHourExpression copy() {
			return new LocalDateTimeHourExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

	}

	protected static class LocalDateTimeMinuteExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeMinuteExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMinuteTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeMinuteExpression copy() {
			return new LocalDateTimeMinuteExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

	}

	protected static class LocalDateTimeSecondExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeSecondExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeSecondExpression copy() {
			return new LocalDateTimeSecondExpression((LocalDateTimeExpression) getInnerResult().copy());
		}
	}

	protected static class LocalDateTimeSecondAndSubsecondExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeSecondAndSubsecondExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSecondAndSubsecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeSecondAndSubsecondExpression copy() {
			return new LocalDateTimeSecondAndSubsecondExpression((LocalDateTimeExpression) getInnerResult().copy());
		}
	}

	protected static class LocalDateTimeSubsecondExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeSubsecondExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSubsecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeSubsecondExpression copy() {
			return new LocalDateTimeSubsecondExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

	}

	protected static class LocalDateTimeIsExpression extends LocalDateTimeLocalDateTimeExpressionWithBooleanResult {

		public LocalDateTimeIsExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " = ";
		}

		@Override
		public LocalDateTimeIsExpression copy() {
			return new LocalDateTimeIsExpression(first.copy(), second.copy());
		}

	}

	protected static class LocalDateTimeIsNotExpression extends LocalDateTimeLocalDateTimeExpressionWithBooleanResult {

		public LocalDateTimeIsNotExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <> ";
		}

		@Override
		public LocalDateTimeIsNotExpression copy() {
			return new LocalDateTimeIsNotExpression(first.copy(), second.copy());
		}

	}

	protected static class LocalDateTimeIsLessThanExpression extends LocalDateTimeLocalDateTimeExpressionWithBooleanResult {

		public LocalDateTimeIsLessThanExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " < ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeIsLessThanExpression copy() {
			return new LocalDateTimeIsLessThanExpression(first.copy(), second.copy());
		}

	}

	protected static class LocalDateTimeGetDateRepeatFromExpression extends LocalDateTimeLocalDateTimeExpressionWithDateRepeatResult {

		public LocalDateTimeGetDateRepeatFromExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateMinusToDateRepeatTransformation(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final LocalDateTimeExpression left = getFirst();
				final LocalDateTimeExpression right = new LocalDateTimeExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullString(),
								StringExpression.value(INTERVAL_PREFIX)
										.append(left.year().minus(right.year()).bracket()).append(YEAR_SUFFIX)
										.append(left.month().minus(right.month()).bracket()).append(MONTH_SUFFIX)
										.append(left.day().minus(right.day()).bracket()).append(DAY_SUFFIX)
										.append(left.hour().minus(right.hour()).bracket()).append(HOUR_SUFFIX)
										.append(left.minute().minus(right.minute()).bracket()).append(MINUTE_SUFFIX)
										.append(left.secondAndSubSecond().minus(right.secondAndSubSecond()).bracket())
										.append(SECOND_SUFFIX)
						).toSQLString(db);
			}
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeGetDateRepeatFromExpression copy() {
			return new LocalDateTimeGetDateRepeatFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeMinusDateRepeatExpression extends LocalDateTimeDateRepeatArithmeticLocalDateTimeResult {

		public LocalDateTimeMinusDateRepeatExpression(LocalDateTimeExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateMinusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final LocalDateTimeExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullLocalDateTime(),
								left.addYears(right.getYears().times(-1))
										.addMonths(right.getMonths().times(-1))
										.addDays(right.getDays().times(-1))
										.addHours(right.getHours().times(-1))
										.addMinutes(right.getMinutes().times(-1))
										.addSeconds(right.getSeconds().times(-1))
						//									.addMilliseconds(right.getMilliseconds().times(-1))
						).toSQLString(db);
			}
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeMinusDateRepeatExpression copy() {
			return new LocalDateTimeMinusDateRepeatExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimePlusDateRepeatExpression extends LocalDateTimeDateRepeatArithmeticLocalDateTimeResult {

		public LocalDateTimePlusDateRepeatExpression(LocalDateTimeExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDatePlusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final LocalDateTimeExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullLocalDateTime(),
								left.addYears(right.getYears())
										.addMonths(right.getMonths())
										.addDays(right.getDays())
										.addHours(right.getHours())
										.addMinutes(right.getMinutes())
										.addSeconds(right.getSeconds())
						).toSQLString(db);
			}
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimePlusDateRepeatExpression copy() {
			return new LocalDateTimePlusDateRepeatExpression(getFirst().copy(), getSecond().copy());
		}

	}

	protected static class LocalDateTimeIsLessThanOrEqualExpression extends LocalDateTimeLocalDateTimeExpressionWithBooleanResult {

		public LocalDateTimeIsLessThanOrEqualExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeIsLessThanOrEqualExpression copy() {
			return new LocalDateTimeIsLessThanOrEqualExpression(first.copy(), second.copy());
		}

	}

	protected static class LocalDateTimeIsGreaterThanExpression extends LocalDateTimeLocalDateTimeExpressionWithBooleanResult {

		public LocalDateTimeIsGreaterThanExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " > ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeIsGreaterThanExpression copy() {
			return new LocalDateTimeIsGreaterThanExpression(first.copy(), second.copy());
		}

	}

	protected static class LocalDateTimeIsGreaterThanOrEqualExpression extends LocalDateTimeLocalDateTimeExpressionWithBooleanResult {

		public LocalDateTimeIsGreaterThanOrEqualExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " >= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeIsGreaterThanOrEqualExpression copy() {
			return new LocalDateTimeIsGreaterThanOrEqualExpression(first.copy(), second.copy());
		}
	}

	protected class LocalDateTimeIsInExpression extends LocalDateTimeLocalDateTimeResultFunctionWithBooleanResult {

		public LocalDateTimeIsInExpression(LocalDateTimeExpression leftHandSide, Collection<LocalDateTimeResult> rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<String>();
			for (LocalDateTimeResult value : getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		public LocalDateTimeIsInExpression copy() {
			return new LocalDateTimeIsInExpression(getColumn().copy(), getValues());
		}

	}

	protected class LocalDateTimeIsNotInExpression extends LocalDateTimeLocalDateTimeResultFunctionWithBooleanResult {

		public LocalDateTimeIsNotInExpression(LocalDateTimeExpression leftHandSide, Collection<LocalDateTimeResult> rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<String>();
			for (LocalDateTimeResult value : getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doNotInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		public LocalDateTimeIsNotInExpression copy() {
			return new LocalDateTimeIsNotInExpression(getColumn().copy(), getValues());
		}

	}

	protected static class LocalDateTimeIfDBNullExpression extends LocalDateTimeLocalDateTimeFunctionWithLocalDateTimeResult {

		public LocalDateTimeIfDBNullExpression(LocalDateTimeExpression first, LocalDateTimeResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateIfNullTransform(this.getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeIfDBNullExpression copy() {
			return new LocalDateTimeIfDBNullExpression(getFirst().copy(), getSecond().copy());
		}
	}

	public static class LocalDateTimeMaxExpression extends LocalDateTimeFunctionWithLocalDateTimeResult implements CanBeWindowingFunctionWithFrame<LocalDateTimeExpression> {

		public LocalDateTimeMaxExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getMaxFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeMaxExpression copy() {
			return new LocalDateTimeMaxExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

		@Override
		public WindowFunctionFramable<LocalDateTimeExpression> over() {
			return new WindowFunctionFramable<LocalDateTimeExpression>(new LocalDateTimeExpression(this));
		}
	}

	public static class LocalDateTimeMinExpression extends LocalDateTimeFunctionWithLocalDateTimeResult implements CanBeWindowingFunctionWithFrame<LocalDateTimeExpression> {

		public LocalDateTimeMinExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getMinFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LocalDateTimeMinExpression copy() {
			return new LocalDateTimeMinExpression((LocalDateTimeExpression) getInnerResult().copy());
		}

		@Override
		public WindowFunctionFramable<LocalDateTimeExpression> over() {
			return new WindowFunctionFramable<LocalDateTimeExpression>(new LocalDateTimeExpression(this));
		}
	}

	protected static class LocalDateTimeAddSecondsExpression extends LocalDateTimeNumberExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddSecondsExpression(LocalDateTimeExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddSecondsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddSecondsExpression copy() {
			return new LocalDateTimeAddSecondsExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddIntegerSecondsExpression extends LocalDateTimeIntegerExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddIntegerSecondsExpression(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddSecondsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddIntegerSecondsExpression copy() {
			return new LocalDateTimeAddIntegerSecondsExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddIntegerMinutesExpression extends LocalDateTimeIntegerExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddIntegerMinutesExpression(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddMinutesTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddIntegerMinutesExpression copy() {
			return new LocalDateTimeAddIntegerMinutesExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddIntegerDaysExpression extends LocalDateTimeIntegerExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddIntegerDaysExpression(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddDaysTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddIntegerDaysExpression copy() {
			return new LocalDateTimeAddIntegerDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddDaysExpression extends LocalDateTimeNumberExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddDaysExpression(LocalDateTimeExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddDaysTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddDaysExpression copy() {
			return new LocalDateTimeAddDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddIntegerHoursExpression extends LocalDateTimeIntegerExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddIntegerHoursExpression(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddHoursTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddIntegerHoursExpression copy() {
			return new LocalDateTimeAddIntegerHoursExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddIntegerWeeksExpression extends LocalDateTimeIntegerExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddIntegerWeeksExpression(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddWeeksTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddIntegerWeeksExpression copy() {
			return new LocalDateTimeAddIntegerWeeksExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddIntegerMonthsExpression extends LocalDateTimeIntegerExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddIntegerMonthsExpression(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddMonthsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddIntegerMonthsExpression copy() {
			return new LocalDateTimeAddIntegerMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddMonthsExpression extends LocalDateTimeNumberExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddMonthsExpression(LocalDateTimeExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddMonthsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddMonthsExpression copy() {
			return new LocalDateTimeAddMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeAddIntegerYearsExpression extends LocalDateTimeIntegerExpressionWithLocalDateTimeResult {

		public LocalDateTimeAddIntegerYearsExpression(LocalDateTimeExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDateAddYearsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeAddIntegerYearsExpression copy() {
			return new LocalDateTimeAddIntegerYearsExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeDaysFromExpression extends LocalDateTimeLocalDateTimeFunctionWithNumberResult {

		public LocalDateTimeDaysFromExpression(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			super(dateExp, otherLocalDateTimeExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDayDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeDaysFromExpression copy() {
			return new LocalDateTimeDaysFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeWeeksFromExpression extends LocalDateTimeLocalDateTimeFunctionWithNumberResult {

		public LocalDateTimeWeeksFromExpression(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			super(dateExp, otherLocalDateTimeExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doWeekDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeWeeksFromExpression copy() {
			return new LocalDateTimeWeeksFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeMonthsFromExpression extends LocalDateTimeLocalDateTimeFunctionWithNumberResult {

		public LocalDateTimeMonthsFromExpression(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			super(dateExp, otherLocalDateTimeExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMonthDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeMonthsFromExpression copy() {
			return new LocalDateTimeMonthsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeYearsFromExpression extends LocalDateTimeLocalDateTimeFunctionWithNumberResult {

		public LocalDateTimeYearsFromExpression(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			super(dateExp, otherLocalDateTimeExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doYearDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeYearsFromExpression copy() {
			return new LocalDateTimeYearsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeHoursFromExpression extends LocalDateTimeLocalDateTimeFunctionWithNumberResult {

		public LocalDateTimeHoursFromExpression(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			super(dateExp, otherLocalDateTimeExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doHourDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeHoursFromExpression copy() {
			return new LocalDateTimeHoursFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeMinutesFromExpression extends LocalDateTimeLocalDateTimeFunctionWithNumberResult {

		public LocalDateTimeMinutesFromExpression(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			super(dateExp, otherLocalDateTimeExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMinuteDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeMinutesFromExpression copy() {
			return new LocalDateTimeMinutesFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeSecondsFromExpression extends LocalDateTimeLocalDateTimeFunctionWithNumberResult {

		public LocalDateTimeSecondsFromExpression(LocalDateTimeExpression dateExp, LocalDateTimeResult otherLocalDateTimeExp) {
			super(dateExp, otherLocalDateTimeExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSecondDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public LocalDateTimeSecondsFromExpression copy() {
			return new LocalDateTimeSecondsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class LocalDateTimeEndOfMonthExpression extends LocalDateTimeExpression {

		public LocalDateTimeEndOfMonthExpression(LocalDateTimeResult dateVariable) {
			super(dateVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			try {
				return db.doEndOfMonthTransform(this.getInnerResult().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				LocalDateTimeExpression only = (LocalDateTimeExpression) getInnerResult();
				return only
						.setDay(1)
						.addMonths(1).addDays(-1)
						.toSQLString(db);
			}
		}

		@Override
		public LocalDateTimeEndOfMonthExpression copy() {
			return new LocalDateTimeEndOfMonthExpression((LocalDateTimeResult) getInnerResult().copy());
		}
	}

	protected static class LocalDateTimeDayOfWeekExpression extends LocalDateTimeExpressionWithNumberResult {

		public LocalDateTimeDayOfWeekExpression(LocalDateTimeExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDayOfWeekTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public LocalDateTimeDayOfWeekExpression copy() {
			return new LocalDateTimeDayOfWeekExpression((LocalDateTimeExpression) getInnerResult().copy());
		}
	}

	protected static class LocalDateTimeLeastOfExpression extends LocalDateTimeArrayFunctionWithLocalDateTimeResult {

		public LocalDateTimeLeastOfExpression(LocalDateTimeResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (LocalDateTimeResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doLeastOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getLeastOfFunctionName();
		}

		@Override
		public boolean getIncludesNull() {
			return true;
		}

		@Override
		public LocalDateTimeLeastOfExpression copy() {
			List<LocalDateTimeResult> newValues = new ArrayList<>();
			for (LocalDateTimeResult value : values) {
				newValues.add(value.copy());
			}
			return new LocalDateTimeLeastOfExpression(newValues.toArray(new LocalDateTimeResult[]{}));
		}
	}

	protected static class LocalDateTimeGreatestOfExpression extends LocalDateTimeArrayFunctionWithLocalDateTimeResult {

		public LocalDateTimeGreatestOfExpression(LocalDateTimeResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (LocalDateTimeResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doGreatestOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getGreatestOfFunctionName();
		}

		@Override
		public LocalDateTimeGreatestOfExpression copy() {
			List<LocalDateTimeResult> newValues = new ArrayList<>();
			for (LocalDateTimeResult value : values) {
				newValues.add(value.copy());
			}
			return new LocalDateTimeGreatestOfExpression(newValues.toArray(new LocalDateTimeResult[]{}));
		}
	}

	public static WindowFunctionFramable<LocalDateTimeExpression> firstValue() {
		return new FirstValueExpression().over();
	}

	public static class FirstValueExpression extends BooleanExpression implements CanBeWindowingFunctionWithFrame<LocalDateTimeExpression> {

		public FirstValueExpression() {
			super();
		}

		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getFirstValueFunctionName() + "()";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		@SuppressWarnings("unchecked")
		public FirstValueExpression copy() {
			return new FirstValueExpression();
		}

		@Override
		public WindowFunctionFramable<LocalDateTimeExpression> over() {
			return new WindowFunctionFramable<LocalDateTimeExpression>(new LocalDateTimeExpression(this));
		}

	}

	public static WindowFunctionFramable<LocalDateTimeExpression> lastValue() {
		return new LastValueExpression().over();
	}

	public static class LastValueExpression extends LocalDateTimeExpression implements CanBeWindowingFunctionWithFrame<LocalDateTimeExpression> {

		public LastValueExpression() {
			super();
		}

		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getLastValueFunctionName() + "()";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		@SuppressWarnings("unchecked")
		public LocalDateTimeExpression copy() {
			return new LastValueExpression();
		}

		@Override
		public WindowFunctionFramable<LocalDateTimeExpression> over() {
			return new WindowFunctionFramable<LocalDateTimeExpression>(new LocalDateTimeExpression(this));
		}

	}

	public static WindowFunctionFramable<LocalDateTimeExpression> nthValue(IntegerExpression indexExpression) {
		return new NthValueExpression(indexExpression).over();
	}

	public static class NthValueExpression extends LocalDateTimeExpression implements CanBeWindowingFunctionWithFrame<LocalDateTimeExpression> {

		public NthValueExpression(IntegerExpression only) {
			super(only);
		}

		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNthValueFunctionName() + "(" + getInnerResult().toSQLString(db) + ")";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		@SuppressWarnings("unchecked")
		public NthValueExpression copy() {
			return new NthValueExpression(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}

		@Override
		public WindowFunctionFramable<LocalDateTimeExpression> over() {
			return new WindowFunctionFramable<LocalDateTimeExpression>(new LocalDateTimeExpression(this));
		}

	}

	/**
	 * previousRowValue is a synonym for LAG.
	 *
	 * <p>
	 * LAG() is a window function that provides access to a row at a specified
	 * physical offset which comes before the current row.</p>
	 *
	 * <p>
	 * The function will "look" back one row and return the value there. If there
	 * is no previous row NULL will be returned.</p>
	 *
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> previousRowValue() {
		return lag();
	}

	/**
	 * LAG() is a window function that provides access to a row at a specified
	 * physical offset which comes before the current row.
	 *
	 * <p>
	 * The function will "look" back one row and return the value there. If there
	 * is no previous row NULL will be returned.</p>
	 *
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lag() {
		return lag(IntegerExpression.value(1));
	}

	/**
	 * LAG() is a window function that provides access to a row at a specified
	 * physical offset which comes before the current row.
	 *
	 * <p>
	 * When there is no row at the offset NULL will be returned.</p>
	 *
	 * @param offset the number of rows to look backwards
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lag(IntegerExpression offset) {
		return lag(offset, LocalDateTimeExpression.nullLocalDateTime());
	}

	/**
	 * LAG() is a window function that provides access to a row at a specified
	 * physical offset which comes before the current row.
	 *
	 * @param offset the number of rows to look backwards
	 * @param defaultExpression the expression to return when there is no row at
	 * the offset
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lag(IntegerExpression offset, LocalDateTimeExpression defaultExpression) {
		return new LagExpression(this, offset, defaultExpression).over();
	}

	/**
	 * LAG() is a window function that provides access to a row at a specified
	 * physical offset which comes before the current row.
	 *
	 * @param offset the number of rows to look backwards
	 * @param defaultExpression the expression to return when there is no row at
	 * the offset
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lag(Integer offset, LocalDateTime defaultExpression) {
		return lag(IntegerExpression.value(offset), LocalDateTimeExpression.value(defaultExpression));
	}

	/**
	 * nextRowValue is a synonym for LEAD.
	 *
	 * <p>
	 * LEAD() is a window function that provides access to a row at a specified
	 * physical offset which comes after the current row.</p>
	 *
	 * <p>
	 * The function will "look" forward one row and return the value there. If
	 * there is no next row NULL will be returned.</p>
	 *
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> nextRowValue() {
		return lead();
	}

	/**
	 * LAG() is a window function that provides access to a row at a specified
	 * physical offset which comes before the current row.
	 *
	 * @param offset the number of rows to look backwards
	 * @param defaultExpression the expression to return when there is no row at
	 * the offset
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lag(Long offset, LocalDateTime defaultExpression) {
		return lag(IntegerExpression.value(offset), LocalDateTimeExpression.value(defaultExpression));
	}

	/**
	 * LEAD() is a window function that provides access to a row at a specified
	 * physical offset which comes after the current row.
	 *
	 * <p>
	 * The function will "look" forward one row and return the value there. If
	 * there is no next row NULL will be returned.</p>
	 *
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lead() {
		return lead(value(1));
	}

	/**
	 * LEAD() is a window function that provides access to a row at a specified
	 * physical offset which comes after the current row.
	 *
	 * <p>
	 * When there is no row at the offset NULL will be returned.</p>
	 *
	 * @param offset the number of rows to look backwards
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lead(IntegerExpression offset) {
		return lead(offset, nullLocalDateTime());
	}

	/**
	 * LEAD() is a window function that provides access to a row at a specified
	 * physical offset which comes after the current row.
	 *
	 * @param offset the number of rows to look forwards
	 * @param defaultExpression the expression to use when there is no row at the
	 * offset
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> lead(IntegerExpression offset, LocalDateTimeExpression defaultExpression) {
		return new LeadExpression(this, offset, defaultExpression).over();
	}

	private static abstract class LagLeadFunction extends LocalDateTimeExpression implements CanBeWindowingFunctionRequiresOrderBy<LocalDateTimeExpression> {

		private static final long serialVersionUID = 1L;

		protected LocalDateTimeExpression first;
		protected IntegerExpression second;
		protected LocalDateTimeExpression third;

		LagLeadFunction(LocalDateTimeExpression first, IntegerExpression second, LocalDateTimeExpression third) {
			this.first = (first == null ? nullExpression() : first);
			this.second = (second == null ? IntegerExpression.value(1) : second);
			this.third = (third == null ? nullLocalDateTime() : third);
		}

		@Override
		public DBLocalDateTime getQueryableDatatypeForExpressionValue() {
			return new DBLocalDateTime();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return SeparatedStringBuilder
					.forSeparator(getSeparator(db))
					.withPrefix(beforeValue(db))
					.containing(
							first.toSQLString(db),
							second.toSQLString(db),
							third.toSQLString(db)
					).withSuffix(afterValue(db))
					.toString();
//			return this.beforeValue(db) + getFirst().toSQLString(db) + this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db)) + this.afterValue(db);
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return " " + getFunctionName(db) + "( ";
		}

		protected String getSeparator(DBDefinition db) {
			return ", ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			hashSet.addAll(getFirst().getTablesInvolved());
			hashSet.addAll(getSecond().getTablesInvolved());
			hashSet.addAll(getThird().getTablesInvolved());
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator() || getSecond().isAggregator() || getThird().isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		protected LocalDateTimeExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected IntegerExpression getSecond() {
			return second;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected LocalDateTimeExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
		}

		@Override
		public WindowFunctionRequiresOrderBy<LocalDateTimeExpression> over() {
			final LocalDateTimeExpression localDateTimeExpression = new LocalDateTimeExpression(this);
			return new WindowFunctionRequiresOrderBy<>(localDateTimeExpression);
		}
	}

	public class LagExpression extends LagLeadFunction {

		private static final long serialVersionUID = 1L;

		public LagExpression(LocalDateTimeExpression first, IntegerExpression second, LocalDateTimeExpression third) {
			super(first, second, third);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getLagFunctionName();
		}

		@Override
		public LagExpression copy() {
			return new LagExpression(getFirst(), getSecond(), getThird());
		}
	}

	public class LeadExpression extends LagLeadFunction {

		private static final long serialVersionUID = 1L;

		public LeadExpression(LocalDateTimeExpression first, IntegerExpression second, LocalDateTimeExpression third) {
			super(first, second, third);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getLeadFunctionName();
		}

		@Override
		public LeadExpression copy() {
			return new LeadExpression(getFirst(), getSecond(), getThird());
		}
	}
}
