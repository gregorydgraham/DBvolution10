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
import java.time.Instant;
import java.time.Month;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.results.NumberResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.InstantColumn;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.DateRepeatResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.InstantResult;
import org.joda.time.Period;

/**
 * InstantExpression implements standard functions that produce a Instant or
 * Time result.
 *
 * <p>
 * Instant and Time are considered synonymous with timestamp as that appears to
 * be the standard usage by developers. So every date has a time component and
 * every time has a date component. {@link DBLocalDate} implements a time-less
 * date for DBvolution but is considered a DBLocalDateTime with a time of
 * Midnight (the beginning of the day) for LocalDateTimeExpression purposes.
 *
 * <p>
 * Most query requirements are provided by {@link QueryableDatatype}s like
 * {@link DBString} or {@link DBInteger} but expressions can provide more
 * functions or more precise control.
 *
 * <p>
 * Use a InstantExpression to produce a date from an existing column, expression
 * or value and perform date arithmetic.
 *
 * <p>
 * Generally you get a InstantExpression from a column or value using
 * {@link InstantExpression#value(java.time.Instant) } or
 * {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBInstant)}.
 *
 * @author Gregory Graham
 */
public class InstantExpression extends RangeExpression<Instant, InstantResult, DBInstant> implements InstantResult {

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

	public static InstantExpression newInstant(DateExpression dateExpression) {
		return new InstantExpression(dateExpression);
	}

	static InstantExpression currentTime() {
		return InstantExpression.now();
	}

	public static InstantExpression currentDate() {
		return InstantExpression.now();
	}

	public static InstantExpression currentDateOnly() {
		return InstantExpression.currentInstantDateOnly();
	}

	/**
	 * Default Constructor
	 */
	protected InstantExpression() {
		super();
	}

	/**
	 * Create a InstantExpression based on an existing {@link InstantResult}.
	 *
	 * <p>
	 * {@link InstantResult} is generally a InstantExpression but it may also be a
	 * {@link DBInstant} or {@link DBInstant}.
	 *
	 * @param dateVariable a date expression or QueryableDatatype
	 */
	public InstantExpression(InstantResult dateVariable) {
		super(dateVariable);
	}

	/**
	 * Create a InstantExpression based on an existing {@link InstantResult}.
	 *
	 * <p>
	 * {@link InstantResult} is generally a InstantExpression but it may also be a
	 * {@link DBInstant} or {@link InstantColumn}.
	 *
	 * @param variable a date expression or QueryableDatatype
	 */
	protected InstantExpression(AnyResult<?> variable) {
		super(variable);
	}

	/**
	 * Create a InstantExpression based on an existing Instant.
	 *
	 * <p>
	 * This performs a similar function to {@link InstantExpression#value(java.time.Instant)
	 * }.
	 *
	 * @param date the date to be used in this expression
	 */
	public InstantExpression(Instant date) {
		super(new DBInstant(date));
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return getInnerResult().toSQLString(db);
	}

	@Override
	public InstantExpression copy() {
		return isNullSafetyTerminator() ? nullInstant() : new InstantExpression((AnyResult<?>) this.getInnerResult().copy());
	}

	@Override
	public InstantExpression nullExpression() {
		return new InstantNullExpression();
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
	public static InstantExpression currentInstantDateOnly() {
		return new InstantExpression(
				new CurrentInstantDateOnlyExpression());
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
	public static InstantExpression currentInstant() {
		return new CurrentInstantExpression();
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
	public static InstantExpression now() {
		return InstantExpression.currentInstant();
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
		return new InstantYearExpression(this);
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
		return new InstantMonthExpression(this);
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
		return new InstantDayExpression(this).integerResult();
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
		return new InstantHourExpression(this).integerResult();
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
		return (new InstantMinuteExpression(this)).integerResult();
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
		return new InstantSecondExpression(this);
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
				new InstantSubsecondExpression(this));
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
	 * @return a BooleanExpression comparing the date and this InstantExpression.
	 */
	@Override
	public BooleanExpression is(Instant date) {
		return is(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * @param dateExpression the date the expression must match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the InstantResult and this
	 * InstantExpression.
	 */
	@Override
	public BooleanExpression is(InstantResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new InstantIsExpression(this, dateExpression));
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
	 * @return a BooleanExpression comparing the InstantResult and this
	 * InstantExpression.
	 */
	@Override
	public BooleanExpression isNot(Instant date) {
		return this.isNot(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is NOT
	 * equal to the supplied date.
	 *
	 * @param dateExpression the date the expression must not match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the InstantResult and this
	 * InstantExpression.
	 */
	@Override
	public BooleanExpression isNot(InstantResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new InstantIsNotExpression(this, dateExpression));
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
	public BooleanExpression isBetween(InstantResult lowerBound, InstantResult upperBound) {
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
	public BooleanExpression isBetween(Instant lowerBound, InstantResult upperBound) {
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
	public BooleanExpression isBetween(InstantResult lowerBound, Instant upperBound) {
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
	public BooleanExpression isBetween(Instant lowerBound, Instant upperBound) {
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
	public BooleanExpression isBetweenInclusive(InstantResult lowerBound, InstantResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(Instant lowerBound, InstantResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(InstantResult lowerBound, Instant upperBound) {
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
	public BooleanExpression isBetweenInclusive(Instant lowerBound, Instant upperBound) {
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
	public BooleanExpression isBetweenExclusive(InstantResult lowerBound, InstantResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(Instant lowerBound, InstantResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(InstantResult lowerBound, Instant upperBound) {
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
	public BooleanExpression isBetweenExclusive(Instant lowerBound, Instant upperBound) {
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
	public BooleanExpression isLessThan(Instant date) {
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
	public BooleanExpression isLessThan(InstantResult dateExpression) {
		return new BooleanExpression(new InstantIsLessThanExpression(this, dateExpression));
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
	public BooleanExpression isEarlierThan(Instant date) {
		return isEarlierThan(new InstantExpression(date));
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
	public BooleanExpression isEarlierThan(InstantResult dateExpression) {
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
	public DateRepeatExpression getDateRepeatFrom(Instant date) {
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
	public DateRepeatExpression getDateRepeatFrom(InstantResult dateExpression) {
		return new DateRepeatExpression(new InstantGetDateRepeatFromExpression(this, dateExpression));
	}

	/**
	 * Subtract the period/duration provided from this date expression to get an
	 * offset date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Instant expression
	 */
	public InstantExpression minus(Period interval) {
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
	 * @return a Instant expression
	 */
	public InstantExpression minus(DateRepeatResult intervalExpression) {
		return new InstantExpression(new InstantMinusDateRepeatExpression(this, intervalExpression));
	}

	/**
	 * Add the period/duration provided from this date expression to get an offset
	 * date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Instant expression
	 */
	public InstantExpression plus(Period interval) {
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
	 * @return a Instant expression
	 */
	public InstantExpression plus(DateRepeatResult intervalExpression) {
		return new InstantExpression(new InstantPlusDateRepeatExpression(this, intervalExpression));
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
	public BooleanExpression isLessThanOrEqual(Instant date) {
		return super.isLessThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied InstantResult.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(InstantResult dateExpression) {
		return new BooleanExpression(new InstantIsLessThanOrEqualExpression(this, dateExpression));
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
	public BooleanExpression isGreaterThan(Instant date) {
		return super.isGreaterThan(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied InstantResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThan(InstantResult dateExpression) {
		return new BooleanExpression(new InstantIsGreaterThanExpression(this, dateExpression));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied date.
	 *
	 * @param date the date this expression must be compared to
	 * @return an expression that will evaluate to a greater than operation
	 */
	public BooleanExpression isLaterThan(Instant date) {
		return isGreaterThan(new InstantExpression(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied InstantResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLaterThan(InstantResult dateExpression) {
		return isGreaterThan(dateExpression);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied Instant.
	 *
	 * @param date the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(Instant date) {
		return super.isGreaterThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied InstantResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(InstantResult dateExpression) {
		return new BooleanExpression(new InstantInstantIsGreaterThanOrEqualExpression(this, dateExpression));
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
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(Instant value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isGreaterThan(Instant value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isLessThan(InstantResult value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isGreaterThan(InstantResult value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of Instants.
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
//	public BooleanExpression isIn(Instant... possibleValues) {
//		List<InstantExpression> possVals = new ArrayList<InstantExpression>();
//		for (Instant num : possibleValues) {
//			possVals.add(value(num));
//		}
//		return isIn(possVals.toArray(new InstantExpression[]{}));
//	}
	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of Instants.
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
	public BooleanExpression isIn(Collection<? extends InstantResult> possibleValues) {
		return isIn(possibleValues.toArray(new InstantResult[]{}));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of InstantResults.
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
	public BooleanExpression isInCollection(Collection<InstantResult> possibleValues) {
		BooleanExpression isInExpr = new BooleanExpression(new InstantIsInExpression(this, possibleValues));
		if (isInExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isInExpr);
		} else {
			return isInExpr;
		}
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of InstantResults.
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
	public BooleanExpression isNotInCollection(Collection<InstantResult> possibleValues) {
		BooleanExpression isNotInExpr = new BooleanExpression(new InstantIsNotInExpression(this, possibleValues));
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
	public InstantExpression ifDBNull(Instant alternative) {
		return ifDBNull(value(alternative));
	}

	/**
	 * Creates and expression that replaces a NULL result with the supplied
	 * InstantResult.
	 *
	 * <p>
	 * This is a way of handling dates that should have a value but don't.
	 *
	 * @param alternative use this value if the expression evaluates to NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public InstantExpression ifDBNull(InstantResult alternative) {
		return new InstantExpression(
				new InstantIfDBNullExpression(this, alternative));
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
	public InstantMaxExpression max() {
		return new InstantMaxExpression(this);
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
	public InstantMinExpression min() {
		return new InstantMinExpression(this);
	}

	@Override
	public DBInstant getQueryableDatatypeForExpressionValue() {
		return new DBInstant();
	}

	/**
	 * Instant Arithmetic: add the supplied number of seconds to the date
	 * expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addSeconds(int secondsToAdd) {
		return this.addSeconds(value(secondsToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of seconds to the date
	 * expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addSeconds(NumberExpression secondsToAdd) {
		return new InstantExpression(
				new AddSecondsExpression(this, secondsToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of seconds to the date
	 * expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addSeconds(IntegerExpression secondsToAdd) {
		return new InstantExpression(
				new AddIntegerSecondsExpression(this, secondsToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of minutes to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addMinutes(int minutesToAdd) {
		return this.addMinutes(value(minutesToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of minutes to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addMinutes(IntegerExpression minutesToAdd) {
		return new InstantExpression(
				new AddIntegerMinutesExpression(this, minutesToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addDays(Integer daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addDays(Long daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addDays(Number daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addDays(IntegerExpression daysToAdd) {
		return new InstantExpression(
				new AddIntegerDaysExpression(this, daysToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addDays(NumberExpression daysToAdd) {
		return new InstantExpression(
				new AddDaysExpression(this, daysToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of hours to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addHours(int hoursToAdd) {
		return this.addHours(value(hoursToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of hours to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addHours(IntegerExpression hoursToAdd) {
		return new InstantExpression(
				new AddIntegerHoursExpression(this, hoursToAdd));
	}

	/**
	 * Instant Arithmetic: add the supplied number of weeks to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addWeeks(int weeksToAdd) {
		return this.addWeeks(value(weeksToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of weeks to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addWeeks(IntegerExpression weeksToAdd) {
		return new InstantExpression(
				new AddIntegerWeeksExpression(this, weeksToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addMonths(Number monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addMonths(Integer monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addMonths(Long monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addMonths(IntegerExpression monthsToAdd) {
		return new InstantExpression(
				new AddIntegerMonthsExpression(this, monthsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addMonths(NumberExpression monthsToAdd) {
		return new InstantExpression(
				new AddMonthsExpression(this, monthsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of years to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addYears(int yearsToAdd) {
		return this.addYears(value(yearsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of years to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a InstantExpression
	 */
	public InstantExpression addYears(IntegerExpression yearsToAdd) {
		return new InstantExpression(
				new AddIntegerYearsExpression(this, yearsToAdd));
	}

	/**
	 * Date Arithmetic: get the days between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression daysFrom(Instant dateToCompareTo) {
		return daysFrom(InstantExpression.value(dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the days between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression daysFrom(InstantResult dateToCompareTo) {
		return new NumberExpression(
				new DaysFromExpression(this, dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the weeks between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression weeksFrom(Instant dateToCompareTo) {
		return weeksFrom(InstantExpression.value(dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the weeks between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression weeksFrom(InstantExpression dateToCompareTo) {
		return new NumberExpression(
				new WeeksFromExpression(this, dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the months between the date expression and the
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
	public NumberExpression monthsFrom(Instant dateToCompareTo) {
		return monthsFrom(InstantExpression.value(dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the months between the date expression and the
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
	public NumberExpression monthsFrom(InstantResult dateToCompareTo) {
		return new NumberExpression(
				new MonthsFromExpression(this, dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the years between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression yearsFrom(Instant dateToCompareTo) {
		return yearsFrom(InstantExpression.value(dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the years between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression yearsFrom(InstantResult dateToCompareTo) {
		return new NumberExpression(
				new YearsFromExpression(this, dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the Hours between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression hoursFrom(Instant dateToCompareTo) {
		return hoursFrom(InstantExpression.value(dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the Hours between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression hoursFrom(InstantResult dateToCompareTo) {
		return new NumberExpression(
				new HoursFromExpression(this, dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the minutes between the date expression and the
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
	public NumberExpression minutesFrom(Instant dateToCompareTo) {
		return minutesFrom(InstantExpression.value(dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the days between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression minutesFrom(InstantResult dateToCompareTo) {
		return new NumberExpression(
				new MinutesFromExpression(this, dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the seconds between the date expression and the
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
	public NumberExpression secondsFrom(Instant dateToCompareTo) {
		return secondsFrom(InstantExpression.value(dateToCompareTo));
	}

	/**
	 * Date Arithmetic: get the days between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression secondsFrom(InstantResult dateToCompareTo) {
		return new NumberExpression(
				new SecondsFromExpression(this, dateToCompareTo));
	}

	/**
	 * Derive the first day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Instant expression
	 */
	public InstantExpression atStartOfDay() {
		return this.setHour(0).setMinute(0).setSecond(0);
	}

	/**
	 * Derive the first day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Instant expression
	 */
	public InstantExpression atStartOfMonth() {
		return this.setDay(1).atStartOfDay();
	}

	/**
	 * Derive the last day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Instant expression
	 */
	public InstantExpression atEndOfMonth() {
		return new InstantExpression(
				new EndOfMonthExpression(this)
		);
	}

	/**
	 * Derive the first day of the year for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Instant expression
	 */
	public InstantExpression atStartOfYear() {
		return this.setMonth(1).atStartOfMonth();
	}

	/**
	 * Derive the last day of the year for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Instant expression
	 */
	public InstantExpression atEndOfYear() {
		return this.addYears(1).atStartOfYear().addDays(-1).atStartOfDay();
	}

	/**
	 * Derive the first day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Instant expression
	 */
	public InstantExpression firstOfMonth() {
		return this.setDay(1);//.atStartOfDay();
	}

	/**
	 * Derive the last day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Instant expression
	 */
	public InstantExpression endOfMonth() {
		return new InstantExpression(
				new EndOfMonthExpression(this)
		);
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
				new DayOfWeekExpression(this));
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
	public static BooleanExpression overlaps(Instant firstStartTime, Instant firstEndTime, Instant secondStartTime, Instant secondEndtime) {
		return InstantExpression.overlaps(InstantExpression.value(firstStartTime), InstantExpression.value(firstEndTime),
				InstantExpression.value(secondStartTime), InstantExpression.value(secondEndtime)
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
	public static BooleanExpression overlaps(InstantResult firstStartTime, InstantResult firstEndTime, InstantResult secondStartTime, InstantResult secondEndtime) {
		return InstantExpression.overlaps(new InstantExpression(firstStartTime), new InstantExpression(firstEndTime),
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
	public static BooleanExpression overlaps(InstantExpression firstStartTime, InstantExpression firstEndTime, InstantResult secondStartTime, InstantResult secondEndtime) {
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
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static InstantExpression leastOf(Instant... possibleValues) {
		ArrayList<InstantExpression> possVals = new ArrayList<InstantExpression>();
		for (Instant num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new InstantExpression[]{}));
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
	public static InstantExpression leastOf(Collection<? extends InstantResult> possibleValues) {
		ArrayList<InstantExpression> possVals = new ArrayList<InstantExpression>();
		for (InstantResult num : possibleValues) {
			possVals.add(new InstantExpression(num));
		}
		return leastOf(possVals.toArray(new InstantExpression[]{}));
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
	public static InstantExpression leastOf(InstantResult... possibleValues) {
		InstantExpression leastExpr
				= new InstantExpression(new LeastOfExpression(possibleValues));
		return leastExpr;
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
	public static InstantExpression greatestOf(Instant... possibleValues) {
		ArrayList<InstantExpression> possVals = new ArrayList<InstantExpression>();
		for (Instant num : possibleValues) {
			possVals.add(value(num));
		}
		return greatestOf(possVals.toArray(new InstantExpression[]{}));
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
	public static InstantExpression greatestOf(Collection<? extends InstantResult> possibleValues) {
		ArrayList<InstantExpression> possVals = new ArrayList<InstantExpression>();
		for (InstantResult num : possibleValues) {
			possVals.add(new InstantExpression(num));
		}
		return greatestOf(possVals.toArray(new InstantExpression[]{}));
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
	public static InstantExpression greatestOf(InstantResult... possibleValues) {
		InstantExpression greatestOf
				= new InstantExpression(new GreatestOfExpression(possibleValues));
		return greatestOf;
	}

	@Override
	public DBInstant asExpressionColumn() {
		return new DBInstant(this);
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
		StringExpression isoFormatDateTime = value("")
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
		return isoFormatDateTime;
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
		StringExpression usaFormatDateTime = value("")
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
		return usaFormatDateTime;
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
		StringExpression commonFormatDateTime = value("")
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
		return commonFormatDateTime;
	}

	@Override
	public InstantExpression expression(Instant value) {
		return new InstantExpression(value);
	}

	@Override
	public InstantExpression expression(InstantResult value) {
		return new InstantExpression(value);
	}

	@Override
	public InstantResult expression(DBInstant value) {
		return new InstantExpression(value);
	}

	public InstantExpression toInstant() {
		return new InstantExpression(this);
	}

	public InstantExpression setYear(int i) {
		return this.addYears(IntegerExpression.value(i).minus(this.year().integerResult()));
	}

	public InstantExpression setMonth(Month month) {
		return setMonth(month.getValue());
	}

	public InstantExpression setMonth(int i) {
		return this.addMonths(IntegerExpression.value(i).minus(this.month().integerResult()));
	}

	public InstantExpression setDay(int i) {
		return this.addDays(IntegerExpression.value(i).minus(this.day().integerResult()));
	}

	public InstantExpression setHour(int i) {
		return this.addHours(IntegerExpression.value(i).minus(this.hour().integerResult()));
	}

	public InstantExpression setMinute(int i) {
		return this.addMinutes(IntegerExpression.value(i).minus(this.minute().integerResult()));
	}

	public InstantExpression setSecond(int i) {
		return this.addSeconds(IntegerExpression.value(i).minus(this.second().integerResult()));
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
	private NumberExpression secondAndSubsecond() {
		return new InstantSecondAndSubsecondExpression(this);
	}

	private static abstract class FunctionWithInstantResult extends InstantExpression implements CanBeWindowingFunctionWithFrame<InstantExpression> {

		private static final long serialVersionUID = 1L;

		FunctionWithInstantResult() {
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
		public InstantExpression.FunctionWithInstantResult copy() {
			InstantExpression.FunctionWithInstantResult newInstance;
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
		public WindowFunctionFramable<InstantExpression> over() {
			return new WindowFunctionFramable<InstantExpression>(new InstantExpression(this));
		}
	}

	private static abstract class InstantExpressionWithIntegerResult extends IntegerExpression {

		private static final long serialVersionUID = 1L;

		InstantExpressionWithIntegerResult() {
			super();
		}

		InstantExpressionWithIntegerResult(InstantExpression only) {
			super(only);
		}

		@Override
		public DBInteger getQueryableDatatypeForExpressionValue() {
			return new DBInteger();
		}

		@Override
		public abstract String toSQLString(DBDefinition db);
	}

	private static abstract class InstantExpressionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		InstantExpressionWithNumberResult() {
			super();
		}

		InstantExpressionWithNumberResult(InstantExpression only) {
			super(only);
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public abstract String toSQLString(DBDefinition db);
	}

	private static abstract class InstantInstantExpressionWithBooleanResult extends BooleanExpression implements CanBeWindowingFunctionWithFrame<BooleanExpression> {

		private static final long serialVersionUID = 1L;

		protected InstantExpression first;
		protected InstantExpression second;
		private boolean requiresNullProtection = false;

		InstantInstantExpressionWithBooleanResult(InstantExpression first, InstantResult second) {
			this.first = first;
			this.second = new InstantExpression(second);
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
		public InstantInstantExpressionWithBooleanResult copy() {
			InstantInstantExpressionWithBooleanResult newInstance;
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

	private static abstract class InstantInstantExpressionWithDateRepeatResult extends DateRepeatExpression implements CanBeWindowingFunctionWithFrame<DateRepeatExpression> {

		private static final long serialVersionUID = 1L;

		protected InstantExpression first;
		protected InstantExpression second;
		private boolean requiresNullProtection = false;

		InstantInstantExpressionWithDateRepeatResult(InstantExpression first, InstantResult second) {
			this.first = first;
			this.second = new InstantExpression(second);
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public InstantInstantExpressionWithDateRepeatResult copy() {
			InstantInstantExpressionWithDateRepeatResult newInstance;
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
		 *
		 * @return the first
		 */
		public InstantExpression getFirst() {
			return first;
		}

		/**
		 *
		 * @return the second
		 */
		public InstantExpression getSecond() {
			return second;
		}

		@Override
		public WindowFunctionFramable<DateRepeatExpression> over() {
			return new WindowFunctionFramable<DateRepeatExpression>(new DateRepeatExpression(this));
		}
	}

	private static abstract class InstantDateRepeatArithmeticInstantResult extends InstantExpression {

		private static final long serialVersionUID = 1L;

		protected InstantExpression first;
		protected DateRepeatExpression second;
		private boolean requiresNullProtection = false;

		InstantDateRepeatArithmeticInstantResult(InstantExpression first, DateRepeatResult second) {
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
		public InstantDateRepeatArithmeticInstantResult copy() {
			InstantDateRepeatArithmeticInstantResult newInstance;
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
		public InstantExpression getFirst() {
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

	private static abstract class InstantArrayFunctionWithInstantResult extends InstantExpression {

		private static final long serialVersionUID = 1L;

		protected InstantExpression column;
		protected final List<InstantResult> values = new ArrayList<InstantResult>();
		boolean nullProtectionRequired = false;

		InstantArrayFunctionWithInstantResult() {
		}

		InstantArrayFunctionWithInstantResult(InstantResult[] rightHandSide) {
			for (InstantResult dateResult : rightHandSide) {
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
			for (InstantResult val : values) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public InstantArrayFunctionWithInstantResult copy() {
			InstantArrayFunctionWithInstantResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			for (InstantResult value : this.values) {
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
			for (InstantResult second : values) {
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
			for (InstantResult numer : values) {
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
				for (InstantResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class InstantInstantResultFunctionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private InstantExpression column;
		private List<InstantResult> values = new ArrayList<>();
		boolean nullProtectionRequired = false;

		InstantInstantResultFunctionWithBooleanResult() {
		}

		InstantInstantResultFunctionWithBooleanResult(InstantExpression leftHandSide, Collection<InstantResult> rightHandSide) {
			this.column = leftHandSide;
			for (InstantResult dateResult : rightHandSide) {
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
			for (InstantResult val : getValues()) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public InstantInstantResultFunctionWithBooleanResult copy() {
			InstantInstantResultFunctionWithBooleanResult newInstance;
			try {
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.getColumn().copy();
			for (InstantResult value : this.getValues()) {
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
			for (InstantResult val : getValues()) {
				if (val != null) {
					hashSet.addAll(val.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false || getColumn().isAggregator();
			for (InstantResult dater : getValues()) {
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
		protected InstantExpression getColumn() {
			return column;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the values
		 */
		protected List<InstantResult> getValues() {
			return values;
		}
	}

	private static abstract class InstantInstantFunctionWithInstantResult extends InstantExpression {

		private static final long serialVersionUID = 1L;

		private InstantExpression first;
		private InstantResult second;

		InstantInstantFunctionWithInstantResult(InstantExpression first) {
			this.first = first;
			this.second = null;
		}

		InstantInstantFunctionWithInstantResult(InstantExpression first, InstantResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public abstract String toSQLString(DBDefinition db);

		@Override
		public InstantInstantFunctionWithInstantResult copy() {
			InstantInstantFunctionWithInstantResult newInstance;
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
		protected InstantExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected InstantResult getSecond() {
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

	private static abstract class InstantFunctionWithInstantResult extends InstantExpression {

		private static final long serialVersionUID = 1L;

		InstantFunctionWithInstantResult() {
			super();
		}

		InstantFunctionWithInstantResult(InstantExpression only) {
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

	private static abstract class InstantIntegerExpressionWithInstantResult extends InstantExpression {

		private static final long serialVersionUID = 1L;

		protected InstantExpression first;
		protected IntegerExpression second;

		InstantIntegerExpressionWithInstantResult() {
			this.first = null;
			this.second = null;
		}

		InstantIntegerExpressionWithInstantResult(InstantExpression dateExp, IntegerExpression numbExp) {
			this.first = dateExp;
			this.second = numbExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public InstantIntegerExpressionWithInstantResult copy() {
			InstantIntegerExpressionWithInstantResult newInstance;
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

	private static abstract class InstantNumberExpressionWithInstantResult extends InstantExpression {

		private static final long serialVersionUID = 1L;

		protected InstantExpression first;
		protected NumberExpression second;

		InstantNumberExpressionWithInstantResult() {
			this.first = null;
			this.second = null;
		}

		InstantNumberExpressionWithInstantResult(InstantExpression dateExp, NumberExpression numbExp) {
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

	private static abstract class InstantInstantFunctionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		protected InstantExpression first;
		protected InstantResult second;

		InstantInstantFunctionWithNumberResult() {
			this.first = null;
			this.second = null;
		}

		InstantInstantFunctionWithNumberResult(InstantExpression dateExp, InstantResult otherExp) {
			this.first = dateExp;
			this.second = otherExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public InstantInstantFunctionWithNumberResult copy() {
			InstantInstantFunctionWithNumberResult newInstance;
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

	private static class InstantNullExpression extends InstantExpression {

		public InstantNullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public InstantNullExpression copy() {
			return new InstantNullExpression();
		}
	}

	protected static class CurrentInstantDateOnlyExpression extends FunctionWithInstantResult {

		public CurrentInstantDateOnlyExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentDateTimeTransform();
		}

		@Override
		public DBInstant getQueryableDatatypeForExpressionValue() {
			return new DBInstant();
		}

		@Override
		public CurrentInstantDateOnlyExpression copy() {
			return new CurrentInstantDateOnlyExpression();
		}

	}

	protected static class CurrentInstantExpression extends FunctionWithInstantResult {

		public CurrentInstantExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentUTCDateTimeTransform();
		}

		@Override
		public CurrentInstantExpression copy() {
			return new CurrentInstantExpression();
		}

	}

	protected static class CurrentTimeExpression extends FunctionWithInstantResult {

		public CurrentTimeExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentUTCTimeTransform();
		}

		@Override
		public CurrentTimeExpression copy() {
			return new CurrentTimeExpression();
		}

	}

	protected static class InstantYearExpression extends InstantExpressionWithIntegerResult {

		public InstantYearExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantYearTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantYearExpression copy() {
			return new InstantYearExpression((InstantExpression) getInnerResult().copy());
		}

	}

	protected static class InstantMonthExpression extends InstantExpressionWithIntegerResult {

		public InstantMonthExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantMonthTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantMonthExpression copy() {
			return new InstantMonthExpression((InstantExpression) getInnerResult().copy());
		}

	}

	protected static class InstantDayExpression extends InstantExpressionWithIntegerResult {

		public InstantDayExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantDayTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantDayExpression copy() {
			return new InstantDayExpression((InstantExpression) getInnerResult().copy());
		}

	}

	protected static class InstantHourExpression extends InstantExpressionWithIntegerResult {

		public InstantHourExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantHourTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantHourExpression copy() {
			return new InstantHourExpression((InstantExpression) getInnerResult().copy());
		}

	}

	protected static class InstantMinuteExpression extends InstantExpressionWithIntegerResult {

		public InstantMinuteExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantMinuteTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantMinuteExpression copy() {
			return new InstantMinuteExpression((InstantExpression) getInnerResult().copy());
		}

	}

	protected static class InstantSecondExpression extends InstantExpressionWithIntegerResult {

		public InstantSecondExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantSecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantSecondExpression copy() {
			return new InstantSecondExpression((InstantExpression) getInnerResult().copy());
		}
	}

	protected static class InstantSecondAndSubsecondExpression extends InstantExpressionWithNumberResult {

		public InstantSecondAndSubsecondExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSecondAndSubsecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantSecondAndSubsecondExpression copy() {
			return new InstantSecondAndSubsecondExpression((InstantExpression) getInnerResult().copy());
		}
	}

	protected static class InstantSubsecondExpression extends InstantExpressionWithNumberResult {

		public InstantSubsecondExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantSubsecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public InstantSubsecondExpression copy() {
			return new InstantSubsecondExpression((InstantExpression) getInnerResult().copy());
		}

	}

	protected static class InstantIsExpression extends InstantInstantExpressionWithBooleanResult {

		public InstantIsExpression(InstantExpression first, InstantResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " = ";
		}

		@Override
		public InstantIsExpression copy() {
			return new InstantIsExpression(first.copy(), second.copy());
		}

	}

	protected static class InstantIsNotExpression extends InstantInstantExpressionWithBooleanResult {

		public InstantIsNotExpression(InstantExpression first, InstantResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <> ";
		}

		@Override
		public InstantIsNotExpression copy() {
			return new InstantIsNotExpression(first.copy(), second.copy());
		}

	}

	protected static class InstantIsLessThanExpression extends InstantInstantExpressionWithBooleanResult {

		public InstantIsLessThanExpression(InstantExpression first, InstantResult second) {
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
		public InstantIsLessThanExpression copy() {
			return new InstantIsLessThanExpression(first.copy(), second.copy());
		}

	}

	protected static class InstantGetDateRepeatFromExpression extends InstantInstantExpressionWithDateRepeatResult {

		public InstantGetDateRepeatFromExpression(InstantExpression first, InstantResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateMinusToDateRepeatTransformation(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final InstantExpression left = getFirst();
				final InstantExpression right = new InstantExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullString(),
								StringExpression.value(INTERVAL_PREFIX)
										.append(left.year().minus(right.year()).bracket()).append(YEAR_SUFFIX)
										.append(left.month().minus(right.month()).bracket()).append(MONTH_SUFFIX)
										.append(left.day().minus(right.day()).bracket()).append(DAY_SUFFIX)
										.append(left.hour().minus(right.hour()).bracket()).append(HOUR_SUFFIX)
										.append(left.minute().minus(right.minute()).bracket()).append(MINUTE_SUFFIX)
										.append(left.secondAndSubsecond().minus(right.secondAndSubsecond()).bracket().formatAsDateRepeatSeconds())
										.append(SECOND_SUFFIX)
						).toSQLString(db);
			}
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public InstantGetDateRepeatFromExpression copy() {
			return new InstantGetDateRepeatFromExpression(first.copy(), second.copy());
		}
	}

	protected static class InstantMinusDateRepeatExpression extends InstantDateRepeatArithmeticInstantResult {

		public InstantMinusDateRepeatExpression(InstantExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateMinusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final InstantExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullInstant(),
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
		public InstantMinusDateRepeatExpression copy() {
			return new InstantMinusDateRepeatExpression(first.copy(), second.copy());
		}
	}

	protected static class InstantPlusDateRepeatExpression extends InstantDateRepeatArithmeticInstantResult {

		public InstantPlusDateRepeatExpression(InstantExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDatePlusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final InstantExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullInstant(),
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
		public InstantPlusDateRepeatExpression copy() {
			return new InstantPlusDateRepeatExpression(getFirst().copy(), getSecond().copy());
		}

	}

	protected static class InstantIsLessThanOrEqualExpression extends InstantInstantExpressionWithBooleanResult {

		public InstantIsLessThanOrEqualExpression(InstantExpression first, InstantResult second) {
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
		public InstantIsLessThanOrEqualExpression copy() {
			return new InstantIsLessThanOrEqualExpression(first.copy(), second.copy());
		}

	}

	protected static class InstantIsGreaterThanExpression extends InstantInstantExpressionWithBooleanResult {

		public InstantIsGreaterThanExpression(InstantExpression first, InstantResult second) {
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
		public InstantIsGreaterThanExpression copy() {
			return new InstantIsGreaterThanExpression(first.copy(), second.copy());
		}

	}

	protected static class InstantInstantIsGreaterThanOrEqualExpression extends InstantInstantExpressionWithBooleanResult {

		public InstantInstantIsGreaterThanOrEqualExpression(InstantExpression first, InstantResult second) {
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
		public InstantInstantIsGreaterThanOrEqualExpression copy() {
			return new InstantInstantIsGreaterThanOrEqualExpression(first.copy(), second.copy());
		}
	}

	protected class InstantIsInExpression extends InstantInstantResultFunctionWithBooleanResult {

		public InstantIsInExpression(InstantExpression leftHandSide, Collection<InstantResult> rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<String>();
			for (InstantResult value : getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		public InstantIsInExpression copy() {
			return new InstantIsInExpression(getColumn().copy(), getValues());
		}

	}

	protected class InstantIsNotInExpression extends InstantInstantResultFunctionWithBooleanResult {

		public InstantIsNotInExpression(InstantExpression leftHandSide, Collection<InstantResult> rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<String>();
			for (InstantResult value : getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doNotInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		public InstantIsNotInExpression copy() {
			return new InstantIsNotInExpression(getColumn().copy(), getValues());
		}

	}

	protected static class InstantIfDBNullExpression extends InstantInstantFunctionWithInstantResult {

		public InstantIfDBNullExpression(InstantExpression first, InstantResult second) {
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
		public InstantIfDBNullExpression copy() {
			return new InstantIfDBNullExpression(getFirst().copy(), getSecond().copy());
		}
	}

	public static class InstantMaxExpression extends InstantFunctionWithInstantResult implements CanBeWindowingFunctionWithFrame<InstantExpression> {

		public InstantMaxExpression(InstantExpression only) {
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
		public InstantMaxExpression copy() {
			return new InstantMaxExpression((InstantExpression) getInnerResult().copy());
		}

		@Override
		public WindowFunctionFramable<InstantExpression> over() {
			return new WindowFunctionFramable<InstantExpression>(new InstantExpression(this));
		}
	}

	public static class InstantMinExpression extends InstantFunctionWithInstantResult implements CanBeWindowingFunctionWithFrame<InstantExpression> {

		public InstantMinExpression(InstantExpression only) {
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
		public InstantMinExpression copy() {
			return new InstantMinExpression((InstantExpression) getInnerResult().copy());
		}

		@Override
		public WindowFunctionFramable<InstantExpression> over() {
			return new WindowFunctionFramable<InstantExpression>(new InstantExpression(this));
		}
	}

	protected static class AddSecondsExpression extends InstantNumberExpressionWithInstantResult {

		public AddSecondsExpression(InstantExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddSecondsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddSecondsExpression copy() {
			return new AddSecondsExpression(first.copy(), second.copy());
		}
	}

	protected static class AddIntegerSecondsExpression extends InstantIntegerExpressionWithInstantResult {

		public AddIntegerSecondsExpression(InstantExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddSecondsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddIntegerSecondsExpression copy() {
			return new AddIntegerSecondsExpression(first.copy(), second.copy());
		}
	}

	protected static class AddIntegerMinutesExpression extends InstantIntegerExpressionWithInstantResult {

		public AddIntegerMinutesExpression(InstantExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddMinutesTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddIntegerMinutesExpression copy() {
			return new AddIntegerMinutesExpression(first.copy(), second.copy());
		}
	}

	protected static class AddIntegerDaysExpression extends InstantIntegerExpressionWithInstantResult {

		public AddIntegerDaysExpression(InstantExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddDaysTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddIntegerDaysExpression copy() {
			return new AddIntegerDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class AddDaysExpression extends InstantNumberExpressionWithInstantResult {

		public AddDaysExpression(InstantExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddDaysTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddDaysExpression copy() {
			return new AddDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class AddIntegerHoursExpression extends InstantIntegerExpressionWithInstantResult {

		public AddIntegerHoursExpression(InstantExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddHoursTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddIntegerHoursExpression copy() {
			return new AddIntegerHoursExpression(first.copy(), second.copy());
		}
	}

	protected static class AddIntegerWeeksExpression extends InstantIntegerExpressionWithInstantResult {

		public AddIntegerWeeksExpression(InstantExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddWeeksTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddIntegerWeeksExpression copy() {
			return new AddIntegerWeeksExpression(first.copy(), second.copy());
		}
	}

	protected static class AddIntegerMonthsExpression extends InstantIntegerExpressionWithInstantResult {

		public AddIntegerMonthsExpression(InstantExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddMonthsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddIntegerMonthsExpression copy() {
			return new AddIntegerMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class AddMonthsExpression extends InstantNumberExpressionWithInstantResult {

		public AddMonthsExpression(InstantExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddMonthsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddMonthsExpression copy() {
			return new AddMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class AddIntegerYearsExpression extends InstantIntegerExpressionWithInstantResult {

		public AddIntegerYearsExpression(InstantExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantAddYearsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public AddIntegerYearsExpression copy() {
			return new AddIntegerYearsExpression(first.copy(), second.copy());
		}
	}

	protected static class DaysFromExpression extends InstantInstantFunctionWithNumberResult {

		public DaysFromExpression(InstantExpression dateExp, InstantResult otherExp) {
			super(dateExp, otherExp);
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
		public DaysFromExpression copy() {
			return new DaysFromExpression(first.copy(), second.copy());
		}
	}

	protected static class WeeksFromExpression extends InstantInstantFunctionWithNumberResult {

		public WeeksFromExpression(InstantExpression dateExp, InstantResult otherExp) {
			super(dateExp, otherExp);
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
		public WeeksFromExpression copy() {
			return new WeeksFromExpression(first.copy(), second.copy());
		}
	}

	protected static class MonthsFromExpression extends InstantInstantFunctionWithNumberResult {

		public MonthsFromExpression(InstantExpression dateExp, InstantResult otherExp) {
			super(dateExp, otherExp);
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
		public MonthsFromExpression copy() {
			return new MonthsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class YearsFromExpression extends InstantInstantFunctionWithNumberResult {

		public YearsFromExpression(InstantExpression dateExp, InstantResult otherExp) {
			super(dateExp, otherExp);
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
		public YearsFromExpression copy() {
			return new YearsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class HoursFromExpression extends InstantInstantFunctionWithNumberResult {

		public HoursFromExpression(InstantExpression dateExp, InstantResult otherExp) {
			super(dateExp, otherExp);
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
		public HoursFromExpression copy() {
			return new HoursFromExpression(first.copy(), second.copy());
		}
	}

	protected static class MinutesFromExpression extends InstantInstantFunctionWithNumberResult {

		public MinutesFromExpression(InstantExpression dateExp, InstantResult otherExp) {
			super(dateExp, otherExp);
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
		public MinutesFromExpression copy() {
			return new MinutesFromExpression(first.copy(), second.copy());
		}
	}

	protected static class SecondsFromExpression extends InstantInstantFunctionWithNumberResult {

		public SecondsFromExpression(InstantExpression dateExp, InstantResult otherExp) {
			super(dateExp, otherExp);
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
		public SecondsFromExpression copy() {
			return new SecondsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class EndOfMonthExpression extends InstantExpression {

		public EndOfMonthExpression(InstantResult dateVariable) {
			super(dateVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			try {
				return db.doInstantEndOfMonthTransform(this.getInnerResult().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				InstantExpression only = (InstantExpression) getInnerResult();
				return only
						.addDays(only.day().minus(1).bracket().times(-1).integerResult())
						.addMonths(1).addDays(-1)
						//.setHour(0).setMinute(0).setSecond(0)
						.toSQLString(db);
			}
		}

		@Override
		public EndOfMonthExpression copy() {
			return new EndOfMonthExpression((InstantResult) getInnerResult().copy());
		}
	}

	protected static class DayOfWeekExpression extends InstantExpressionWithIntegerResult {

		public DayOfWeekExpression(InstantExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doInstantDayOfWeekTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DayOfWeekExpression copy() {
			return new DayOfWeekExpression((InstantExpression) getInnerResult().copy());
		}
	}

	protected static class LeastOfExpression extends InstantArrayFunctionWithInstantResult {

		public LeastOfExpression(InstantResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (InstantResult num : this.values) {
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
		public LeastOfExpression copy() {
			List<InstantResult> newValues = new ArrayList<>();
			for (InstantResult value : values) {
				newValues.add(value.copy());
			}
			return new LeastOfExpression(newValues.toArray(new InstantResult[]{}));
		}
	}

	protected static class GreatestOfExpression extends InstantArrayFunctionWithInstantResult {

		public GreatestOfExpression(InstantResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (InstantResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doGreatestOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getGreatestOfFunctionName();
		}

		@Override
		public GreatestOfExpression copy() {
			List<InstantResult> newValues = new ArrayList<>();
			for (InstantResult value : values) {
				newValues.add(value.copy());
			}
			return new GreatestOfExpression(newValues.toArray(new InstantResult[]{}));
		}
	}

	public static WindowFunctionFramable<InstantExpression> firstValue() {
		return new FirstValueExpression().over();
	}

	public static class FirstValueExpression extends BooleanExpression implements CanBeWindowingFunctionWithFrame<InstantExpression> {

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
		public WindowFunctionFramable<InstantExpression> over() {
			return new WindowFunctionFramable<InstantExpression>(new InstantExpression(this));
		}

	}

	public static WindowFunctionFramable<InstantExpression> lastValue() {
		return new LastValueExpression().over();
	}

	public static class LastValueExpression extends InstantExpression implements CanBeWindowingFunctionWithFrame<InstantExpression> {

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
		public InstantExpression copy() {
			return new LastValueExpression();
		}

		@Override
		public WindowFunctionFramable<InstantExpression> over() {
			return new WindowFunctionFramable<InstantExpression>(new InstantExpression(this));
		}

	}

	public static WindowFunctionFramable<InstantExpression> nthValue(IntegerExpression indexExpression) {
		return new NthValueExpression(indexExpression).over();
	}

	public static class NthValueExpression extends InstantExpression implements CanBeWindowingFunctionWithFrame<InstantExpression> {

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
		public WindowFunctionFramable<InstantExpression> over() {
			return new WindowFunctionFramable<InstantExpression>(new InstantExpression(this));
		}
	}

	/**
	 * Synonym for lag.
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
	public WindowFunctionRequiresOrderBy<InstantExpression> previousRowValue() {
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
	public WindowFunctionRequiresOrderBy<InstantExpression> lag() {
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
	public WindowFunctionRequiresOrderBy<InstantExpression> lag(IntegerExpression offset) {
		return lag(offset, nullExpression());
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
	public WindowFunctionRequiresOrderBy<InstantExpression> lag(IntegerExpression offset, InstantExpression defaultExpression) {
		return new LagExpression(this, offset, defaultExpression).over();
	}

	/**
	 * Synonym for lead().
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
	public WindowFunctionRequiresOrderBy<InstantExpression> nextRowValue() {
		return lead();
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
	public WindowFunctionRequiresOrderBy<InstantExpression> lead() {
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
	public WindowFunctionRequiresOrderBy<InstantExpression> lead(IntegerExpression offset) {
		return lead(offset, nullExpression());
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
	public WindowFunctionRequiresOrderBy<InstantExpression> lead(IntegerExpression offset, InstantExpression defaultExpression) {
		return new LeadExpression(this, offset, defaultExpression).over();
	}

	private static abstract class LagLeadExpression extends InstantExpression implements CanBeWindowingFunctionRequiresOrderBy<InstantExpression> {

		private static final long serialVersionUID = 1L;

		protected InstantExpression first;
		protected IntegerExpression second;
		protected InstantExpression third;

		LagLeadExpression(InstantExpression first, IntegerExpression second, InstantExpression third) {
			this.first = first;
			this.second = second == null ? value(1) : second;
			this.third = third == null ? nullInstant() : third;
		}

		@Override
		public DBInstant getQueryableDatatypeForExpressionValue() {
			return new DBInstant();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + getFirst().toSQLString(db) + this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db)) + this.afterValue(db);
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
		protected InstantExpression getFirst() {
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
		protected InstantExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
		}

		@Override
		public WindowFunctionRequiresOrderBy<InstantExpression> over() {
			return new WindowFunctionRequiresOrderBy<InstantExpression>(new InstantExpression(this));
		}
	}

	public class LagExpression extends LagLeadExpression {

		private static final long serialVersionUID = 1L;

		public LagExpression(InstantExpression first, IntegerExpression second, InstantExpression third) {
			super(first, second, third);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getLagFunctionName();
		}

		@Override
		public LagExpression copy() {
			return new LagExpression(first, second, third);
		}
	}

	public class LeadExpression extends LagLeadExpression {

		private static final long serialVersionUID = 1L;

		public LeadExpression(InstantExpression first, IntegerExpression second, InstantExpression third) {
			super(first, second, third);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getLeadFunctionName();
		}

		@Override
		public LeadExpression copy() {
			return new LeadExpression(first, second, third);
		}
	}
}
