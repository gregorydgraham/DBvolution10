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

import nz.co.gregs.dbvolution.results.DateRepeatResult;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import org.joda.time.Period;

/**
 * DateExpression implements standard functions that produce a Date or Time
 * result.
 *
 * <p>
 * Date and Time are considered synonymous with timestamp as that appears to be
 * the standard usage by developers. So every date has a time component and
 * every time has a date component. {@link DBDateOnly} implements a time-less
 * date for DBvolution but is considered a DBDate with a time of Midnight for
 * DateExpression purposes.
 *
 * <p>
 * Most query requirements are provided by {@link QueryableDatatype}s like
 * {@link DBString} or {@link DBInteger} but expressions can provide more
 * functions or more precise control.
 *
 * <p>
 * Use a DateExpression to produce a date from an existing column, expression or
 * value and perform date arithmetic.
 *
 * <p>
 * Generally you get a DateExpression from a column or value using
 * {@link DateExpression#value(java.util.Date) } or
 * {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBDate)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DateExpression extends RangeExpression<Date, DateResult, DBDate> implements DateResult {

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

	/**
	 * Default Constructor
	 */
	protected DateExpression() {
		super();
	}

	/**
	 * Create a DateExpression based on an existing {@link DateResult}.
	 *
	 * <p>
	 * {@link DateResult} is generally a DateExpression but it may also be a
	 * {@link DBDate} or {@link DBDateOnly}.
	 *
	 * @param dateVariable a date expression or QueryableDatatype
	 */
	public DateExpression(DateResult dateVariable) {
		super(dateVariable);
	}

	/**
	 * Create a DateExpression based on an existing {@link DateResult}.
	 *
	 * <p>
	 * {@link DateResult} is generally a DateExpression but it may also be a
	 * {@link DBDate} or {@link DBDateOnly}.
	 *
	 * @param variable a date expression or QueryableDatatype
	 */
	protected DateExpression(AnyResult<?> variable) {
		super(variable);
	}

	/**
	 * Create a DateExpression based on an existing Date.
	 *
	 * <p>
	 * This performs a similar function to {@link DateExpression#value(java.util.Date)
	 * }.
	 *
	 * @param date the date to be used in this expression
	 */
	public DateExpression(Date date) {
		super(new DBDate(date));
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return getInnerResult().toSQLString(db);
	}

	@Override
	public DateExpression copy() {
		return isNullSafetyTerminator() ? nullDate() : new DateExpression((AnyResult<?>) this.getInnerResult().copy());
	}

	@Override
	public DateExpression nullExpression() {
		return new DateNullExpression();
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
	public static DateExpression currentDateOnly() {
		return new DateExpression(
				new DateOnlyCurrentDateExpression());
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
	public static DateExpression currentDate() {
		return new DateExpression(
				new DateCurrentDateExpression());
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
	public static DateExpression currentTime() {
		return new DateExpression(
				new DateCurrentTimeExpression());
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
	public NumberExpression year() {
		return new NumberExpression(
				new DateYearExpression(this));
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
	public NumberExpression month() {
		return new NumberExpression(
				new DateMonthExpression(this));
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
	public NumberExpression day() {
		return new NumberExpression(
				new DateDayExpression(this));
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
	public NumberExpression hour() {
		return new NumberExpression(
				new DateHourExpression(this));
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
	public NumberExpression minute() {
		return new NumberExpression(
				new DateMinuteExpression(this));
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
	public NumberExpression second() {
		return new NumberExpression(
				new DateSecondExpression(this));
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
				new DateSubsecondExpression(this));
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
	 * @return a BooleanExpression comparing the date and this DateExpression.
	 */
	@Override
	public BooleanExpression is(Date date) {
		return is(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * @param dateExpression the date the expression must match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the DateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression is(DateResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new DateIsExpression(this, dateExpression));
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
	 * @return a BooleanExpression comparing the DateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression isNot(Date date) {
		return this.isNot(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is NOT
	 * equal to the supplied date.
	 *
	 * @param dateExpression the date the expression must not match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the DateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression isNot(DateResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new DateIsNotExpression(this, dateExpression));
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
	public BooleanExpression isBetween(DateResult lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetween(Date lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetween(DateResult lowerBound, Date upperBound) {
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
	public BooleanExpression isBetween(Date lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenInclusive(DateResult lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(Date lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(DateResult lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenInclusive(Date lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenExclusive(DateResult lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(Date lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(DateResult lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenExclusive(Date lowerBound, Date upperBound) {
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
	public BooleanExpression isLessThan(Date date) {
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
	public BooleanExpression isLessThan(DateResult dateExpression) {
		return new BooleanExpression(new DateIsLessThanExpression(this, dateExpression));
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
	public BooleanExpression isEarlierThan(Date date) {
		return isEarlierThan(new DateExpression(date));
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
	public BooleanExpression isEarlierThan(DateResult dateExpression) {
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
	public DateRepeatExpression getDateRepeatFrom(Date date) {
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
	public DateRepeatExpression getDateRepeatFrom(DateResult dateExpression) {
		return new DateRepeatExpression(new DateGetDateRepeatFromExpression(this, dateExpression));
	}

	/**
	 * Subtract the period/duration provided from this date expression to get an
	 * offset date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Date expression
	 */
	public DateExpression minus(Period interval) {
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
	 * @return a Date expression
	 */
	public DateExpression minus(DateRepeatResult intervalExpression) {
		return new DateExpression(new DateMinusDateRepeatExpression(this, intervalExpression));
	}

	/**
	 * Add the period/duration provided from this date expression to get an offset
	 * date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Date expression
	 */
	public DateExpression plus(Period interval) {
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
	 * @return a Date expression
	 */
	public DateExpression plus(DateRepeatResult intervalExpression) {
		return new DateExpression(new DatePlusDateRepeatExpression(this, intervalExpression));
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
	public BooleanExpression isLessThanOrEqual(Date date) {
		return super.isLessThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied DateResult.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(DateResult dateExpression) {
		return new BooleanExpression(new DateIsLessThanOrEqualExpression(this, dateExpression));
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
	public BooleanExpression isGreaterThan(Date date) {
		return super.isGreaterThan(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied DateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThan(DateResult dateExpression) {
		return new BooleanExpression(new DateIsGreaterThanExpression(this, dateExpression));
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
	public BooleanExpression isLaterThan(Date date) {
		return isGreaterThan(new DateExpression(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied DateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLaterThan(DateResult dateExpression) {
		return isGreaterThan(dateExpression);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied Date.
	 *
	 * @param date the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(Date date) {
		return super.isGreaterThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied DateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(DateResult dateExpression) {
		return new BooleanExpression(new DateIsGreaterThanOrEqualExpression(this, dateExpression));
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
	public BooleanExpression isLessThan(Date value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isGreaterThan(Date value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isLessThan(DateResult value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isGreaterThan(DateResult value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of Dates.
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
	public BooleanExpression isIn(Date... possibleValues) {
		List<DateExpression> possVals = new ArrayList<DateExpression>();
		for (Date num : possibleValues) {
			possVals.add(value(num));
		}
		return isIn(possVals.toArray(new DateExpression[]{}));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of Dates.
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
	public BooleanExpression isIn(Collection<? extends DateResult> possibleValues) {
		//List<DateExpression> possVals = new ArrayList<DateExpression>();
		//for (Date num : possibleValues) {
		//	possVals.add(value(num));
		//}
		return isIn(possibleValues.toArray(new DateResult[]{}));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of DateResults.
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
	public BooleanExpression isIn(DateResult... possibleValues) {
		BooleanExpression isInExpr = new BooleanExpression(new DateIsInExpression(this, possibleValues));
		if (isInExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isInExpr);
		} else {
			return isInExpr;
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
	public DateExpression ifDBNull(Date alternative) {
		return ifDBNull(value(alternative));
//		return new DateExpression(
//				new DateExpression.DateDateFunctionWithDateResult(this, new DateExpression(alternative)) {
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
	 * DateResult.
	 *
	 * <p>
	 * This is a way of handling dates that should have a value but don't.
	 *
	 * @param alternative use this value if the expression evaluates to NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public DateExpression ifDBNull(DateResult alternative) {
		return new DateExpression(
				new DateIfDBNullExpression(this, alternative));
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
	public DateExpression max() {
		return new DateExpression(new DateMaxExpression(this));
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
	public DateExpression min() {
		return new DateExpression(new DateMinExpression(this));
	}

	@Override
	public DBDate getQueryableDatatypeForExpressionValue() {
		return new DBDate();
	}

	/**
	 * Date Arithmetic: add the supplied number of seconds to the date expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addSeconds(int secondsToAdd) {
		return this.addSeconds(value(secondsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of seconds to the date expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addSeconds(NumberExpression secondsToAdd) {
		return new DateExpression(
				new DateAddSecondsExpression(this, secondsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of seconds to the date expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addSeconds(IntegerExpression secondsToAdd) {
		return new DateExpression(
				new DateAddIntegerSecondsExpression(this, secondsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of minutes to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addMinutes(int minutesToAdd) {
		return this.addMinutes(value(minutesToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of minutes to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addMinutes(IntegerExpression minutesToAdd) {
		return new DateExpression(
				new DateAddIntegerMinutesExpression(this, minutesToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addDays(Integer daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addDays(Long daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addDays(Number daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addDays(IntegerExpression daysToAdd) {
		return new DateExpression(
				new DateAddIntegerDaysExpression(this, daysToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addDays(NumberExpression daysToAdd) {
		return new DateExpression(
				new DateAddDaysExpression(this, daysToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of hours to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addHours(int hoursToAdd) {
		return this.addHours(value(hoursToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of hours to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public DateExpression addHours(IntegerExpression hoursToAdd) {
		return new DateExpression(
				new DateAddIntegerHoursExpression(this, hoursToAdd));
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
	 * @return a DateExpression
	 */
	public DateExpression addWeeks(int weeksToAdd) {
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
	 * @return a DateExpression
	 */
	public DateExpression addWeeks(IntegerExpression weeksToAdd) {
		return new DateExpression(
				new DateAddIntegerWeeksExpression(this, weeksToAdd));
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
	 * @return a DateExpression
	 */
	public DateExpression addMonths(Number monthsToAdd) {
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
	 * @return a DateExpression
	 */
	public DateExpression addMonths(Integer monthsToAdd) {
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
	 * @return a DateExpression
	 */
	public DateExpression addMonths(Long monthsToAdd) {
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
	 * @return a DateExpression
	 */
	public DateExpression addMonths(IntegerExpression monthsToAdd) {
		return new DateExpression(
				new DateAddIntegerMonthsExpression(this, monthsToAdd));
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
	 * @return a DateExpression
	 */
	public DateExpression addMonths(NumberExpression monthsToAdd) {
		return new DateExpression(
				new DateAddMonthsExpression(this, monthsToAdd));
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
	 * @return a DateExpression
	 */
	public DateExpression addYears(int yearsToAdd) {
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
	 * @return a DateExpression
	 */
	public DateExpression addYears(IntegerExpression yearsToAdd) {
		return new DateExpression(
				new DateAddIntegerYearsExpression(this, yearsToAdd));
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
	public NumberExpression daysFrom(Date dateToCompareTo) {
		return daysFrom(DateExpression.value(dateToCompareTo));
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
	public NumberExpression daysFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDaysFromExpression(this, dateToCompareTo));
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
	public NumberExpression weeksFrom(Date dateToCompareTo) {
		return weeksFrom(DateExpression.value(dateToCompareTo));
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
	public NumberExpression weeksFrom(DateExpression dateToCompareTo) {
		return new NumberExpression(
				new DateWeeksFromExpression(this, dateToCompareTo));
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
	public NumberExpression monthsFrom(Date dateToCompareTo) {
		return monthsFrom(DateExpression.value(dateToCompareTo));
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
	public NumberExpression monthsFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateMonthsFromExpression(this, dateToCompareTo));
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
	public NumberExpression yearsFrom(Date dateToCompareTo) {
		return yearsFrom(DateExpression.value(dateToCompareTo));
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
	public NumberExpression yearsFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateYearsFromExpression(this, dateToCompareTo));
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
	public NumberExpression hoursFrom(Date dateToCompareTo) {
		return hoursFrom(DateExpression.value(dateToCompareTo));
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
	public NumberExpression hoursFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateHoursFromExpression(this, dateToCompareTo));
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
	public NumberExpression minutesFrom(Date dateToCompareTo) {
		return minutesFrom(DateExpression.value(dateToCompareTo));
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
	public NumberExpression minutesFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateMinutesFromExpression(this, dateToCompareTo));
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
	public NumberExpression secondsFrom(Date dateToCompareTo) {
		return secondsFrom(DateExpression.value(dateToCompareTo));
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
	public NumberExpression secondsFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateSecondsFromExpression(this, dateToCompareTo));
	}

	/**
	 * Derive the first day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Date expression
	 */
	public DateExpression firstOfMonth() {
		return this.addDays(this.day().minus(1).bracket().times(-1).integerResult());
	}

	/**
	 * Derive the last day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Date expression
	 */
	public DateExpression endOfMonth() {
		return new DateExpression(
				new DateEndOfMonthExpression(this)
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
				new DateDayOfWeekExpression(this));
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
	public static BooleanExpression overlaps(Date firstStartTime, Date firstEndTime, Date secondStartTime, Date secondEndtime) {
		return DateExpression.overlaps(
				DateExpression.value(firstStartTime), DateExpression.value(firstEndTime),
				DateExpression.value(secondStartTime), DateExpression.value(secondEndtime)
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
	public static BooleanExpression overlaps(DateResult firstStartTime, DateResult firstEndTime, DateResult secondStartTime, DateResult secondEndtime) {
		return DateExpression.overlaps(
				new DateExpression(firstStartTime), new DateExpression(firstEndTime),
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
	public static BooleanExpression overlaps(DateExpression firstStartTime, DateExpression firstEndTime, DateResult secondStartTime, DateResult secondEndtime) {
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
	public static DateExpression leastOf(Date... possibleValues) {
		ArrayList<DateExpression> possVals = new ArrayList<DateExpression>();
		for (Date num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new DateExpression[]{}));
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
	public static DateExpression leastOf(Collection<? extends DateResult> possibleValues) {
		ArrayList<DateExpression> possVals = new ArrayList<DateExpression>();
		for (DateResult num : possibleValues) {
			possVals.add(new DateExpression(num));
		}
		return leastOf(possVals.toArray(new DateExpression[]{}));
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
	public static DateExpression leastOf(DateResult... possibleValues) {
		DateExpression leastExpr
				= new DateExpression(new DateLeastOfExpression(possibleValues));
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
	public static DateExpression greatestOf(Date... possibleValues) {
		ArrayList<DateExpression> possVals = new ArrayList<DateExpression>();
		for (Date num : possibleValues) {
			possVals.add(value(num));
		}
		return greatestOf(possVals.toArray(new DateExpression[]{}));
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
	public static DateExpression greatestOf(Collection<? extends DateResult> possibleValues) {
		ArrayList<DateExpression> possVals = new ArrayList<DateExpression>();
		for (DateResult num : possibleValues) {
			possVals.add(new DateExpression(num));
		}
		return greatestOf(possVals.toArray(new DateExpression[]{}));
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
	public static DateExpression greatestOf(DateResult... possibleValues) {
		DateExpression greatestOf
				= new DateExpression(new DateGreatestOfExpression(possibleValues));
		return greatestOf;
	}

	@Override
	public DBDate asExpressionColumn() {
		return new DBDate(this);
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
		StringExpression isoFormatDateTime = new StringExpression("")
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
		StringExpression usaFormatDateTime = new StringExpression("")
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
		StringExpression commonFormatDateTime = new StringExpression("")
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
	public DateExpression expression(Date value) {
		return new DateExpression(value);
	}

	@Override
	public DateExpression expression(DateResult value) {
		return new DateExpression(value);
	}

	@Override
	public DateResult expression(DBDate value) {
		return new DateExpression(value);
	}

	private static abstract class FunctionWithDateResult extends DateExpression {

		private static final long serialVersionUID = 1L;

		FunctionWithDateResult() {
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
		public DateExpression.FunctionWithDateResult copy() {
			DateExpression.FunctionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
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
	}

	private static abstract class DateExpressionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		DateExpressionWithNumberResult() {
			super();
		}

		DateExpressionWithNumberResult(DateExpression only) {
			super(only);
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public abstract String toSQLString(DBDefinition db);
	}

	private static abstract class DateDateExpressionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		protected DateExpression first;
		protected DateExpression second;
		private boolean requiresNullProtection = false;

		DateDateExpressionWithBooleanResult(DateExpression first, DateResult second) {
			this.first = first;
			this.second = new DateExpression(second);
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
		public DateDateExpressionWithBooleanResult copy() {
			DateDateExpressionWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
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
	}

	private static abstract class DateDateExpressionWithDateRepeatResult extends DateRepeatExpression {

		private static final long serialVersionUID = 1L;

		protected DateExpression first;
		protected DateExpression second;
		private boolean requiresNullProtection = false;

		DateDateExpressionWithDateRepeatResult(DateExpression first, DateResult second) {
			this.first = first;
			this.second = new DateExpression(second);
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DateDateExpressionWithDateRepeatResult copy() {
			DateDateExpressionWithDateRepeatResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
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
		public DateExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		public DateExpression getSecond() {
			return second;
		}
	}

	private static abstract class DateDateRepeatArithmeticDateResult extends DateExpression {

		private static final long serialVersionUID = 1L;

		protected DateExpression first;
		protected DateRepeatExpression second;
		private boolean requiresNullProtection = false;

		DateDateRepeatArithmeticDateResult(DateExpression first, DateRepeatResult second) {
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
		public DateDateRepeatArithmeticDateResult copy() {
			DateDateRepeatArithmeticDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
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
		public DateExpression getFirst() {
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

	private static abstract class DateArrayFunctionWithDateResult extends DateExpression {

		private static final long serialVersionUID = 1L;

		protected DateExpression column;
		protected final List<DateResult> values = new ArrayList<DateResult>();
		boolean nullProtectionRequired = false;

		DateArrayFunctionWithDateResult() {
		}

		DateArrayFunctionWithDateResult(DateResult[] rightHandSide) {
			for (DateResult dateResult : rightHandSide) {
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
			for (DateResult val : values) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public DateArrayFunctionWithDateResult copy() {
			DateArrayFunctionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			for (DateResult value : this.values) {
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
			for (DateResult second : values) {
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
			for (DateResult numer : values) {
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
				for (DateResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class DateDateResultFunctionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private DateExpression column;
		private List<DateResult> values = new ArrayList<DateResult>();
		boolean nullProtectionRequired = false;

		DateDateResultFunctionWithBooleanResult() {
		}

		DateDateResultFunctionWithBooleanResult(DateExpression leftHandSide, DateResult[] rightHandSide) {
			this.column = leftHandSide;
			for (DateResult dateResult : rightHandSide) {
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
			for (DateResult val : getValues()) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public DateDateResultFunctionWithBooleanResult copy() {
			DateDateResultFunctionWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.getColumn().copy();
			for (DateResult value : this.getValues()) {
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
			for (DateResult val : getValues()) {
				if (val != null) {
					hashSet.addAll(val.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false || getColumn().isAggregator();
			for (DateResult dater : getValues()) {
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
		protected DateExpression getColumn() {
			return column;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the values
		 */
		protected List<DateResult> getValues() {
			return values;
		}
	}

	private static abstract class DateDateFunctionWithDateResult extends DateExpression {

		private static final long serialVersionUID = 1L;

		private DateExpression first;
		private DateResult second;

		DateDateFunctionWithDateResult(DateExpression first) {
			this.first = first;
			this.second = null;
		}

		DateDateFunctionWithDateResult(DateExpression first, DateResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public abstract String toSQLString(DBDefinition db);

		@Override
		public DateDateFunctionWithDateResult copy() {
			DateDateFunctionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
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
		protected DateExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected DateResult getSecond() {
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

	private static abstract class DateFunctionWithDateResult extends DateExpression {

		private static final long serialVersionUID = 1L;

		DateFunctionWithDateResult() {
			super();
		}

		DateFunctionWithDateResult(DateExpression only) {
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

	private static abstract class DateIntegerExpressionWithDateResult extends DateExpression {

		private static final long serialVersionUID = 1L;

		protected DateExpression first;
		protected IntegerExpression second;

		DateIntegerExpressionWithDateResult() {
			this.first = null;
			this.second = null;
		}

		DateIntegerExpressionWithDateResult(DateExpression dateExp, IntegerExpression numbExp) {
			this.first = dateExp;
			this.second = numbExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public DateIntegerExpressionWithDateResult copy() {
			DateIntegerExpressionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
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

	private static abstract class DateNumberExpressionWithDateResult extends DateExpression {

		private static final long serialVersionUID = 1L;

		protected DateExpression first;
		protected NumberExpression second;

		DateNumberExpressionWithDateResult() {
			this.first = null;
			this.second = null;
		}

		DateNumberExpressionWithDateResult(DateExpression dateExp, NumberExpression numbExp) {
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

	private static abstract class DateDateFunctionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		protected DateExpression first;
		protected DateResult second;

		DateDateFunctionWithNumberResult() {
			this.first = null;
			this.second = null;
		}

		DateDateFunctionWithNumberResult(DateExpression dateExp, DateResult otherDateExp) {
			this.first = dateExp;
			this.second = otherDateExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public DateDateFunctionWithNumberResult copy() {
			DateDateFunctionWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
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

	private static class DateNullExpression extends DateExpression {

		public DateNullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public DateNullExpression copy() {
			return new DateNullExpression();
		}
	}

	protected static class DateOnlyCurrentDateExpression extends FunctionWithDateResult {

		public DateOnlyCurrentDateExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentDateOnlyTransform();
		}

		@Override
		public DBDate getQueryableDatatypeForExpressionValue() {
			return new DBDateOnly();
		}

		@Override
		public DateOnlyCurrentDateExpression copy() {
			return new DateOnlyCurrentDateExpression();
		}

	}

	protected static class DateCurrentDateExpression extends FunctionWithDateResult {

		public DateCurrentDateExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentDateTimeTransform();
		}

		@Override
		public DateCurrentDateExpression copy() {
			return new DateCurrentDateExpression();
		}

	}

	protected static class DateCurrentTimeExpression extends FunctionWithDateResult {

		public DateCurrentTimeExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentTimeTransform();
		}

		@Override
		public DateOnlyCurrentDateExpression copy() {
			return new DateOnlyCurrentDateExpression();
		}

	}

	protected static class DateYearExpression extends DateExpressionWithNumberResult {

		public DateYearExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doYearTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateYearExpression copy() {
			return new DateYearExpression((DateExpression) getInnerResult().copy());
		}

	}

	protected static class DateMonthExpression extends DateExpressionWithNumberResult {

		public DateMonthExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMonthTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateMonthExpression copy() {
			return new DateMonthExpression((DateExpression) getInnerResult().copy());
		}

	}

	protected static class DateDayExpression extends DateExpressionWithNumberResult {

		public DateDayExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDayTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateDayExpression copy() {
			return new DateDayExpression((DateExpression) getInnerResult().copy());
		}

	}

	protected static class DateHourExpression extends DateExpressionWithNumberResult {

		public DateHourExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doHourTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateHourExpression copy() {
			return new DateHourExpression((DateExpression) getInnerResult().copy());
		}

	}

	protected static class DateMinuteExpression extends DateExpressionWithNumberResult {

		public DateMinuteExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMinuteTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateMinuteExpression copy() {
			return new DateMinuteExpression((DateExpression) getInnerResult().copy());
		}

	}

	protected static class DateSecondExpression extends DateExpressionWithNumberResult {

		public DateSecondExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateSecondExpression copy() {
			return new DateSecondExpression((DateExpression) getInnerResult().copy());
		}
	}

	protected static class DateSubsecondExpression extends DateExpressionWithNumberResult {

		public DateSubsecondExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSubsecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateSubsecondExpression copy() {
			return new DateSubsecondExpression((DateExpression) getInnerResult().copy());
		}

	}

	protected static class DateIsExpression extends DateDateExpressionWithBooleanResult {

		public DateIsExpression(DateExpression first, DateResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " = ";
		}

		@Override
		public DateIsExpression copy() {
			return new DateIsExpression(first.copy(), second.copy());
		}

	}

	protected static class DateIsNotExpression extends DateDateExpressionWithBooleanResult {

		public DateIsNotExpression(DateExpression first, DateResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <> ";
		}

		@Override
		public DateIsNotExpression copy() {
			return new DateIsNotExpression(first.copy(), second.copy());
		}

	}

	protected static class DateIsLessThanExpression extends DateDateExpressionWithBooleanResult {

		public DateIsLessThanExpression(DateExpression first, DateResult second) {
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
		public DateIsLessThanExpression copy() {
			return new DateIsLessThanExpression(first.copy(), second.copy());
		}

	}

	protected static class DateGetDateRepeatFromExpression extends DateDateExpressionWithDateRepeatResult {

		public DateGetDateRepeatFromExpression(DateExpression first, DateResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
				return db.doDateMinusToDateRepeatTransformation(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final DateExpression left = getFirst();
				final DateExpression right = new DateExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullString(),
								StringExpression.value(INTERVAL_PREFIX)
										.append(left.year().minus(right.year()).bracket()).append(YEAR_SUFFIX)
										.append(left.month().minus(right.month()).bracket()).append(MONTH_SUFFIX)
										.append(left.day().minus(right.day()).bracket()).append(DAY_SUFFIX)
										.append(left.hour().minus(right.hour()).bracket()).append(HOUR_SUFFIX)
										.append(left.minute().minus(right.minute()).bracket()).append(MINUTE_SUFFIX)
										.append(left.second().minus(right.second()).bracket())
										.append(".")
										.append(left.subsecond().minus(right.subsecond()).absoluteValue().stringResult().substringAfter("."))
										.append(SECOND_SUFFIX)
						).toSQLString(db);
			}
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public DateGetDateRepeatFromExpression copy() {
			return new DateGetDateRepeatFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateMinusDateRepeatExpression extends DateDateRepeatArithmeticDateResult {

		public DateMinusDateRepeatExpression(DateExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
				return db.doDateMinusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final DateExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullDate(),
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
		public DateMinusDateRepeatExpression copy() {
			return new DateMinusDateRepeatExpression(first.copy(), second.copy());
		}
	}

	protected static class DatePlusDateRepeatExpression extends DateDateRepeatArithmeticDateResult {

		public DatePlusDateRepeatExpression(DateExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
				return db.doDatePlusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final DateExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullDate(),
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
		public DatePlusDateRepeatExpression copy() {
			return new DatePlusDateRepeatExpression(getFirst().copy(), getSecond().copy());
		}

	}

	protected static class DateIsLessThanOrEqualExpression extends DateDateExpressionWithBooleanResult {

		public DateIsLessThanOrEqualExpression(DateExpression first, DateResult second) {
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
		public DateIsLessThanOrEqualExpression copy() {
			return new DateIsLessThanOrEqualExpression(first.copy(), second.copy());
		}

	}

	protected static class DateIsGreaterThanExpression extends DateDateExpressionWithBooleanResult {

		public DateIsGreaterThanExpression(DateExpression first, DateResult second) {
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
		public DateIsGreaterThanExpression copy() {
			return new DateIsGreaterThanExpression(first.copy(), second.copy());
		}

	}

	protected static class DateIsGreaterThanOrEqualExpression extends DateDateExpressionWithBooleanResult {

		public DateIsGreaterThanOrEqualExpression(DateExpression first, DateResult second) {
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
		public DateIsGreaterThanOrEqualExpression copy() {
			return new DateIsGreaterThanOrEqualExpression(first.copy(), second.copy());
		}
	}

	protected class DateIsInExpression extends DateDateResultFunctionWithBooleanResult {

		public DateIsInExpression(DateExpression leftHandSide, DateResult[] rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<String>();
			for (DateResult value : getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		public DateIsInExpression copy() {
			final List<DateResult> values = getValues();
			final List<DateResult> newValues = new ArrayList<>();
			for (DateResult value : values) {
				newValues.add(value.copy());
			}
			return new DateIsInExpression(
					getColumn().copy(),
					newValues.toArray(new DateResult[]{}));
		}

	}

	protected static class DateIfDBNullExpression extends DateDateFunctionWithDateResult {

		public DateIfDBNullExpression(DateExpression first, DateResult second) {
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
		public DateIfDBNullExpression copy() {
			return new DateIfDBNullExpression(getFirst().copy(), getSecond().copy());
		}
	}

	protected static class DateMaxExpression extends DateFunctionWithDateResult {

		public DateMaxExpression(DateExpression only) {
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
		public DateMaxExpression copy() {
			return new DateMaxExpression((DateExpression) getInnerResult().copy());
		}
	}

	protected static class DateMinExpression extends DateFunctionWithDateResult {

		public DateMinExpression(DateExpression only) {
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
		public DateMinExpression copy() {
			return new DateMinExpression((DateExpression) getInnerResult().copy());
		}
	}

	protected static class DateAddSecondsExpression extends DateNumberExpressionWithDateResult {

		public DateAddSecondsExpression(DateExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		private DateAddSecondsExpression(DateExpression dateExpression) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddSecondsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddSecondsExpression copy() {
			return new DateAddSecondsExpression((DateExpression) getInnerResult().copy());
		}
	}

	protected static class DateAddIntegerSecondsExpression extends DateIntegerExpressionWithDateResult {

		public DateAddIntegerSecondsExpression(DateExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddSecondsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddIntegerSecondsExpression copy() {
			return new DateAddIntegerSecondsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerMinutesExpression extends DateIntegerExpressionWithDateResult {

		public DateAddIntegerMinutesExpression(DateExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddMinutesTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddIntegerMinutesExpression copy() {
			return new DateAddIntegerMinutesExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerDaysExpression extends DateIntegerExpressionWithDateResult {

		public DateAddIntegerDaysExpression(DateExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddDaysTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddIntegerDaysExpression copy() {
			return new DateAddIntegerDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddDaysExpression extends DateNumberExpressionWithDateResult {

		public DateAddDaysExpression(DateExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddDaysTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddDaysExpression copy() {
			return new DateAddDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerHoursExpression extends DateIntegerExpressionWithDateResult {

		public DateAddIntegerHoursExpression(DateExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddHoursTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddIntegerHoursExpression copy() {
			return new DateAddIntegerHoursExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerWeeksExpression extends DateIntegerExpressionWithDateResult {

		public DateAddIntegerWeeksExpression(DateExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddWeeksTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddIntegerWeeksExpression copy() {
			return new DateAddIntegerWeeksExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerMonthsExpression extends DateIntegerExpressionWithDateResult {

		public DateAddIntegerMonthsExpression(DateExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddMonthsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddIntegerMonthsExpression copy() {
			return new DateAddIntegerMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddMonthsExpression extends DateNumberExpressionWithDateResult {

		public DateAddMonthsExpression(DateExpression dateExp, NumberExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddMonthsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddMonthsExpression copy() {
			return new DateAddMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerYearsExpression extends DateIntegerExpressionWithDateResult {

		public DateAddIntegerYearsExpression(DateExpression dateExp, IntegerExpression numbExp) {
			super(dateExp, numbExp);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doAddYearsTransform(first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public DateAddIntegerYearsExpression copy() {
			return new DateAddIntegerYearsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateDaysFromExpression extends DateDateFunctionWithNumberResult {

		public DateDaysFromExpression(DateExpression dateExp, DateResult otherDateExp) {
			super(dateExp, otherDateExp);
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
		public DateDaysFromExpression copy() {
			return new DateDaysFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateWeeksFromExpression extends DateDateFunctionWithNumberResult {

		public DateWeeksFromExpression(DateExpression dateExp, DateResult otherDateExp) {
			super(dateExp, otherDateExp);
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
		public DateWeeksFromExpression copy() {
			return new DateWeeksFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateMonthsFromExpression extends DateDateFunctionWithNumberResult {

		public DateMonthsFromExpression(DateExpression dateExp, DateResult otherDateExp) {
			super(dateExp, otherDateExp);
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
		public DateMonthsFromExpression copy() {
			return new DateMonthsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateYearsFromExpression extends DateDateFunctionWithNumberResult {

		public DateYearsFromExpression(DateExpression dateExp, DateResult otherDateExp) {
			super(dateExp, otherDateExp);
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
		public DateYearsFromExpression copy() {
			return new DateYearsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateHoursFromExpression extends DateDateFunctionWithNumberResult {

		public DateHoursFromExpression(DateExpression dateExp, DateResult otherDateExp) {
			super(dateExp, otherDateExp);
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
		public DateHoursFromExpression copy() {
			return new DateHoursFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateMinutesFromExpression extends DateDateFunctionWithNumberResult {

		public DateMinutesFromExpression(DateExpression dateExp, DateResult otherDateExp) {
			super(dateExp, otherDateExp);
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
		public DateMinutesFromExpression copy() {
			return new DateMinutesFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateSecondsFromExpression extends DateDateFunctionWithNumberResult {

		public DateSecondsFromExpression(DateExpression dateExp, DateResult otherDateExp) {
			super(dateExp, otherDateExp);
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
		public DateSecondsFromExpression copy() {
			return new DateSecondsFromExpression(first.copy(), second.copy());
		}
	}

	protected static class DateEndOfMonthExpression extends DateExpression {

		public DateEndOfMonthExpression(DateResult dateVariable) {
			super(dateVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			try {
				return db.doEndOfMonthTransform(this.getInnerResult().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				DateExpression only = (DateExpression) getInnerResult();
				return only
						.addDays(only.day().minus(1).bracket().times(-1).integerResult())
						.addMonths(1).addDays(-1).toSQLString(db);
			}
		}

		@Override
		public DateEndOfMonthExpression copy() {
			return new DateEndOfMonthExpression((DateResult) getInnerResult().copy());
		}
	}

	protected static class DateDayOfWeekExpression extends DateExpressionWithNumberResult {

		public DateDayOfWeekExpression(DateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDayOfWeekTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateDayOfWeekExpression copy() {
			return new DateDayOfWeekExpression((DateExpression) getInnerResult().copy());
		}
	}

	protected static class DateLeastOfExpression extends DateArrayFunctionWithDateResult {

		public DateLeastOfExpression(DateResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (DateResult num : this.values) {
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
		public DateLeastOfExpression copy() {
			List<DateResult> newValues = new ArrayList<>();
			for (DateResult value : values) {
				newValues.add(value.copy());
			}
			return new DateLeastOfExpression(newValues.toArray(new DateResult[]{}));
		}
	}

	protected static class DateGreatestOfExpression extends DateArrayFunctionWithDateResult {

		public DateGreatestOfExpression(DateResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (DateResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doGreatestOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getGreatestOfFunctionName();
		}

		@Override
		public DateGreatestOfExpression copy() {
			List<DateResult> newValues = new ArrayList<>();
			for (DateResult value : values) {
				newValues.add(value.copy());
			}
			return new DateGreatestOfExpression(newValues.toArray(new DateResult[]{}));
		}
	}
}
