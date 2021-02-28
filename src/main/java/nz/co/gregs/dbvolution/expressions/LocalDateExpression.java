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
import java.time.LocalDate;
import java.time.Month;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.results.DateRepeatResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.LocalDateResult;
import org.joda.time.Period;

/**
 * DateExpression implements standard functions that produce a LocalDate or Time
 * result.
 *
 * <p>
 * LocalDate and Time are considered synonymous with timestamp as that appears
 * to be the standard usage by developers. So every date has a time component
 * and every time has a date component. {@link DBDateOnly} implements a
 * time-less date for DBvolution but is considered a DBDate with a time of
 * Midnight for DateExpression purposes.
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
 * {@link DateExpression#value(java.util.LocalDate) } or
 * {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBDate)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class LocalDateExpression extends RangeExpression<LocalDate, LocalDateResult, DBLocalDate> implements LocalDateResult {

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

	public static LocalDateExpression value(IntegerExpression year, IntegerExpression month, IntegerExpression day) {
		return new NewLocalDateExpression(year, month, day);
	}

	public static LocalDateExpression newLocalDate(IntegerExpression year, IntegerExpression month, IntegerExpression day) {
		return new NewLocalDateExpression(year, month, day);
	}

	public static LocalDateExpression newLocalDate(DateExpression dateExpression) {
		//return new LocalDateExpression(dateExpression);
		return new NewLocalDateExpression(dateExpression.year(), dateExpression.month(), dateExpression.day());
	}

	/**
	 * Default Constructor
	 */
	protected LocalDateExpression() {
		super();
	}

	/**
	 * Create a DateExpression based on an existing {@link LocalDateResult}.
	 *
	 * <p>
	 * {@link LocalDateResult} is generally a DateExpression but it may also be a
	 * {@link DBDate} or {@link DBDateOnly}.
	 *
	 * @param dateVariable a date expression or QueryableDatatype
	 */
	public LocalDateExpression(LocalDateResult dateVariable) {
		super(dateVariable);
	}

	/**
	 * Create a DateExpression based on an existing {@link LocalDateResult}.
	 *
	 * <p>
	 * {@link LocalDateResult} is generally a DateExpression but it may also be a
	 * {@link DBDate} or {@link DBDateOnly}.
	 *
	 * @param variable a date expression or QueryableDatatype
	 */
	protected LocalDateExpression(AnyResult<?> variable) {
		super(variable);
	}

	/**
	 * Create a DateExpression based on an existing LocalDate.
	 *
	 * <p>
	 * This performs a similar function to {@link DateExpression#value(java.util.LocalDate)
	 * }.
	 *
	 * @param date the date to be used in this expression
	 */
	public LocalDateExpression(LocalDate date) {
		super(new DBLocalDate(date));
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return getInnerResult().toSQLString(db);
	}

	@Override
	public LocalDateExpression copy() {
		return isNullSafetyTerminator() ? nullExpression() : new LocalDateExpression((AnyResult<?>) this.getInnerResult().copy());
	}

	@Override
	public LocalDateExpression nullExpression() {
		return new DateNullExpression();
	}

	public LocalDateExpression toLocalDate() {
		return this;
	}

	public LocalDateTimeExpression toLocalDateTime() {
		return new LocalDateTimeExpression()
				.setYear(this.year())
				.setMonth(this.month())
				.setDay(this.day())
				.setHour(0)
				.setMinute(0)
				.setSecond(0);
	}

	/**
	 * Creates a date expression that returns only the date part of current date
	 * on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current day, according to the
	 * database, with the time set to Midnight.
	 *
	 * @return a date expression of only the date part of the current database
	 * timestamp.
	 */
	public static LocalDateExpression currentLocalDate() {
		return new LocalDateExpression(
				new DateOnlyCurrentDateExpression());
	}

	/**
	 * Creates a date expression that returns the current date on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current day and time according to
	 * the database.
	 *
	 * @return a datetime expression of the current database timestamp.
	 */
	public static LocalDateTimeExpression currentLocalDateTime() {
		return LocalDateTimeExpression.now();
	}

	/**
	 * Creates a date expression that returns today's date, that is the current
	 * date on the database.
	 *
	 * @return a date expression of today's date.
	 */
	public static LocalDateExpression currentDate() {
		return currentLocalDate();
	}

	/**
	 * Creates a date expression that returns today's date, that is the current
	 * date on the database.
	 *
	 * @return a date expression of today's date.
	 */
	public static LocalDateExpression now() {
		return currentLocalDate();
	}

	/**
	 * Creates a date expression that returns today's date, that is the current
	 * date on the database.
	 *
	 * @return a date expression of today's date.
	 */
	public static LocalDateExpression today() {
		return currentLocalDate();
	}

	/**
	 * Creates a date expression that returns yesterday's date, that is the
	 * current date on the database minus one day.
	 *
	 * @return a date expression of yesterday's date.
	 */
	public static LocalDateExpression yesterday() {
		return currentLocalDate().addDays(-1);
	}

	/**
	 * Creates a date expression that returns tomorrow's date, that is the current
	 * date on the database plus one day.
	 *
	 * @return a date expression of tomorrow's date.
	 */
	public static LocalDateExpression tomorrow() {
		return currentLocalDate().addDays(1);
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
		return new DateYearExpression(this);
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
		return new IntegerExpression(
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
	public IntegerExpression day() {
		return new DateDayExpression(this);
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
	public BooleanExpression is(LocalDate date) {
		return is(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * @param dateExpression the date the expression must match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the LocalDateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression is(LocalDateResult dateExpression) {
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
	 * @return a BooleanExpression comparing the LocalDateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression isNot(LocalDate date) {
		return this.isNot(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is NOT
	 * equal to the supplied date.
	 *
	 * @param dateExpression the date the expression must not match
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression comparing the LocalDateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression isNot(LocalDateResult dateExpression) {
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
	public BooleanExpression isBetween(LocalDateResult lowerBound, LocalDateResult upperBound) {
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
	public BooleanExpression isBetween(LocalDate lowerBound, LocalDateResult upperBound) {
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
	public BooleanExpression isBetween(LocalDateResult lowerBound, LocalDate upperBound) {
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
	public BooleanExpression isBetween(LocalDate lowerBound, LocalDate upperBound) {
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
	public BooleanExpression isBetweenInclusive(LocalDateResult lowerBound, LocalDateResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(LocalDate lowerBound, LocalDateResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(LocalDateResult lowerBound, LocalDate upperBound) {
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
	public BooleanExpression isBetweenInclusive(LocalDate lowerBound, LocalDate upperBound) {
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
	public BooleanExpression isBetweenExclusive(LocalDateResult lowerBound, LocalDateResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(LocalDate lowerBound, LocalDateResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(LocalDateResult lowerBound, LocalDate upperBound) {
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
	public BooleanExpression isBetweenExclusive(LocalDate lowerBound, LocalDate upperBound) {
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
	public BooleanExpression isLessThan(LocalDate date) {
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
	public BooleanExpression isLessThan(LocalDateResult dateExpression) {
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
	public BooleanExpression isEarlierThan(LocalDate date) {
		return isEarlierThan(new LocalDateExpression(date));
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
	public BooleanExpression isEarlierThan(LocalDateResult dateExpression) {
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
	public DateRepeatExpression getDateRepeatFrom(LocalDate date) {
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
	public DateRepeatExpression getDateRepeatFrom(LocalDateResult dateExpression) {
		return new DateRepeatExpression(new DateGetDateRepeatFromExpression(this, dateExpression));
	}

	/**
	 * Subtract the period/duration provided from this date expression to get an
	 * offset date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDate expression
	 */
	public LocalDateExpression minus(Period interval) {
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
	 * @return a LocalDate expression
	 */
	public LocalDateExpression minus(DateRepeatResult intervalExpression) {
		return new LocalDateExpression(new DateMinusDateRepeatExpression(this, intervalExpression));
	}

	/**
	 * Add the period/duration provided from this date expression to get an offset
	 * date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LocalDate expression
	 */
	public LocalDateExpression plus(Period interval) {
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
	 * @return a LocalDate expression
	 */
	public LocalDateExpression plus(DateRepeatResult intervalExpression) {
		return new LocalDateExpression(new DatePlusDateRepeatExpression(this, intervalExpression));
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
	public BooleanExpression isLessThanOrEqual(LocalDate date) {
		return super.isLessThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied LocalDateResult.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(LocalDateResult dateExpression) {
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
	public BooleanExpression isGreaterThan(LocalDate date) {
		return super.isGreaterThan(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied LocalDateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThan(LocalDateResult dateExpression) {
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
	public BooleanExpression isLaterThan(LocalDate date) {
		return isGreaterThan(new LocalDateExpression(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied LocalDateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLaterThan(LocalDateResult dateExpression) {
		return isGreaterThan(dateExpression);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied LocalDate.
	 *
	 * @param date the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(LocalDate date) {
		return super.isGreaterThanOrEqual(date);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied LocalDateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(LocalDateResult dateExpression) {
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
	public BooleanExpression isLessThan(LocalDate value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isGreaterThan(LocalDate value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isLessThan(LocalDateResult value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isGreaterThan(LocalDateResult value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isIn(LocalDate... possibleValues) {
		List<LocalDateExpression> possVals = new ArrayList<LocalDateExpression>();
		for (LocalDate num : possibleValues) {
			possVals.add(value(num));
		}
		return isIn(possVals.toArray(new LocalDateExpression[]{}));
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
	public BooleanExpression isIn(Collection<? extends LocalDateResult> possibleValues) {
		//List<DateExpression> possVals = new ArrayList<DateExpression>();
		//for (LocalDate num : possibleValues) {
		//	possVals.add(value(num));
		//}
		return isIn(possibleValues.toArray(new LocalDateResult[]{}));
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
	public BooleanExpression isIn(LocalDateResult... possibleValues) {
		BooleanExpression isInExpr = new BooleanExpression(new DateIsInExpression(this, possibleValues));
		if (isInExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isInExpr);
		} else {
			return isInExpr;
		}
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
	public BooleanExpression isNotIn(LocalDateResult... possibleValues) {
		BooleanExpression isNotInExpr = new BooleanExpression(new DateIsNotInExpression(this, possibleValues));
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
	public LocalDateExpression ifDBNull(LocalDate alternative) {
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
	 * LocalDateResult.
	 *
	 * <p>
	 * This is a way of handling dates that should have a value but don't.
	 *
	 * @param alternative use this value if the expression evaluates to NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public LocalDateExpression ifDBNull(LocalDateResult alternative) {
		return new LocalDateExpression(
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
	public DateMaxExpression max() {
		return new DateMaxExpression(this);
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
	public DateMinExpression min() {
		return new DateMinExpression(this);
	}

	@Override
	public DBDate getQueryableDatatypeForExpressionValue() {
		return new DBDate();
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addDays(Integer daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addDays(Long daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addDays(Number daysToAdd) {
		return this.addDays(value(daysToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addDays(IntegerExpression daysToAdd) {
		return new LocalDateExpression(
				new DateAddIntegerDaysExpression(this, daysToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of days to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addDays(NumberExpression daysToAdd) {
		return new LocalDateExpression(
				new DateAddDaysExpression(this, daysToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of weeks to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addWeeks(int weeksToAdd) {
		return this.addWeeks(value(weeksToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of weeks to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addWeeks(IntegerExpression weeksToAdd) {
		return new LocalDateExpression(
				new DateAddIntegerWeeksExpression(this, weeksToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addMonths(Number monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addMonths(Integer monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addMonths(Long monthsToAdd) {
		return this.addMonths(value(monthsToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addMonths(IntegerExpression monthsToAdd) {
		return new LocalDateExpression(
				new DateAddIntegerMonthsExpression(this, monthsToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of months to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addMonths(NumberExpression monthsToAdd) {
		return new LocalDateExpression(
				new DateAddMonthsExpression(this, monthsToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of years to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addYears(int yearsToAdd) {
		return this.addYears(value(yearsToAdd));
	}

	/**
	 * LocalDate Arithmetic: add the supplied number of years to the date
	 * expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateExpression
	 */
	public LocalDateExpression addYears(IntegerExpression yearsToAdd) {
		return new LocalDateExpression(
				new DateAddIntegerYearsExpression(this, yearsToAdd));
	}

	/**
	 * LocalDate Arithmetic: get the days between the date expression and the
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
	public NumberExpression daysFrom(LocalDate dateToCompareTo) {
		return daysFrom(LocalDateExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDate Arithmetic: get the days between the date expression and the
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
	public NumberExpression daysFrom(LocalDateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDaysFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDate Arithmetic: get the weeks between the date expression and the
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
	public NumberExpression weeksFrom(LocalDate dateToCompareTo) {
		return weeksFrom(LocalDateExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDate Arithmetic: get the weeks between the date expression and the
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
	public NumberExpression weeksFrom(LocalDateExpression dateToCompareTo) {
		return new NumberExpression(
				new DateWeeksFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDate Arithmetic: get the months between the date expression and the
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
	public NumberExpression monthsFrom(LocalDate dateToCompareTo) {
		return monthsFrom(LocalDateExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDate Arithmetic: get the months between the date expression and the
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
	public NumberExpression monthsFrom(LocalDateResult dateToCompareTo) {
		return new NumberExpression(
				new DateMonthsFromExpression(this, dateToCompareTo));
	}

	/**
	 * LocalDate Arithmetic: get the years between the date expression and the
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
	public NumberExpression yearsFrom(LocalDate dateToCompareTo) {
		return yearsFrom(LocalDateExpression.value(dateToCompareTo));
	}

	/**
	 * LocalDate Arithmetic: get the years between the date expression and the
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
	public NumberExpression yearsFrom(LocalDateResult dateToCompareTo) {
		return new NumberExpression(
				new DateYearsFromExpression(this, dateToCompareTo));
	}

	/**
	 * Derive the first day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a LocalDate expression
	 */
	public LocalDateExpression firstOfMonth() {
		return this.addDays(this.day().minus(1).bracket().times(-1).integerResult());
	}

	/**
	 * Derive the last day of the month for this date expression
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a LocalDate expression
	 */
	public LocalDateExpression endOfMonth() {
		return new DateEndOfMonthExpression(this);
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
	public static BooleanExpression overlaps(LocalDate firstStartTime, LocalDate firstEndTime, LocalDate secondStartTime, LocalDate secondEndtime) {
		return LocalDateExpression.overlaps(LocalDateExpression.value(firstStartTime), LocalDateExpression.value(firstEndTime),
				LocalDateExpression.value(secondStartTime), LocalDateExpression.value(secondEndtime)
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
	public static BooleanExpression overlaps(LocalDateResult firstStartTime, LocalDateResult firstEndTime, LocalDateResult secondStartTime, LocalDateResult secondEndtime) {
		return LocalDateExpression.overlaps(new LocalDateExpression(firstStartTime), new LocalDateExpression(firstEndTime),
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
	public static BooleanExpression overlaps(LocalDateExpression firstStartTime, LocalDateExpression firstEndTime, LocalDateResult secondStartTime, LocalDateResult secondEndtime) {
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
	public static LocalDateExpression leastOf(LocalDate... possibleValues) {
		ArrayList<LocalDateExpression> possVals = new ArrayList<LocalDateExpression>();
		for (LocalDate num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new LocalDateExpression[]{}));
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
	public static LocalDateExpression leastOf(Collection<? extends LocalDateResult> possibleValues) {
		ArrayList<LocalDateExpression> possVals = new ArrayList<LocalDateExpression>();
		for (LocalDateResult num : possibleValues) {
			possVals.add(new LocalDateExpression(num));
		}
		return leastOf(possVals.toArray(new LocalDateExpression[]{}));
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
	public static LocalDateExpression leastOf(LocalDateResult... possibleValues) {
		LocalDateExpression leastExpr
				= new LocalDateExpression(new DateLeastOfExpression(possibleValues));
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
	public static LocalDateExpression greatestOf(LocalDate... possibleValues) {
		ArrayList<LocalDateExpression> possVals = new ArrayList<LocalDateExpression>();
		for (LocalDate num : possibleValues) {
			possVals.add(value(num));
		}
		return greatestOf(possVals.toArray(new LocalDateExpression[]{}));
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
	public static LocalDateExpression greatestOf(Collection<? extends LocalDateResult> possibleValues) {
		ArrayList<LocalDateExpression> possVals = new ArrayList<LocalDateExpression>();
		for (LocalDateResult num : possibleValues) {
			possVals.add(new LocalDateExpression(num));
		}
		return greatestOf(possVals.toArray(new LocalDateExpression[]{}));
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
	public static LocalDateExpression greatestOf(LocalDateResult... possibleValues) {
		LocalDateExpression greatestOf
				= new LocalDateExpression(new DateGreatestOfExpression(possibleValues));
		return greatestOf;
	}

	@Override
	public DBLocalDate asExpressionColumn() {
		return new DBLocalDate(this);
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
				.append(this.day());
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
				.append(this.year());
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
				.append(this.year());
		return commonFormatDateTime;
	}

	@Override
	public LocalDateExpression expression(LocalDate value) {
		return new LocalDateExpression(value);
	}

	@Override
	public LocalDateExpression expression(LocalDateResult value) {
		return new LocalDateExpression(value);
	}

	@Override
	public LocalDateResult expression(DBLocalDate value) {
		return new LocalDateExpression(value);
	}

	public LocalDateExpression setYear(int i) {
		return this.setYear(IntegerExpression.value(i));
	}

	public LocalDateExpression setYear(IntegerExpression i) {
		return new NewLocalDateExpression(i, this.month(), this.day());
	}

	public LocalDateExpression setMonth(Month month) {
		return setMonth(month.getValue());
	}

	public LocalDateExpression setMonth(int i) {
		return setMonth(AnyExpression.value(i));
	}

	public LocalDateExpression setMonth(IntegerExpression i) {
//		return this.addDays(IntegerExpression.value(i).minus(this.day().integerResult()));
		return new NewLocalDateExpression(this.year(), i, this.day());
	}

	public LocalDateExpression setDay(int i) {
//		return this.addDays(IntegerExpression.value(i).minus(this.day().integerResult()));
		return setDay(AnyExpression.value(i));
	}

	public LocalDateExpression setDay(IntegerExpression i) {
//		return this.addDays(IntegerExpression.value(i).minus(this.day().integerResult()));
		return new NewLocalDateExpression(this.year(), this.month(), i);
	}

	private static abstract class FunctionWithLocalDateResult extends LocalDateExpression implements CanBeWindowingFunctionWithFrame<LocalDateExpression> {

		private static final long serialVersionUID = 1L;

		FunctionWithLocalDateResult() {
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
		public LocalDateExpression.FunctionWithLocalDateResult copy() {
			LocalDateExpression.FunctionWithLocalDateResult newInstance;
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
		public WindowFunctionFramable<LocalDateExpression> over() {
			return new WindowFunctionFramable<LocalDateExpression>(new LocalDateExpression(this));
		}
	}

	private static abstract class DateExpressionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		DateExpressionWithNumberResult() {
			super();
		}

		DateExpressionWithNumberResult(LocalDateExpression only) {
			super(only);
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public abstract String toSQLString(DBDefinition db);
	}

	private static abstract class DateExpressionWithIntegerResult extends IntegerExpression {

		private static final long serialVersionUID = 1L;

		DateExpressionWithIntegerResult() {
			super();
		}

		DateExpressionWithIntegerResult(LocalDateExpression only) {
			super(only);
		}

		@Override
		public abstract String toSQLString(DBDefinition db);
	}

	private static abstract class DateDateExpressionWithBooleanResult extends BooleanExpression implements CanBeWindowingFunctionWithFrame<BooleanExpression> {

		private static final long serialVersionUID = 1L;

		protected LocalDateExpression first;
		protected LocalDateExpression second;
		private boolean requiresNullProtection = false;

		DateDateExpressionWithBooleanResult(LocalDateExpression first, LocalDateResult second) {
			this.first = first;
			this.second = new LocalDateExpression(second);
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

	private static abstract class DateDateExpressionWithDateRepeatResult extends DateRepeatExpression implements CanBeWindowingFunctionWithFrame<DateRepeatExpression> {

		private static final long serialVersionUID = 1L;

		protected LocalDateExpression first;
		protected LocalDateExpression second;
		private boolean requiresNullProtection = false;

		DateDateExpressionWithDateRepeatResult(LocalDateExpression first, LocalDateResult second) {
			this.first = first;
			this.second = new LocalDateExpression(second);
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DateDateExpressionWithDateRepeatResult copy() {
			DateDateExpressionWithDateRepeatResult newInstance;
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
		public LocalDateExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		public LocalDateExpression getSecond() {
			return second;
		}

		@Override
		public WindowFunctionFramable<DateRepeatExpression> over() {
			return new WindowFunctionFramable<DateRepeatExpression>(new DateRepeatExpression(this));
		}
	}

	private static abstract class DateDateRepeatArithmeticDateResult extends LocalDateExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateExpression first;
		protected DateRepeatExpression second;
		private boolean requiresNullProtection = false;

		DateDateRepeatArithmeticDateResult(LocalDateExpression first, DateRepeatResult second) {
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
		public LocalDateExpression getFirst() {
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

	private static abstract class DateArrayFunctionWithDateResult extends LocalDateExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateExpression column;
		protected final List<LocalDateResult> values = new ArrayList<LocalDateResult>();
		boolean nullProtectionRequired = false;

		DateArrayFunctionWithDateResult() {
		}

		DateArrayFunctionWithDateResult(LocalDateResult[] rightHandSide) {
			for (LocalDateResult dateResult : rightHandSide) {
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
			for (LocalDateResult val : values) {
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
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			for (LocalDateResult value : this.values) {
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
			for (LocalDateResult second : values) {
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
			for (LocalDateResult numer : values) {
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
				for (LocalDateResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class DateDateResultFunctionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private LocalDateExpression column;
		private List<LocalDateResult> values = new ArrayList<LocalDateResult>();
		boolean nullProtectionRequired = false;

		DateDateResultFunctionWithBooleanResult() {
		}

		DateDateResultFunctionWithBooleanResult(LocalDateExpression leftHandSide, LocalDateResult[] rightHandSide) {
			this.column = leftHandSide;
			for (LocalDateResult dateResult : rightHandSide) {
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
			for (LocalDateResult val : getValues()) {
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
				newInstance = getClass().getDeclaredConstructor().newInstance();
			} catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.getColumn().copy();
			for (LocalDateResult value : this.getValues()) {
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
			for (LocalDateResult val : getValues()) {
				if (val != null) {
					hashSet.addAll(val.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false || getColumn().isAggregator();
			for (LocalDateResult dater : getValues()) {
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
		protected LocalDateExpression getColumn() {
			return column;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the values
		 */
		protected List<LocalDateResult> getValues() {
			return values;
		}
	}

	private static abstract class DateDateFunctionWithDateResult extends LocalDateExpression {

		private static final long serialVersionUID = 1L;

		private LocalDateExpression first;
		private LocalDateResult second;

		DateDateFunctionWithDateResult(LocalDateExpression first) {
			this.first = first;
			this.second = null;
		}

		DateDateFunctionWithDateResult(LocalDateExpression first, LocalDateResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public abstract String toSQLString(DBDefinition db);

		@Override
		public DateDateFunctionWithDateResult copy() {
			DateDateFunctionWithDateResult newInstance;
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
		protected LocalDateExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected LocalDateResult getSecond() {
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

	private static abstract class DateFunctionWithDateResult extends LocalDateExpression {

		private static final long serialVersionUID = 1L;

		DateFunctionWithDateResult() {
			super();
		}

		DateFunctionWithDateResult(LocalDateExpression only) {
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

	private static abstract class DateIntegerExpressionWithLocalDateResult extends LocalDateExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateExpression first;
		protected IntegerExpression second;

		DateIntegerExpressionWithLocalDateResult() {
			this.first = null;
			this.second = null;
		}

		DateIntegerExpressionWithLocalDateResult(LocalDateExpression dateExp, IntegerExpression numbExp) {
			this.first = dateExp;
			this.second = numbExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public DateIntegerExpressionWithLocalDateResult copy() {
			DateIntegerExpressionWithLocalDateResult newInstance;
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

	private static abstract class DateNumberExpressionWithLocalDateResult extends LocalDateExpression {

		private static final long serialVersionUID = 1L;

		protected LocalDateExpression first;
		protected NumberExpression second;

		DateNumberExpressionWithLocalDateResult() {
			this.first = null;
			this.second = null;
		}

		DateNumberExpressionWithLocalDateResult(LocalDateExpression dateExp, NumberExpression numbExp) {
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

		protected LocalDateExpression first;
		protected LocalDateResult second;

		DateDateFunctionWithNumberResult() {
			this.first = null;
			this.second = null;
		}

		DateDateFunctionWithNumberResult(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
			this.first = dateExp;
			this.second = otherDateExp;
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public DateDateFunctionWithNumberResult copy() {
			DateDateFunctionWithNumberResult newInstance;
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

	private static class DateNullExpression extends LocalDateExpression {

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

	protected static class DateOnlyCurrentDateExpression extends FunctionWithLocalDateResult {

		public DateOnlyCurrentDateExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doCurrentDateOnlyTransform();
		}

		@Override
		public DateOnlyCurrentDateExpression copy() {
			return new DateOnlyCurrentDateExpression();
		}

	}

	protected static class DateCurrentDateExpression extends FunctionWithLocalDateResult {

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

	protected static class DateCurrentTimeExpression extends FunctionWithLocalDateResult {

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

	protected static class DateYearExpression extends DateExpressionWithIntegerResult {

		public DateYearExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doYearTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateYearExpression copy() {
			return new DateYearExpression((LocalDateExpression) getInnerResult().copy());
		}

	}

	protected static class DateMonthExpression extends DateExpressionWithIntegerResult {

		public DateMonthExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMonthTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateMonthExpression copy() {
			return new DateMonthExpression((LocalDateExpression) getInnerResult().copy());
		}

	}

	protected static class DateDayExpression extends DateExpressionWithIntegerResult {

		public DateDayExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDayTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateDayExpression copy() {
			return new DateDayExpression((LocalDateExpression) getInnerResult().copy());
		}

	}

	protected static class DateHourExpression extends DateExpressionWithNumberResult {

		public DateHourExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doHourTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateHourExpression copy() {
			return new DateHourExpression((LocalDateExpression) getInnerResult().copy());
		}

	}

	protected static class DateMinuteExpression extends DateExpressionWithNumberResult {

		public DateMinuteExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doMinuteTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateMinuteExpression copy() {
			return new DateMinuteExpression((LocalDateExpression) getInnerResult().copy());
		}

	}

	protected static class DateSecondExpression extends DateExpressionWithNumberResult {

		public DateSecondExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateSecondExpression copy() {
			return new DateSecondExpression((LocalDateExpression) getInnerResult().copy());
		}
	}

	protected static class DateSubsecondExpression extends DateExpressionWithNumberResult {

		public DateSubsecondExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doSubsecondTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateSubsecondExpression copy() {
			return new DateSubsecondExpression((LocalDateExpression) getInnerResult().copy());
		}

	}

	protected static class DateIsExpression extends DateDateExpressionWithBooleanResult {

		public DateIsExpression(LocalDateExpression first, LocalDateResult second) {
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

		public DateIsNotExpression(LocalDateExpression first, LocalDateResult second) {
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

		public DateIsLessThanExpression(LocalDateExpression first, LocalDateResult second) {
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

		public DateGetDateRepeatFromExpression(LocalDateExpression first, LocalDateResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateMinusToDateRepeatTransformation(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final LocalDateExpression left = getFirst();
				final LocalDateExpression right = new LocalDateExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullString(),
								StringExpression.value(INTERVAL_PREFIX)
										.append(left.year().minus(right.year()).bracket()).append(YEAR_SUFFIX)
										.append(left.month().minus(right.month()).bracket()).append(MONTH_SUFFIX)
										.append(left.day().minus(right.day()).bracket()).append(DAY_SUFFIX)
						//										.append(left.hour().minus(right.hour()).bracket()).append(HOUR_SUFFIX)
						//										.append(left.minute().minus(right.minute()).bracket()).append(MINUTE_SUFFIX)
						//										.append(left.second().minus(right.second()).bracket())
						//										.append(".")
						//										.append(left.subsecond().minus(right.subsecond()).absoluteValue().stringResult().substringAfter("."))
						//										.append(SECOND_SUFFIX)
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

		public DateMinusDateRepeatExpression(LocalDateExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateMinusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final LocalDateExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullLocalDate(),
								left.addYears(right.getYears().times(-1))
										.addMonths(right.getMonths().times(-1))
										.addDays(right.getDays().times(-1))
						//										.addHours(right.getHours().times(-1))
						//										.addMinutes(right.getMinutes().times(-1))
						//										.addSeconds(right.getSeconds().times(-1))
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

		public DatePlusDateRepeatExpression(LocalDateExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransformation(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDatePlusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				final LocalDateExpression left = getFirst();
				final DateRepeatExpression right = new DateRepeatExpression(getSecond());
				return BooleanExpression.anyOf(left.isNull(), right.isNull())
						.ifThenElse(
								nullLocalDate(),
								left.addYears(right.getYears())
										.addMonths(right.getMonths())
										.addDays(right.getDays())
						//										.addHours(right.getHours())
						//										.addMinutes(right.getMinutes())
						//										.addSeconds(right.getSeconds())
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

		public DateIsLessThanOrEqualExpression(LocalDateExpression first, LocalDateResult second) {
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

		public DateIsGreaterThanExpression(LocalDateExpression first, LocalDateResult second) {
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

		public DateIsGreaterThanOrEqualExpression(LocalDateExpression first, LocalDateResult second) {
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

	private static class NewLocalDateExpression extends LocalDateExpression {

		private final static long serialVersionUID = 1l;

		private final IntegerExpression yearExpression;
		private final IntegerExpression monthExpression;
		private final IntegerExpression dayExpression;

		public NewLocalDateExpression(IntegerExpression year, IntegerExpression month, IntegerExpression day) {
			this.yearExpression = year;
			this.monthExpression = month;
			this.dayExpression = day;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			final String doNewLocalDateFromYearMonthDayTransform = db.doNewLocalDateFromYearMonthDayTransform(
					yearExpression.toSQLString(db),//.stringResult().toSQLString(db),
					monthExpression.toSQLString(db),//.stringResult().leftPad("0", 2).toSQLString(db),
					dayExpression.toSQLString(db)//.stringResult().leftPad("0", 2).toSQLString(db)
			);
			return doNewLocalDateFromYearMonthDayTransform;
		}

		@Override
		public LocalDateExpression copy() {
			return new NewLocalDateExpression(yearExpression.copy(), monthExpression.copy(), dayExpression.copy());
		}

		@Override
		public boolean isPurelyFunctional() {
			List<AnyExpression<?, ?, ?>> exprs = new ArrayList<AnyExpression<?, ?, ?>>();
			exprs.addAll(Arrays.asList(new AnyExpression<?, ?, ?>[]{yearExpression, monthExpression, dayExpression}));
			Optional<Boolean> reduced = exprs.stream().map((t) -> t.isPurelyFunctional()).reduce((t, u) -> t || u);
			return reduced.isPresent() && reduced.get();
		}

		@Override
		public boolean isComplexExpression() {
			List<AnyExpression<?, ?, ?>> exprs = new ArrayList<AnyExpression<?, ?, ?>>();
			exprs.addAll(Arrays.asList(new AnyExpression<?, ?, ?>[]{yearExpression, monthExpression, dayExpression}));
			Optional<Boolean> reduced = exprs.stream().map((t) -> t.isComplexExpression()).reduce((t, u) -> t || u);
			return reduced.isPresent() && reduced.get();
		}

		@Override
		public boolean isAggregator() {
			List<AnyExpression<?, ?, ?>> exprs = new ArrayList<AnyExpression<?, ?, ?>>();
			exprs.addAll(Arrays.asList(new AnyExpression<?, ?, ?>[]{yearExpression, monthExpression, dayExpression}));
			Optional<Boolean> reduced = exprs.stream().map((t) -> t.isAggregator()).reduce((t, u) -> t || u);
			return reduced.isPresent() && reduced.get();
		}

		@Override
		protected boolean isNullSafetyTerminator() {
			List<AnyExpression<?, ?, ?>> exprs = new ArrayList<AnyExpression<?, ?, ?>>();
			exprs.addAll(Arrays.asList(new AnyExpression<?, ?, ?>[]{yearExpression, monthExpression, dayExpression}));
			Optional<Boolean> reduced = exprs.stream().map((t) -> t.isNullSafetyTerminator()).reduce((t, u) -> t || u);
			return reduced.isPresent() && reduced.get();
		}

		@Override
		public boolean isWindowingFunction() {
			List<AnyExpression<?, ?, ?>> exprs = new ArrayList<AnyExpression<?, ?, ?>>();
			exprs.addAll(Arrays.asList(new AnyExpression<?, ?, ?>[]{yearExpression, monthExpression, dayExpression}));
			Optional<Boolean> reduced = exprs.stream().map((t) -> t.isWindowingFunction()).reduce((t, u) -> t || u);
			return reduced.isPresent() && reduced.get();
		}
	}

	protected class DateIsInExpression extends DateDateResultFunctionWithBooleanResult {

		public DateIsInExpression(LocalDateExpression leftHandSide, LocalDateResult[] rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<String>();
			for (LocalDateResult value : getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		public DateIsInExpression copy() {
			final List<LocalDateResult> values = getValues();
			final List<LocalDateResult> newValues = new ArrayList<>();
			for (LocalDateResult value : values) {
				newValues.add(value.copy());
			}
			return new DateIsInExpression(
					getColumn().copy(),
					newValues.toArray(new LocalDateResult[]{}));
		}

	}

	protected class DateIsNotInExpression extends DateDateResultFunctionWithBooleanResult {

		public DateIsNotInExpression(LocalDateExpression leftHandSide, LocalDateResult[] rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<String>();
			for (LocalDateResult value : getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doNotInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		public DateIsNotInExpression copy() {
			final List<LocalDateResult> values = getValues();
			final List<LocalDateResult> newValues = new ArrayList<>();
			for (LocalDateResult value : values) {
				newValues.add(value.copy());
			}
			return new DateIsNotInExpression(
					getColumn().copy(),
					newValues.toArray(new LocalDateResult[]{}));
		}

	}

	protected static class DateIfDBNullExpression extends DateDateFunctionWithDateResult {

		public DateIfDBNullExpression(LocalDateExpression first, LocalDateResult second) {
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

	public static class DateMaxExpression extends DateFunctionWithDateResult implements CanBeWindowingFunctionWithFrame<LocalDateExpression> {

		public DateMaxExpression(LocalDateExpression only) {
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
			return new DateMaxExpression((LocalDateExpression) getInnerResult().copy());
		}

		@Override
		public WindowFunctionFramable<LocalDateExpression> over() {
			return new WindowFunctionFramable<LocalDateExpression>(new LocalDateExpression(this));
		}
	}

	public static class DateMinExpression extends DateFunctionWithDateResult implements CanBeWindowingFunctionWithFrame<LocalDateExpression> {

		public DateMinExpression(LocalDateExpression only) {
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
			return new DateMinExpression((LocalDateExpression) getInnerResult().copy());
		}

		@Override
		public WindowFunctionFramable<LocalDateExpression> over() {
			return new WindowFunctionFramable<LocalDateExpression>(new LocalDateExpression(this));
		}
	}

	protected static class DateAddSecondsExpression extends DateNumberExpressionWithLocalDateResult {

		public DateAddSecondsExpression(LocalDateExpression dateExp, NumberExpression numbExp) {
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
		public DateAddSecondsExpression copy() {
			return new DateAddSecondsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerSecondsExpression extends DateIntegerExpressionWithLocalDateResult {

		public DateAddIntegerSecondsExpression(LocalDateExpression dateExp, IntegerExpression numbExp) {
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
		public DateAddIntegerSecondsExpression copy() {
			return new DateAddIntegerSecondsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerMinutesExpression extends DateIntegerExpressionWithLocalDateResult {

		public DateAddIntegerMinutesExpression(LocalDateExpression dateExp, IntegerExpression numbExp) {
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
		public DateAddIntegerMinutesExpression copy() {
			return new DateAddIntegerMinutesExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerDaysExpression extends DateIntegerExpressionWithLocalDateResult {

		public DateAddIntegerDaysExpression(LocalDateExpression dateExp, IntegerExpression numbExp) {
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
		public DateAddIntegerDaysExpression copy() {
			return new DateAddIntegerDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddDaysExpression extends DateNumberExpressionWithLocalDateResult {

		public DateAddDaysExpression(LocalDateExpression dateExp, NumberExpression numbExp) {
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
		public DateAddDaysExpression copy() {
			return new DateAddDaysExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerHoursExpression extends DateIntegerExpressionWithLocalDateResult {

		public DateAddIntegerHoursExpression(LocalDateExpression dateExp, IntegerExpression numbExp) {
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
		public DateAddIntegerHoursExpression copy() {
			return new DateAddIntegerHoursExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerWeeksExpression extends DateIntegerExpressionWithLocalDateResult {

		public DateAddIntegerWeeksExpression(LocalDateExpression dateExp, IntegerExpression numbExp) {
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
		public DateAddIntegerWeeksExpression copy() {
			return new DateAddIntegerWeeksExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerMonthsExpression extends DateIntegerExpressionWithLocalDateResult {

		public DateAddIntegerMonthsExpression(LocalDateExpression dateExp, IntegerExpression numbExp) {
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
		public DateAddIntegerMonthsExpression copy() {
			return new DateAddIntegerMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddMonthsExpression extends DateNumberExpressionWithLocalDateResult {

		public DateAddMonthsExpression(LocalDateExpression dateExp, NumberExpression numbExp) {
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
		public DateAddMonthsExpression copy() {
			return new DateAddMonthsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateAddIntegerYearsExpression extends DateIntegerExpressionWithLocalDateResult {

		public DateAddIntegerYearsExpression(LocalDateExpression dateExp, IntegerExpression numbExp) {
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
		public DateAddIntegerYearsExpression copy() {
			return new DateAddIntegerYearsExpression(first.copy(), second.copy());
		}
	}

	protected static class DateDaysFromExpression extends DateDateFunctionWithNumberResult {

		public DateDaysFromExpression(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
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

		public DateWeeksFromExpression(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
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

		public DateMonthsFromExpression(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
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

		public DateYearsFromExpression(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
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

		public DateHoursFromExpression(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
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

		public DateMinutesFromExpression(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
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

		public DateSecondsFromExpression(LocalDateExpression dateExp, LocalDateResult otherDateExp) {
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

	protected static class DateEndOfMonthExpression extends LocalDateExpression {

		public DateEndOfMonthExpression(LocalDateResult dateVariable) {
			super(dateVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			try {
				return db.doEndOfMonthTransform(this.getInnerResult().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				LocalDateExpression only = (LocalDateExpression) getInnerResult();
				return only
						.addDays(only.day().minus(1).bracket().times(-1).integerResult())
						.addMonths(1).addDays(-1).toSQLString(db);
			}
		}

		@Override
		public DateEndOfMonthExpression copy() {
			return new DateEndOfMonthExpression((LocalDateResult) getInnerResult().copy());
		}
	}

	protected static class DateDayOfWeekExpression extends DateExpressionWithNumberResult {

		public DateDayOfWeekExpression(LocalDateExpression only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doDayOfWeekTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public DateDayOfWeekExpression copy() {
			return new DateDayOfWeekExpression((LocalDateExpression) getInnerResult().copy());
		}
	}

	protected static class DateLeastOfExpression extends DateArrayFunctionWithDateResult {

		public DateLeastOfExpression(LocalDateResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (LocalDateResult num : this.values) {
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
			List<LocalDateResult> newValues = new ArrayList<>();
			for (LocalDateResult value : values) {
				newValues.add(value.copy());
			}
			return new DateLeastOfExpression(newValues.toArray(new LocalDateResult[]{}));
		}
	}

	protected static class DateGreatestOfExpression extends DateArrayFunctionWithDateResult {

		public DateGreatestOfExpression(LocalDateResult[] rightHandSide) {
			super(rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<String>();
			for (LocalDateResult num : this.values) {
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
			List<LocalDateResult> newValues = new ArrayList<>();
			for (LocalDateResult value : values) {
				newValues.add(value.copy());
			}
			return new DateGreatestOfExpression(newValues.toArray(new LocalDateResult[]{}));
		}
	}

	public static WindowFunctionFramable<LocalDateExpression> firstValue() {
		return new FirstValueExpression().over();
	}

	public static class FirstValueExpression extends BooleanExpression implements CanBeWindowingFunctionWithFrame<LocalDateExpression> {

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
		public WindowFunctionFramable<LocalDateExpression> over() {
			return new WindowFunctionFramable<LocalDateExpression>(new LocalDateExpression(this));
		}

	}

	public static WindowFunctionFramable<LocalDateExpression> lastValue() {
		return new LastValueExpression().over();
	}

	public static class LastValueExpression extends LocalDateExpression implements CanBeWindowingFunctionWithFrame<LocalDateExpression> {

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
		public LocalDateExpression copy() {
			return new LastValueExpression();
		}

		@Override
		public WindowFunctionFramable<LocalDateExpression> over() {
			return new WindowFunctionFramable<LocalDateExpression>(new LocalDateExpression(this));
		}

	}

	public static WindowFunctionFramable<LocalDateExpression> nthValue(IntegerExpression indexExpression) {
		return new NthValueExpression(indexExpression).over();
	}

	public static class NthValueExpression extends LocalDateExpression implements CanBeWindowingFunctionWithFrame<LocalDateExpression> {

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
		public WindowFunctionFramable<LocalDateExpression> over() {
			return new WindowFunctionFramable<LocalDateExpression>(new LocalDateExpression(this));
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> previousRowValue() {
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> lag() {
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> lag(IntegerExpression offset) {
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> lag(IntegerExpression offset, LocalDateExpression defaultExpression) {
		return new LagExpression(this, offset, defaultExpression).over();
	}

	/**
	 * Synonym for lead.
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> nextRowValue() {
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> lead() {
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> lead(IntegerExpression offset) {
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
	public WindowFunctionRequiresOrderBy<LocalDateExpression> lead(IntegerExpression offset, LocalDateExpression defaultExpression) {
		return new LeadExpression(this, offset, defaultExpression).over();
	}

	private static abstract class LagLeadExpression extends LocalDateExpression implements CanBeWindowingFunctionRequiresOrderBy<LocalDateExpression> {

		private static final long serialVersionUID = 1L;

		protected LocalDateExpression first;
		protected IntegerExpression second;
		protected LocalDateExpression third;

		LagLeadExpression(LocalDateExpression first, IntegerExpression second, LocalDateExpression third) {
			this.first = first;
			this.second = second == null ? value(1) : second;
			this.third = third == null ? nullLocalDate() : third;
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
		protected LocalDateExpression getFirst() {
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
		protected LocalDateExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
		}

		@Override
		public WindowFunctionRequiresOrderBy<LocalDateExpression> over() {
			return new WindowFunctionRequiresOrderBy<>(new LocalDateExpression(this));
		}
	}

	public class LagExpression extends LagLeadExpression {

		private static final long serialVersionUID = 1L;

		public LagExpression(LocalDateExpression first, IntegerExpression second, LocalDateExpression third) {
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

		public LeadExpression(LocalDateExpression first, IntegerExpression second, LocalDateExpression third) {
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
