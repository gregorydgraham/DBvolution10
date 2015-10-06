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

import nz.co.gregs.dbvolution.results.RangeComparable;
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
import java.util.TimeZone;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.results.InComparable;
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
 * @author Gregory Graham
 */
public class DateExpression implements DateResult, RangeComparable<DateResult>, InComparable<DateResult>, ExpressionColumn<DBDate> {

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

	private DateResult date1;
	private boolean needsNullProtection = false;

	/**
	 * Default Constructor
	 */
	protected DateExpression() {
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
		date1 = dateVariable;
		if (date1 == null || date1.getIncludesNull()) {
			needsNullProtection = true;
		}
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
		date1 = new DBDate(date);
		if (date == null || date1.getIncludesNull()) {
			needsNullProtection = true;
		}
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return date1.toSQLString(db);
	}

	@Override
	public DateExpression copy() {
		return new DateExpression(this.date1);
	}

	@Override
	public boolean isPurelyFunctional() {
		if (date1 == null) {
			return true;
		} else {
			return date1.isPurelyFunctional();
		}
	}

	/**
	 * Create An Appropriate Expression Object For This Object
	 *
	 * <p>
	 * The expression framework requires a *Expression to work with. The easiest
	 * way to get that is the {@code DBRow.column()} method.
	 *
	 * <p>
	 * However if you wish your expression to start with a literal value it is a
	 * little trickier.
	 *
	 * <p>
	 * This method provides the easy route to a *Expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param date a literal date to be used in the expression
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static DateExpression value(Date date) {
		return new DateExpression(date);
	}

	static DateExpression nullExpression() {
		return new DateExpression() {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().getNull();
			}

		};
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
	public static DateExpression currentDateOnly() {
		return new DateExpression(
				new FunctionWithDateResult() {

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doCurrentDateOnlyTransform();
					}

					@Override
					String getFunctionName(DBDatabase db) {
						return "";
					}
				});
	}

	/**
	 * Creates a date expression that returns the current date on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current day and time according to
	 * the database.
	 *
	 * @return a date expression of the current database timestamp.
	 */
	public static DateExpression currentDate() {
		return new DateExpression(
				new FunctionWithDateResult() {

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doCurrentDateTimeTransform();
					}

					@Override
					String getFunctionName(DBDatabase db) {
						return "";
					}
				});
	}

	/**
	 * Creates a date expression that returns the current time on the database.
	 *
	 * <p>
	 * That is to say the expression returns the current time, according to the
	 * database, with the date set to database's zero date.
	 *
	 * @return a date expression of only the time part of the current database
	 * timestamp.
	 */
	public static DateExpression currentTime() {
		return new DateExpression(
				new FunctionWithDateResult() {

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doCurrentTimeTransform();
					}

					@Override
					String getFunctionName(DBDatabase db) {
						return "";
					}
				});
	}

	/**
	 * Creates an SQL expression that returns the year part of this date
	 * expression.
	 *
	 * @return the year of this date expression as a number.
	 */
	public NumberExpression year() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doYearTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that tests the year part of this date expression.
	 *
	 * @param yearRequired the year to used in the expression
	 * @return a BooleanExpression that is TRUE if the year is the same as the
	 * example supplied.
	 */
	public BooleanExpression yearIs(Number yearRequired) {
		return this.year().is(yearRequired);
	}

	/**
	 * Creates an SQL expression that tests the year part of this date expression.
	 *
	 * @param yearRequired the year to be used in the expression
	 * @return a BooleanExpression that is TRUE if the year is the same as the
	 * example supplied.
	 */
	public BooleanExpression yearIs(NumberResult yearRequired) {
		return this.year().is(yearRequired);
	}

	/**
	 * Creates an SQL expression that returns the month part of this date
	 * expression.
	 *
	 * @return the month of this date expression as a number.
	 */
	public NumberExpression month() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMonthTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that tests the month part of this date
	 * expression.
	 *
	 * @param monthRequired the month to be used in the expression
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
	 * @return a BooleanExpression that is TRUE if the month is the same as the
	 * example supplied.
	 */
	public BooleanExpression monthIs(NumberResult monthRequired) {
		return this.month().is(monthRequired);
	}

	/**
	 * Returns the day part of the date.
	 *
	 * <p>
	 * Day in this sense is the number of the day within the month: that is the 25
	 * part of Monday 25th of August 2014
	 *
	 * @return a NumberExpression that will provide the day of this date.
	 */
	public NumberExpression day() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doDayTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that tests the day part of this date expression.
	 *
	 * @param dayRequired the day to be used in the expression
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
	 * @return a BooleanExpression that is TRUE if the day is the same as the
	 * example supplied.
	 */
	public BooleanExpression dayIs(NumberResult dayRequired) {
		return this.day().is(dayRequired);
	}

	/**
	 * Creates an SQL expression that returns the hour part of this date
	 * expression.
	 *
	 * @return the hour of this date expression as a number.
	 */
	public NumberExpression hour() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doHourTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that tests the hour part of this date expression.
	 *
	 * @param hourRequired the hour to be used in the expression
	 * @return a BooleanExpression that is TRUE if the hour is the same as the
	 * example supplied.
	 */
	public BooleanExpression hourIs(Number hourRequired) {
		return this.hour().is(hourRequired);
	}

	/**
	 * Creates an SQL expression that tests the hour part of this date expression.
	 *
	 * @param hourRequired the hour to be compared to.
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
	 * @return the minute of this date expression as a number.
	 */
	public NumberExpression minute() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMinuteTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that tests the minute part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute to be compared to
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
	 * @return a BooleanExpression that is TRUE if the minute is the same as the
	 * example supplied.
	 */
	public BooleanExpression minuteIs(NumberResult minuteRequired) {
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
	 * @return the second of this date expression as a number.
	 */
	public NumberExpression second() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doSecondTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that returns the fractions of a second part of
	 * this date expression.
	 *
	 * <p>
	 * Contains only the fractional part of the seconds, that is always between 0
	 * and 1, use {@link #second()} to retrieve the integer part.
	 *
	 * @return the second of this date expression as a number.
	 */
	public NumberExpression subsecond() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doSubsecondTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that tests the second part of this date
	 * expression.
	 *
	 * @param minuteRequired the minute required
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
	 * @param minuteRequired the minute that the expression must match
	 * @return a BooleanExpression that is TRUE if the second is the same as the
	 * example supplied.
	 */
	public BooleanExpression secondIs(NumberResult minuteRequired) {
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
	 * @return a BooleanExpression comparing the date and this DateExpression.
	 */
	public BooleanExpression is(Date date) {
		return is(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * @param dateExpression the date the expression must match
	 * @return a BooleanExpression comparing the DateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression is(DateResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new DateDateExpressionWithBooleanResult(this, dateExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
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
	 * @param dateExpression the date the expression must not match
	 * @return a BooleanExpression comparing the DateResult and this
	 * DateExpression.
	 */
	@Override
	public BooleanExpression isNot(DateResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new DateDateExpressionWithBooleanResult(this, dateExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " <> ";
			}
		});
		if (isExpr.getIncludesNull()) {
			return BooleanExpression.isNull(this);
		} else {
			return isExpr;
		}
	}

	/**
	 * Returns FALSE if this expression evaluates to NULL, otherwise TRUE.
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	/**
	 * Returns TRUE if this expression evaluates to NULL, otherwise FALSE.
	 *
	 * @return a BooleanExpression
	 */
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
	 * @return a boolean expression representing the required comparison
	 */
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(Date lowerBound, DateResult upperBound) {
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(DateResult lowerBound, Date upperBound) {
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(Date lowerBound, Date upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
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
	 * @return a boolean expression representing the required comparison
	 */
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(Date lowerBound, DateResult upperBound) {
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(DateResult lowerBound, Date upperBound) {
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(Date lowerBound, Date upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
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
	 * @return a boolean expression representing the required comparison
	 */
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(Date lowerBound, DateResult upperBound) {
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(DateResult lowerBound, Date upperBound) {
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(Date lowerBound, Date upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than to the supplied date.
	 *
	 * @param date the date this expression must not exceed
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLessThan(Date date) {
		return isLessThan(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than to the supplied date.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThan(DateResult dateExpression) {
		return new BooleanExpression(new DateExpression.DateDateExpressionWithBooleanResult(this, dateExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " < ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Create a DateRepeat value representing the difference between this date
	 * expression and the one provided
	 *
	 * @param date the other date which defines this DateRepeat
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
	 * @return DateRepeat expression
	 */
	public DateRepeatExpression getDateRepeatFrom(DateResult dateExpression) {
		return new DateRepeatExpression(new DateDateExpressionWithDateRepeatResult(this, dateExpression) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateMinusToDateRepeatTransformation(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} else {
					final DateExpression left = getFirst();
					final DateExpression right = new DateExpression(getSecond());
					return BooleanExpression.anyOf(left.isNull(), right.isNull())
							.ifThenElse(
									StringExpression.nullExpression(),
									StringExpression.value(INTERVAL_PREFIX)
									.append(left.year().minus(right.year()).bracket()).append(YEAR_SUFFIX)
									.append(left.month().minus(right.month()).bracket()).append(MONTH_SUFFIX)
									.append(left.day().minus(right.day()).bracket()).append(DAY_SUFFIX)
									.append(left.hour().minus(right.hour()).bracket()).append(HOUR_SUFFIX)
									.append(left.minute().minus(right.minute()).bracket()).append(MINUTE_SUFFIX)
									.append(left.second().minus(right.second()).bracket()).append(SECOND_SUFFIX)
							).toSQLString(db);
				}
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Subtract the period/duration provided from this date expression to get an
	 * offset date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
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
	 * @return a Date expression
	 */
	public DateExpression minus(DateRepeatResult intervalExpression) {
		return new DateExpression(new DateDateRepeatArithmeticDateResult(this, intervalExpression) {
			@Override
			protected String doExpressionTransformation(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateMinusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} else {
					final DateExpression left = getFirst();
					final DateRepeatExpression right = new DateRepeatExpression(getSecond());
					return BooleanExpression.anyOf(left.isNull(), right.isNull())
							.ifThenElse(
									DateExpression.nullExpression(),
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
		});
	}

	/**
	 * Add the period/duration provided from this date expression to get an offset
	 * date.
	 *
	 * @param interval the amount of time this date needs to be offset by.
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
	 * @return a Date expression
	 */
	public DateExpression plus(DateRepeatResult intervalExpression) {
		return new DateExpression(new DateDateRepeatArithmeticDateResult(this, intervalExpression) {
			@Override
			protected String doExpressionTransformation(DBDatabase db) {
				return db.getDefinition().doDatePlusDateRepeatTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied date.
	 *
	 * @param date the date this expression must not exceed
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLessThanOrEqual(Date date) {
		return isLessThanOrEqual(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied DateResult.
	 *
	 * @param dateExpression the date this expression must not exceed
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(DateResult dateExpression) {
		return new BooleanExpression(new DateDateExpressionWithBooleanResult(this, dateExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " <= ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied date.
	 *
	 * @param date the date this expression must be compared to
	 * @return an expression that will evaluate to a greater than operation
	 */
	public BooleanExpression isGreaterThan(Date date) {
		return isGreaterThan(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied DateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThan(DateResult dateExpression) {
		return new BooleanExpression(new DateDateExpressionWithBooleanResult(this, dateExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " > ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied Date.
	 *
	 * @param date the date this expression must be compared to
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isGreaterThanOrEqual(Date date) {
		return isGreaterThanOrEqual(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied DateResult.
	 *
	 * @param dateExpression the date this expression must be compared to
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(DateResult dateExpression) {
		return new BooleanExpression(new DateDateExpressionWithBooleanResult(this, dateExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " >= ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
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
	public BooleanExpression isLessThan(Date value, BooleanExpression fallBackWhenEquals) {
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
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThan(Date value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
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
	 * @return a boolean expression representing the required comparison
	 */
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
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isIn(Collection<? extends Date> possibleValues) {
		List<DateExpression> possVals = new ArrayList<DateExpression>();
		for (Date num : possibleValues) {
			possVals.add(value(num));
		}
		return isIn(possVals.toArray(new DateExpression[]{}));
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
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isIn(DateResult... possibleValues) {
		BooleanExpression isInExpr = new BooleanExpression(new DateDateResultFunctionWithBooleanResult(this, possibleValues) {

			@Override
			public String toSQLString(DBDatabase db) {
				List<String> sqlValues = new ArrayList<String>();
				for (DateResult value : getValues()) {
					sqlValues.add(value.toSQLString(db));
				}
				return db.getDefinition().doInTransform(getColumn().toSQLString(db), sqlValues);
			}

			@Override
			protected String getFunctionName(DBDatabase db) {
				return " IN ";
			}
		});
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
	 * @return a boolean expression representing the required comparison
	 */
	public DateExpression ifDBNull(Date alternative) {
		return new DateExpression(
				new DateExpression.DateDateFunctionWithDateResult(this, new DateExpression(alternative)) {
					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getIfNullFunctionName();
					}

					@Override
					public boolean getIncludesNull() {
						return false;
					}
				});
	}

	/**
	 * Creates and expression that replaces a NULL result with the supplied
	 * DateResult.
	 *
	 * <p>
	 * This is a way of handling dates that should have a value but don't.
	 *
	 * @param alternative use this value if the expression evaluates to NULL
	 * @return a boolean expression representing the required comparison
	 */
	public DateExpression ifDBNull(DateResult alternative) {
		return new DateExpression(
				new DateExpression.DateDateFunctionWithDateResult(this, alternative) {

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doDateIfNullTransform(this.getFirst().toSQLString(db), getSecond().toSQLString(db));
					}

					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getIfNullFunctionName();
					}

					@Override
					public boolean getIncludesNull() {
						return false;
					}
				});
	}

	/**
	 * Aggregates the dates found in a query as a count of items.
	 *
	 * @return a number expression.
	 */
	public NumberExpression count() {
		return new NumberExpression(new DateFunctionWithNumberResult(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getCountFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	/**
	 * Aggregates the dates found in a query to find the maximum date in the
	 * selection.
	 *
	 * <p>
	 * For use in expression columns and {@link DBReport}.
	 *
	 * @return a number expression.
	 */
	public DateExpression max() {
		return new DateExpression(new DateFunctionWithDateResult(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getMaxFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Aggregates the dates found in a query to find the minimum date in the
	 * selection.
	 *
	 * <p>
	 * For use in expression columns and {@link DBReport}.
	 *
	 * @return a number expression.
	 */
	public DateExpression min() {
		return new DateExpression(new DateFunctionWithDateResult(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getMinFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	@Override
	public DBDate getQueryableDatatypeForExpressionValue() {
		return new DBDate();
	}

	@Override
	public boolean isAggregator() {
		return date1 == null ? false : date1.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return date1 == null ? new HashSet<DBRow>() : date1.getTablesInvolved();
	}

	@Override
	public boolean getIncludesNull() {
		return needsNullProtection;
	}

	/**
	 * Date Arithmetic: add the supplied number of seconds to the date expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * @return a DateExpression
	 */
	public DateExpression addSeconds(int secondsToAdd) {
		return this.addSeconds(new NumberExpression(secondsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of seconds to the date expression.
	 *
	 * <p>
	 * Negative seconds are supported.
	 *
	 * @param secondsToAdd seconds to offset by
	 * @return a DateExpression
	 */
	public DateExpression addSeconds(NumberExpression secondsToAdd) {
		return new DateExpression(
				new DateNumberExpressionWithDateResult(this, secondsToAdd) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doAddSecondsTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

//	public DateExpression addMilliseconds(int millisecondsToAdd) {
//		return addMilliseconds(NumberExpression.value(millisecondsToAdd));
//	}
//
//	public DateExpression addMilliseconds(long millisecondsToAdd) {
//		return addMilliseconds(NumberExpression.value(millisecondsToAdd));
//	}
//
//	public DateExpression addMilliseconds(NumberExpression millisecondsToAdd) {
//		return new DateExpression(
//				new DBBinaryDateNumberFunctionWithDateResult(this, millisecondsToAdd) {
//
//					@Override
//					public boolean getIncludesNull() {
//						return false;
//					}
//
//					@Override
//					public String toSQLString(DBDatabase db) {
//						return db.getDefinition().doAddMillisecondsTransform(first.toSQLString(db), second.toSQLString(db));
//					}
//				});
//	}
	/**
	 * Date Arithmetic: add the supplied number of minutes to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * @return a DateExpression
	 */
	public DateExpression addMinutes(int minutesToAdd) {
		return this.addMinutes(new NumberExpression(minutesToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of minutes to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd minutes to offset by
	 * @return a DateExpression
	 */
	public DateExpression addMinutes(NumberExpression minutesToAdd) {
		return new DateExpression(
				new DateNumberExpressionWithDateResult(this, minutesToAdd) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doAddMinutesTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * @return a DateExpression
	 */
	public DateExpression addDays(int daysToAdd) {
		return this.addDays(new NumberExpression(daysToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of days to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param daysToAdd days to offset by
	 * @return a DateExpression
	 */
	public DateExpression addDays(NumberExpression daysToAdd) {
		return new DateExpression(
				new DateNumberExpressionWithDateResult(this, daysToAdd) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doAddDaysTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: add the supplied number of hours to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * @return a DateExpression
	 */
	public DateExpression addHours(int hoursToAdd) {
		return this.addHours(new NumberExpression(hoursToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of hours to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd hours to offset by
	 * @return a DateExpression
	 */
	public DateExpression addHours(NumberExpression hoursToAdd) {
		return new DateExpression(
				new DateNumberExpressionWithDateResult(this, hoursToAdd) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doAddHoursTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: add the supplied number of weeks to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * @return a DateExpression
	 */
	public DateExpression addWeeks(int weeksToAdd) {
		return this.addWeeks(new NumberExpression(weeksToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of weeks to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd weeks to offset by
	 * @return a DateExpression
	 */
	public DateExpression addWeeks(NumberExpression weeksToAdd) {
		return new DateExpression(
				new DateNumberExpressionWithDateResult(this, weeksToAdd) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doAddWeeksTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * @return a DateExpression
	 */
	public DateExpression addMonths(int monthsToAdd) {
		return this.addMonths(new NumberExpression(monthsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd months to offset by
	 * @return a DateExpression
	 */
	public DateExpression addMonths(NumberExpression monthsToAdd) {
		return new DateExpression(
				new DateNumberExpressionWithDateResult(this, monthsToAdd) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doAddMonthsTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: add the supplied number of years to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * @return a DateExpression
	 */
	public DateExpression addYears(int yearsToAdd) {
		return this.addYears(new NumberExpression(yearsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of years to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd years to offset by
	 * @return a DateExpression
	 */
	public DateExpression addYears(NumberExpression yearsToAdd) {
		return new DateExpression(
				new DateNumberExpressionWithDateResult(this, yearsToAdd) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doAddYearsTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: get the days between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
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
	 * @return a NumberExpression
	 */
	public NumberExpression daysFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDateFunctionWithNumberResult(this, dateToCompareTo) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doDayDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: get the weeks between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
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
	 * @return a NumberExpression
	 */
	public NumberExpression weeksFrom(DateExpression dateToCompareTo) {
		return new NumberExpression(
				new DateDateFunctionWithNumberResult(this, dateToCompareTo) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doWeekDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: get the months between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
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
	 * @return a NumberExpression
	 */
	public NumberExpression monthsFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDateFunctionWithNumberResult(this, dateToCompareTo) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMonthDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: get the years between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
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
	 * @return a NumberExpression
	 */
	public NumberExpression yearsFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDateFunctionWithNumberResult(this, dateToCompareTo) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doYearDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: get the Hours between the date expression and the supplied
	 * date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
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
	 * @return a NumberExpression
	 */
	public NumberExpression hoursFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDateFunctionWithNumberResult(this, dateToCompareTo) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doHourDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: get the minutes between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
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
	 * @return a NumberExpression
	 */
	public NumberExpression minutesFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDateFunctionWithNumberResult(this, dateToCompareTo) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMinuteDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

	/**
	 * Date Arithmetic: get the seconds between the date expression and the
	 * supplied date.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param dateToCompareTo date to compare
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
	 * @return a NumberExpression
	 */
	public NumberExpression secondsFrom(DateResult dateToCompareTo) {
		return new NumberExpression(
				new DateDateFunctionWithNumberResult(this, dateToCompareTo) {

					@Override
					public boolean getIncludesNull() {
						return false;
					}

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doSecondDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
					}
				});
	}

//	public NumberExpression millisecondsFrom(Date date) {
//		return millisecondsFrom(value(date));
//	}
//
//	public NumberExpression millisecondsFrom(DateResult dateExpression) {
//		return new NumberExpression(
//				new DBBinaryDateFunctionWithNumberResult(this, dateExpression) {
//
//					@Override
//					public String toSQLString(DBDatabase db) {
//						return db.getDefinition().doMillisecondDifferenceTransform(first.toSQLString(db), second.toSQLString(db));
//					}
//				});
//	}
	/**
	 * Derive the first day of the month for this date expression
	 *
	 * @return a Date expression
	 */
	public DateExpression firstOfMonth() {
		return this.addDays(this.day().minus(1).bracket().times(-1));
	}

	/**
	 * Derive the last day of the month for this date expression
	 *
	 * @return a Date expression
	 */
	public DateExpression endOfMonth() {
		return new DateExpression(
				new DateExpressionWithDateResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						try {
							return db.getDefinition().doEndOfMonthTransform(this.getFirst().toSQLString(db));
						} catch (UnsupportedOperationException exp) {
							return getFirst().addDays(getFirst().day().minus(1).bracket().times(-1)).addMonths(1).addDays(-1).toSQLString(db);
						}
					}
				}
		);
//		return this.addDays(this.day().minus(1).bracket().times(-1)).addMonths(1).addDays(-1);
	}

	/**
	 * Return the index of the day of the week that this date expression refers
	 * to.
	 *
	 * Refer to {@link #SUNDAY},  {@link #MONDAY}, etc
	 *
	 * @return an index of the day of the week.
	 */
	public NumberExpression dayOfWeek() {
		return new NumberExpression(
				new DateExpressionWithNumberResult(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doDayOfWeekTransform(this.only.toSQLString(db));
					}
				});
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
	 * @return the least/smallest value from the list.
	 */
	public static DateExpression leastOf(DateResult... possibleValues) {
		DateExpression leastExpr
				= new DateExpression(new DateArrayFunctionWithDateResult(possibleValues) {

					@Override
					public String toSQLString(DBDatabase db) {
						List<String> strs = new ArrayList<String>();
						for (DateResult num : this.values) {
							strs.add(num.toSQLString(db));
						}
						return db.getDefinition().doLeastOfTransformation(strs);
					}

					@Override
					protected String getFunctionName(DBDatabase db) {
						return db.getDefinition().getLeastOfFunctionName();
					}
				});
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
	 * @return the largest value from the list.
	 */
	public static DateExpression greatestOf(DateResult... possibleValues) {
		DateExpression leastExpr
				= new DateExpression(new DateArrayFunctionWithDateResult(possibleValues) {

					@Override
					public String toSQLString(DBDatabase db) {
						List<String> strs = new ArrayList<String>();
						for (DateResult num : this.values) {
							strs.add(num.toSQLString(db));
						}
						return db.getDefinition().doGreatestOfTransformation(strs);
					}

					@Override
					protected String getFunctionName(DBDatabase db) {
						return db.getDefinition().getGreatestOfFunctionName();
					}
				});
		return leastExpr;
	}

	/**
	 * Roll the date value to the requested time zone.
	 *
	 * <p>
	 * If not implemented natively, a default implementation is provided that uses
	 * the raw offset to calculate the time zone change. This has several problem
	 * so a native implementation is recommended.
	 *
	 * @param timeZone the desired time zone
	 * @return a date expression that evaluates to the date value in the specified time zone.
	 */
	public DateExpression atTimeZone(TimeZone timeZone) {
		return new DateExpression(new DateTimeZoneExpressionWithDateResult(this, timeZone) {

			@Override
			public String toSQLString(DBDatabase db) {
				DBDefinition defn = db.getDefinition();
				try {
					return defn.doDateAtTimeZoneTransform(getFirst().toSQLString(db), getSecond());
				} catch (UnsupportedOperationException exp) {
					Double zoneOffset = (0.0 + this.getSecond().getRawOffset()) / 60.0;

					int hourPart = zoneOffset.intValue() * 100;
					int minutePart = (int) ((zoneOffset - (zoneOffset.intValue())) * 60);
					String hour = NumberExpression.value(hourPart).toSQLString(db);
					String minute = NumberExpression.value(minutePart).toSQLString(db);
					String date = getFirst().toSQLString(db);

					return defn.doAddMinutesTransform(defn.doAddHoursTransform(date, hour), minute);
				}
			}

		});
	}

	@Override
	public DBDate asExpressionColumn() {
		return new DBDate(this);
	}

	private static abstract class FunctionWithDateResult extends DateExpression {

		FunctionWithDateResult() {
		}

		abstract String getFunctionName(DBDatabase db);

		@Override
		public DBDate getQueryableDatatypeForExpressionValue() {
			return new DBDate();
		}

		protected String beforeValue(DBDatabase db) {
			return " " + getFunctionName(db) + "";
		}

		protected String afterValue(DBDatabase db) {
			return " ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return this.beforeValue(db) + this.afterValue(db);
		}

		@Override
		public DateExpression.FunctionWithDateResult copy() {
			DateExpression.FunctionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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

		protected DateExpression only;

		DateExpressionWithNumberResult() {
			this.only = null;
		}

		DateExpressionWithNumberResult(DateExpression only) {
			this.only = only;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public abstract String toSQLString(DBDatabase db);

		@Override
		public DateExpression.DateExpressionWithNumberResult copy() {
			DateExpression.DateExpressionWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = only.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (only != null) {
				hashSet.addAll(only.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (only == null) {
				return true;
			} else {
				return only.isPurelyFunctional();
			}
		}
	}

	private static abstract class DateExpressionWithDateResult extends DateExpression {

		private DateExpression only;

		DateExpressionWithDateResult() {
			this.only = null;
		}

		DateExpressionWithDateResult(DateExpression only) {
			this.only = only;
		}

		@Override
		public DBDate getQueryableDatatypeForExpressionValue() {
			return new DBDate();
		}

		@Override
		public abstract String toSQLString(DBDatabase db);

		@Override
		public DateExpressionWithDateResult copy() {
			DateExpressionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = getFirst().copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (getFirst() != null) {
				hashSet.addAll(getFirst().getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (getFirst() == null) {
				return true;
			} else {
				return getFirst().isPurelyFunctional();
			}
		}

		/**
		 * @return the only
		 */
		public DateExpression getFirst() {
			return only;
		}
	}

	private static abstract class DateDateExpressionWithBooleanResult extends BooleanExpression {

		private DateExpression first;
		private DateResult second;
		private boolean requiresNullProtection = false;

		DateDateExpressionWithBooleanResult(DateExpression first, DateResult second) {
			this.first = first;
			this.second = second;
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
		}

		@Override
		public DateDateExpressionWithBooleanResult copy() {
			DateDateExpressionWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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

		protected abstract String getEquationOperator(DBDatabase db);

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

		private DateExpression first;
		private DateResult second;
		private boolean requiresNullProtection = false;

		DateDateExpressionWithDateRepeatResult(DateExpression first, DateResult second) {
			this.first = first;
			this.second = second;
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DateDateExpressionWithDateRepeatResult copy() {
			DateDateExpressionWithDateRepeatResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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
		 * @return the first
		 */
		public DateExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		public DateResult getSecond() {
			return second;
		}
	}

	private static abstract class DateTimeZoneExpressionWithDateResult extends DateExpression {

		private DateExpression first;
		private TimeZone second;
		private boolean requiresNullProtection = false;

		DateTimeZoneExpressionWithDateResult(DateExpression first, TimeZone second) {
			this.first = first;
			this.second = second;
			if (second == null) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DateTimeZoneExpressionWithDateResult copy() {
			DateTimeZoneExpressionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst().copy();
			newInstance.second = getSecond();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (getFirst() != null) {
				hashSet.addAll(getFirst().getTablesInvolved());
			}
//			if (getSecond() != null) {
//				hashSet.addAll(getSecond().getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}

		/**
		 * @return the first
		 */
		public DateExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		public TimeZone getSecond() {
			return second;
		}
	}

	private static abstract class DateDateRepeatArithmeticDateResult extends DateExpression {

		private DateExpression first;
		private DateRepeatResult second;
		private boolean requiresNullProtection = false;

		DateDateRepeatArithmeticDateResult(DateExpression first, DateRepeatResult second) {
			this.first = first;
			this.second = second;
			if (second == null || second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return this.doExpressionTransformation(db);
		}

		@Override
		public DateDateRepeatArithmeticDateResult copy() {
			DateDateRepeatArithmeticDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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

		protected abstract String doExpressionTransformation(DBDatabase db);

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator() || getSecond().isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}

		/**
		 * @return the first
		 */
		public DateExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		public DateRepeatResult getSecond() {
			return second;
		}
	}

	private static abstract class DateArrayFunctionWithDateResult extends DateExpression {

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

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return "( ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
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
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			Collections.copy(this.values, newInstance.values);
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
			boolean result = column.isAggregator();
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
				boolean result = column.isPurelyFunctional();
				for (DateResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class DateDateResultFunctionWithBooleanResult extends BooleanExpression {

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

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return "( ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
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
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.getColumn().copy();
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
		 * @return the column
		 */
		protected DateExpression getColumn() {
			return column;
		}

		/**
		 * @return the values
		 */
		protected List<DateResult> getValues() {
			return values;
		}
	}

	private static abstract class DateDateFunctionWithDateResult extends DateExpression {

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

//		@Override
//		public DBNumber getQueryableDatatypeForExpressionValue() {
//			return new DBNumber();
//		}
		@Override
		public String toSQLString(DBDatabase db) {
			return this.beforeValue(db) + getFirst().toSQLString(db) + this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DateDateFunctionWithDateResult copy() {
			DateDateFunctionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return " " + getFunctionName(db) + "( ";
		}

		protected String getSeparator(DBDatabase db) {
			return ", ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator() || getSecond().isAggregator();
		}

		/**
		 * @return the first
		 */
		protected DateExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		protected DateResult getSecond() {
			return second;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class DateFunctionWithNumberResult extends NumberExpression {

		protected DateExpression only;

		DateFunctionWithNumberResult() {
			this.only = null;
		}

		DateFunctionWithNumberResult(DateExpression only) {
			this.only = only;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DateFunctionWithNumberResult copy() {
			DateFunctionWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = (only == null ? null : only.copy());
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return only.getTablesInvolved();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (only == null) {
				return true;
			} else {
				return only.isPurelyFunctional();
			}
		}
	}

	private static abstract class DateFunctionWithDateResult extends DateExpression {

		protected DateExpression only;

		DateFunctionWithDateResult() {
			this.only = null;
		}

		DateFunctionWithDateResult(DateExpression only) {
			this.only = only;
		}

//		@Override
//		public DBString getQueryableDatatypeForExpressionValue() {
//			return new DBString();
//		}
		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DateFunctionWithDateResult copy() {
			DateFunctionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = only.copy();
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return only.getTablesInvolved();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (only == null) {
				return true;
			} else {
				return only.isPurelyFunctional();
			}
		}
	}

	private static abstract class DateNumberExpressionWithDateResult extends DateExpression {

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
		abstract public String toSQLString(DBDatabase db); //{
//			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
//		}

		@Override
		public DateNumberExpressionWithDateResult copy() {
			DateNumberExpressionWithDateResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class DateDateFunctionWithNumberResult extends NumberExpression {

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
		abstract public String toSQLString(DBDatabase db);

		@Override
		public DateDateFunctionWithNumberResult copy() {
			DateDateFunctionWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}
}
