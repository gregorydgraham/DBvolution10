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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBDateOnly;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

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
public class DateExpression implements DateResult {

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
	 * @param dateVariable
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
	 * @param date
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
	 * @param date
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static DateExpression value(Date date) {
		return new DateExpression(date);
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
				new DBNonaryFunction() {

					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doCurrentDateOnlyTransform();
					}

					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getCurrentDateOnlyFunctionName();
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
				new DBNonaryFunction() {
					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getCurrentDateTimeFunction();
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
				new DBNonaryFunction() {
					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getCurrentTimeFunction();
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
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doYearTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that returns the month part of this date
	 * expression.
	 *
	 * @return the month of this date expression as a number.
	 */
	public NumberExpression month() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMonthTransform(this.only.toSQLString(db));
					}
				});
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
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doDayTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that returns the hour part of this date
	 * expression.
	 *
	 * @return the hour of this date expression as a number.
	 */
	public NumberExpression hour() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doHourTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that returns the minute part of this date
	 * expression.
	 *
	 * @return the minute of this date expression as a number.
	 */
	public NumberExpression minute() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMinuteTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that returns the second part of this date
	 * expression.
	 *
	 * @return the second of this date expression as a number.
	 */
	public NumberExpression second() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doSecondTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * <p>
	 * Be careful when using this expression as dates have lots of fields and it
	 * is easy to miss a similar date.
	 *
	 * @param date
	 * @return a BooleanExpression comparing the date and this DateExpression.
	 */
	public BooleanExpression is(Date date) {
		return is(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is equal
	 * to the supplied date.
	 *
	 * @param dateExpression
	 * @return a BooleanExpression comparing the DateResult and this
	 * DateExpression.
	 */
	public BooleanExpression is(DateResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param date
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLessThan(Date date) {
		return isLessThan(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than to the supplied date.
	 *
	 * @param dateExpression
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLessThan(DateResult dateExpression) {
		return new BooleanExpression(new DateExpression.DBBinaryBooleanArithmetic(this, dateExpression) {
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
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied date.
	 *
	 * @param date
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLessThanOrEqual(Date date) {
		return isLessThanOrEqual(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is less
	 * than or equal to the supplied DateResult.
	 *
	 * @param dateExpression
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isLessThanOrEqual(DateResult dateExpression) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
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
	 * @param date
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isGreaterThan(Date date) {
		return isGreaterThan(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than the supplied DateResult.
	 *
	 * @param dateExpression
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isGreaterThan(DateResult dateExpression) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
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
	 * @param date
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isGreaterThanOrEqual(Date date) {
		return isGreaterThanOrEqual(value(date));
	}

	/**
	 * Creates an SQL expression that test whether this date expression is greater
	 * than or equal to the supplied DateResult.
	 *
	 * @param dateExpression
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isGreaterThanOrEqual(DateResult dateExpression) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
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
	 * Creates an SQL expression that test whether this date expression is
	 * included in the list of Dates.
	 *
	 * <p>
	 * Be careful when using this expression as dates have lots of fields and it
	 * is easy to miss a similar date.
	 *
	 * @param possibleValues
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
	 * @param possibleValues
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
	 * @param possibleValues
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isIn(DateResult... possibleValues) {
		BooleanExpression isInExpr = new BooleanExpression(new DBNnaryBooleanFunction(this, possibleValues) {
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
	 * @param alternative
	 * @return a boolean expression representing the required comparison
	 */
	public DateExpression ifDBNull(Date alternative) {
		return new DateExpression(
				new DateExpression.DBBinaryFunction(this, new DateExpression(alternative)) {
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
	 * @param alternative
	 * @return a boolean expression representing the required comparison
	 */
	public DateExpression ifDBNull(DateResult alternative) {
		return new DateExpression(
				new DateExpression.DBBinaryFunction(this, alternative) {
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
		return new NumberExpression(new DBUnaryNumberFunction(this) {
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
		return new DateExpression(new DBUnaryDateFunction(this) {
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
		return new DateExpression(new DBUnaryDateFunction(this) {
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
	 * @param secondsToAdd
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
	 * @param secondsToAdd
	 * @return a DateExpression
	 */
	public DateExpression addSeconds(NumberExpression secondsToAdd) {
		return new DateExpression(
				new DBBinaryDateNumberFunctionWithDateResult(this, secondsToAdd) {

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

	/**
	 * Date Arithmetic: add the supplied number of minutes to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param minutesToAdd
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
	 * @param minutesToAdd
	 * @return a DateExpression
	 */
	public DateExpression addMinutes(NumberExpression minutesToAdd) {
		return new DateExpression(
				new DBBinaryDateNumberFunctionWithDateResult(this, minutesToAdd) {

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
	 * @param daysToAdd
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
	 * @param daysToAdd
	 * @return a DateExpression
	 */
	public DateExpression addDays(NumberExpression daysToAdd) {
		return new DateExpression(
				new DBBinaryDateNumberFunctionWithDateResult(this, daysToAdd) {

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
	 * @param hoursToAdd
	 * @return a DateExpression
	 */
	DateExpression addHours(int hoursToAdd) {
		return this.addHours(new NumberExpression(hoursToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of hours to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param hoursToAdd
	 * @return a DateExpression
	 */
	public DateExpression addHours(NumberExpression hoursToAdd) {
		return new DateExpression(
				new DBBinaryDateNumberFunctionWithDateResult(this, hoursToAdd) {

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
	 * @param weeksToAdd
	 * @return a DateExpression
	 */
	DateExpression addWeeks(int weeksToAdd) {
		return this.addWeeks(new NumberExpression(weeksToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of weeks to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param weeksToAdd
	 * @return a DateExpression
	 */
	public DateExpression addWeeks(NumberExpression weeksToAdd) {
		return new DateExpression(
				new DBBinaryDateNumberFunctionWithDateResult(this, weeksToAdd) {

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
	 * @param monthsToAdd
	 * @return a DateExpression
	 */
	DateExpression addMonths(int monthsToAdd) {
		return this.addMonths(new NumberExpression(monthsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of months to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param monthsToAdd
	 * @return a DateExpression
	 */
	public DateExpression addMonths(NumberExpression monthsToAdd) {
		return new DateExpression(
				new DBBinaryDateNumberFunctionWithDateResult(this, monthsToAdd) {

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
	 * @param yearsToAdd
	 * @return a DateExpression
	 */
	DateExpression addYears(int yearsToAdd) {
		return this.addYears(new NumberExpression(yearsToAdd));
	}

	/**
	 * Date Arithmetic: add the supplied number of years to the date expression.
	 *
	 * <p>
	 * Negative values are supported.
	 *
	 * @param yearsToAdd
	 * @return a DateExpression
	 */
	public DateExpression addYears(NumberExpression yearsToAdd) {
		return new DateExpression(
				new DBBinaryDateNumberFunctionWithDateResult(this, yearsToAdd) {

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

	private static abstract class DBNonaryFunction implements DateResult {

		DBNonaryFunction() {
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
		public DateExpression.DBNonaryFunction copy() {
			DateExpression.DBNonaryFunction newInstance;
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
	}

	private static abstract class UnaryComplicatedNumberFunction implements NumberResult {

		protected DateExpression only;

		UnaryComplicatedNumberFunction() {
			this.only = null;
		}

		UnaryComplicatedNumberFunction(DateExpression only) {
			this.only = only;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public abstract String toSQLString(DBDatabase db);

		@Override
		public DateExpression.UnaryComplicatedNumberFunction copy() {
			DateExpression.UnaryComplicatedNumberFunction newInstance;
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
	}

	private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

		private DateExpression first;
		private DateResult second;
		private boolean requiresNullProtection = false;

		DBBinaryBooleanArithmetic(DateExpression first, DateResult second) {
			this.first = first;
			this.second = second;
			if (second.getIncludesNull()) {
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
		public DBBinaryBooleanArithmetic copy() {
			DBBinaryBooleanArithmetic newInstance;
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

	private static abstract class DBNnaryDateFunction implements DateResult {

		protected DateExpression column;
		protected DateResult[] values;

		DBNnaryDateFunction() {
			this.values = null;
		}

		DBNnaryDateFunction(DateExpression leftHandSide, DateResult[] rightHandSide) {
			this.values = new DateResult[rightHandSide.length];
			this.column = leftHandSide;
			System.arraycopy(rightHandSide, 0, this.values, 0, rightHandSide.length);
		}

		@Override
		public DBDate getQueryableDatatypeForExpressionValue() {
			return new DBDate();
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
					.append(column.toSQLString(db)).append(" ")
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
		public DBNnaryDateFunction copy() {
			DBNnaryDateFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			newInstance.values = this.values;
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false || column.isAggregator();
			for (DateResult dater : values) {
				result = result || dater.isAggregator();
			}
			return result;
		}
	}

	private static abstract class DBNnaryBooleanFunction implements BooleanResult {

		protected DateExpression column;
		protected List<DateResult> values = new ArrayList<DateResult>();
		boolean nullProtectionRequired = false;

		DBNnaryBooleanFunction() {
		}

		DBNnaryBooleanFunction(DateExpression leftHandSide, DateResult[] rightHandSide) {
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
					.append(column.toSQLString(db))
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
		public DBNnaryBooleanFunction copy() {
			DBNnaryBooleanFunction newInstance;
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
			for (DateResult val : values) {
				if (val != null) {
					hashSet.addAll(val.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false || column.isAggregator();
			for (DateResult dater : values) {
				result = result || dater.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}
	}

	private static abstract class DBBinaryFunction implements DateResult {

		private DateExpression first;
		private DateResult second;

		DBBinaryFunction(DateExpression first) {
			this.first = first;
			this.second = null;
		}

		DBBinaryFunction(DateExpression first, DateResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return this.beforeValue(db) + first.toSQLString(db) + this.getSeparator(db) + (second == null ? "" : second.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DBBinaryFunction copy() {
			DBBinaryFunction newInstance;
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
			return first.isAggregator() || second.isAggregator();
		}
	}

	private static abstract class DBUnaryNumberFunction implements NumberResult {

		protected DateExpression only;

		DBUnaryNumberFunction() {
			this.only = null;
		}

		DBUnaryNumberFunction(DateExpression only) {
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
		public DBUnaryNumberFunction copy() {
			DBUnaryNumberFunction newInstance;
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
	}

	private static abstract class DBUnaryDateFunction implements DateResult {

		protected DateExpression only;

		DBUnaryDateFunction() {
			this.only = null;
		}

		DBUnaryDateFunction(DateExpression only) {
			this.only = only;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
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
		public DBUnaryDateFunction copy() {
			DBUnaryDateFunction newInstance;
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
	}

	private static abstract class DBBinaryDateNumberFunctionWithDateResult implements DateResult {

		protected DateExpression first;
		protected NumberExpression second;

		DBBinaryDateNumberFunctionWithDateResult() {
			this.first = null;
			this.second = null;
		}

		DBBinaryDateNumberFunctionWithDateResult(DateExpression dateExp, NumberExpression numbExp) {
			this.first = dateExp;
			this.second = numbExp;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

//		abstract String getFunctionName(DBDatabase db);
//
//		protected String beforeValue(DBDatabase db) {
//			return "" + getFunctionName(db) + "( ";
//		}
//
//		protected String afterValue(DBDatabase db) {
//			return ") ";
//		}
		@Override
		abstract public String toSQLString(DBDatabase db); //{
//			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
//		}

		@Override
		public DBBinaryDateNumberFunctionWithDateResult copy() {
			DBBinaryDateNumberFunctionWithDateResult newInstance;
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
	}
}
