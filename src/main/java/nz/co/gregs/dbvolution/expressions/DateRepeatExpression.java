/*
 * Copyright 2015 Gregory Graham
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
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDateRepeat;
import static nz.co.gregs.dbvolution.expressions.AnyExpression.nullInteger;
import static nz.co.gregs.dbvolution.expressions.IntegerExpression.*;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.StringResult;
import org.joda.time.Period;

/**
 *
 * @author gregory.graham
 */
public class DateRepeatExpression extends RangeExpression<Period, DateRepeatResult, DBDateRepeat> implements DateRepeatResult {

	private final static long serialVersionUID = 1l;

	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String INTERVAL_PREFIX = "P";
	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String YEAR_SUFFIX = "Y";
	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String MONTH_SUFFIX = "M";
	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String DAY_SUFFIX = "D";
	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String HOUR_SUFFIX = "h";
	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String MINUTE_SUFFIX = "n";
	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String SECOND_SUFFIX = "s";

	/**
	 * Default constructor
	 *
	 */
	protected DateRepeatExpression() {
		super();
	}

	/**
	 * Creates a new DateRepeatExression that represents the value supplied.
	 *
	 * @param interval the value to use in the expression
	 */
	public DateRepeatExpression(Period interval) {
		super(new DBDateRepeat(interval));
	}

	/**
	 * Creates a new DateRepeatExression that represents the DateRepeat value
	 * supplied.
	 *
	 * @param interval the time period from which to create a DateRepeat value
	 */
	public DateRepeatExpression(DateRepeatResult interval) {
		super(interval);
	}

	/**
	 * Creates a new DateRepeatExression that represents the DateRepeat value
	 * supplied.
	 *
	 * @param interval the time period from which to create a DateRepeat value
	 */
	protected DateRepeatExpression(AnyResult<?> interval) {
		super(interval);
	}

	@Override
	public DateRepeatExpression copy() {
		return isNullSafetyTerminator()
				? nullDateRepeat()
				: new DateRepeatExpression((AnyResult<?>) getInnerResult().copy());
	}

	@Override
	public DBDateRepeat getQueryableDatatypeForExpressionValue() {
		return new DBDateRepeat();
	}

	/**
	 * Creates an expression that will return the most common value of the column
	 * supplied.
	 *
	 * <p>
	 * MODE: The number which appears most often in a set of numbers. For example:
	 * in {6, 3, 9, 6, 6, 5, 9, 3} the Mode is 6.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public DateRepeatExpression modeSimple() {
		@SuppressWarnings("unchecked")
		DateRepeatExpression modeExpr = new DateRepeatExpression(
				new ModeSimpleExpression<>(this));
		return modeExpr;
	}

	/**
	 * Creates an expression that will return the most common value of the column
	 * supplied.
	 *
	 * <p>
	 * MODE: The number which appears most often in a set of numbers. For example:
	 * in {6, 3, 9, 6, 6, 5, 9, 3} the Mode is 6.</p>
	 *
	 * <p>
	 * This version of Mode implements a stricter definition that will return null
	 * if the mode is undefined. The mode can be undefined if there are 2 or more
	 * values with the highest frequency value. </p>
	 *
	 * <p>
	 * For example in the list {0,0,0,0,1,1,2,2,2,2,3,4} both 0 and 2 occur four
	 * times and no other value occurs more frequently so the mode is undefined.
	 * {@link #modeSimple() The modeSimple()} method would return either 0 or 2
	 * randomly for the same set.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the mode or null if undefined.
	 */
	public DateRepeatExpression modeStrict() {
		@SuppressWarnings("unchecked")
		DateRepeatExpression modeExpr = new DateRepeatExpression(
				new ModeStrictExpression<>(this));
		return modeExpr;
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
	 * Returns TRUE if this expression evaluates to a smaller or more negative
	 * offset than the provided value, otherwise FALSE.
	 *
	 * @param period the time period that might be greater in duration than this
	 * expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(Period period) {
		return this.isLessThan(value(period));
	}

	/**
	 * Returns TRUE if this expression evaluates to a smaller or more negative
	 * offset than the provided value, otherwise FALSE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @param anotherInstance the value to compare with
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(DateRepeatResult anotherInstance) {
		return new IsLessThanExpression(this, anotherInstance);
	}

	/**
	 * Returns TRUE if this expression evaluates to a greater or less negative
	 * offset than the provided value, otherwise FALSE.
	 *
	 * @param period the time period that might be lesser in duration than this
	 * expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(Period period) {
		return this.isGreaterThan(value(period));
	}

	/**
	 * Returns TRUE if this expression evaluates to a smaller or more negative
	 * offset than the provided value, otherwise FALSE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @param anotherInstance the value to compare with
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new IsGreaterThanExpression(this, anotherInstance));
	}

	/**
	 * Returns TRUE if this expression evaluates to an equal or smaller offset
	 * than the provided value, otherwise FALSE.
	 *
	 * @param period the time period that might be greater in duration than this
	 * expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(Period period) {
		return this.isLessThanOrEqual(value(period));
	}

	/**
	 * Returns TRUE if this expression evaluates to an equal or smaller offset
	 * than the provided value, otherwise FALSE.
	 *
	 * @param anotherInstance the time period that might be greater in duration
	 * than this expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new IsLessThanOrEqualExpression(this, anotherInstance));
	}

	/**
	 * Returns TRUE if this expression evaluates to an equal or greater offset
	 * than the provided value, otherwise FALSE.
	 *
	 * @param period the time period that might be lesser in duration than this
	 * expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(Period period) {
		return this.isGreaterThanOrEqual(value(period));
	}

	/**
	 * Returns TRUE if this expression evaluates to an equal or greater offset
	 * than the provided value, otherwise FALSE.
	 *
	 * @param anotherInstance the time period that might be lesser in duration
	 * than this expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new IsGreaterThanOrEqualExpression(this, anotherInstance));
	}

	/**
	 * Returns TRUE if this expression evaluates to an smaller offset than the
	 * provided value, FALSE when it is greater than the provided value, and the
	 * value of the fallBackWhenEqual parameter when the 2 values are the same.
	 *
	 * @param period the time period that might be greater in duration than this
	 * expression
	 * @param fallBackWhenEqual the expression to be evaluated when the DateRepeat
	 * values are equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(Period period, BooleanExpression fallBackWhenEqual) {
		return this.isLessThan(value(period), fallBackWhenEqual);
	}

	/**
	 * Returns TRUE if this expression evaluates to an smaller offset than the
	 * provided value, FALSE when the it is greater than the provided value, and
	 * the value of the fallBackWhenEqual parameter when the 2 values are the
	 * same.
	 *
	 * @param anotherInstance the time period that might be greater in duration
	 * than this expression
	 * @param fallBackWhenEqual the expression to be evaluated when the values are
	 * equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(DateRepeatResult anotherInstance, BooleanExpression fallBackWhenEqual) {
		return this.isLessThan(anotherInstance).or(this.is(anotherInstance).and(fallBackWhenEqual));
	}

	/**
	 * Returns TRUE if this expression evaluates to an greater offset than the
	 * provided value, FALSE when the it is smaller than the provided value, and
	 * the value of the fallBackWhenEqual parameter when the 2 values are the
	 * same.
	 *
	 * @param period the time period that might be lesser in duration than this
	 * expression.
	 * @param fallBackWhenEqual the expression to be evaluated when the values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(Period period, BooleanExpression fallBackWhenEqual) {
		return this.isGreaterThan(value(period), fallBackWhenEqual);
	}

	/**
	 * Returns TRUE if this expression evaluates to an greater offset than the
	 * provided value, FALSE when the it is smaller than the provided value, and
	 * the value of the fallBackWhenEqual parameter when the 2 values are the
	 * same.
	 *
	 * @param anotherInstance the time period that might be lesser in duration
	 * than this expression.
	 * @param fallBackWhenEqual the expression to be evaluated when the values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(DateRepeatResult anotherInstance, BooleanExpression fallBackWhenEqual) {
		return this.isGreaterThan(anotherInstance).or(this.is(anotherInstance).and(fallBackWhenEqual));
	}

	/**
	 * Returns TRUE if this expression and the provided value are the same.
	 *
	 * @param period the required value of the DateRepeat
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(Period period) {
		return this.is(value(period));
	}

	/**
	 * Returns TRUE if this expression and the provided value are the same.
	 *
	 * @param anotherInstance the value that is to be found.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new IsExpression(this, anotherInstance));
	}

	/**
	 * Returns FALSE if this expression and the provided value are the same.
	 *
	 * @param anotherInstance a value that the expression should not match.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isNot(Period anotherInstance) {
		return isNot(value(anotherInstance));
	}

	/**
	 * Returns FALSE if this expression and the provided value are the same.
	 *
	 * @param anotherInstance a value that the expression should not match.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isNot(DateRepeatResult anotherInstance) {
		return BooleanExpression.allOf(
				is(anotherInstance).not(),
				isNotNull());
	}

	/**
	 * Returns the YEARS part of this DateRepeat value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public IntegerExpression getYears() {
		return new IntegerExpression(new GetYearsExpression(this)
		);
	}

	/**
	 * Returns the MONTHS part of this DateRepeat value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public IntegerExpression getMonths() {
		return new IntegerExpression(new GetMonthsExpression(this)
		);
	}

	/**
	 * Returns the DAYS part of this DateRepeat value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public IntegerExpression getDays() {
		return new IntegerExpression(new GetDaysExpression(this)
		);
	}

	/**
	 * Returns the HOURS part of this DateRepeat value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public IntegerExpression getHours() {
		return new IntegerExpression(new GetHoursExpression(this)
		);
	}

	/**
	 * Returns the MINUTES part of this DateRepeat value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public IntegerExpression getMinutes() {
		return new IntegerExpression(new GetMinutesExpression(this)
		);
	}

	/**
	 * Returns the SECONDS part of this DateRepeat value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public NumberExpression getSeconds() {
		return new NumberExpression(new GetSecondsExpression(this)
		);
	}

	/**
	 * Converts the interval expression into a string/character expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression of the interval expression.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new StringResultExpression(this));
	}

	@Override
	public DBDateRepeat asExpressionColumn() {
		return new DBDateRepeat(this);
	}

	@Override
	public BooleanExpression isBetween(DateRepeatResult anotherInstance, DateRepeatResult largerExpression) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(anotherInstance),
				this.isLessThan(anotherInstance)
		);
	}

	@Override
	public BooleanExpression isBetweenInclusive(DateRepeatResult anotherInstance, DateRepeatResult largerExpression) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(anotherInstance),
				this.isLessThanOrEqual(anotherInstance)
		);
	}

	@Override
	public BooleanExpression isBetweenExclusive(DateRepeatResult anotherInstance, DateRepeatResult largerExpression) {
		return BooleanExpression.allOf(
				this.isGreaterThan(anotherInstance),
				this.isLessThan(anotherInstance)
		);
	}

	@Override
	public BooleanExpression isIn(DateRepeatResult... otherInstances) {
		StringResult[] strs = new StringResult[otherInstances.length];
		int i = 0;
		for (DateRepeatResult otherInstance : otherInstances) {
			strs[i] = otherInstance.stringResult();
			i++;
		}
		return this.stringResult().isIn(strs);
	}

	@Override
	public BooleanExpression isNotIn(DateRepeatResult... otherInstances) {
		StringResult[] strs = new StringResult[otherInstances.length];
		int i = 0;
		for (DateRepeatResult otherInstance : otherInstances) {
			strs[i] = otherInstance.stringResult();
			i++;
		}
		return this.stringResult().isNotIn(strs);
	}

	@Override
	public DateRepeatExpression nullExpression() {
		return new NullExpression();
	}

	@Override
	public DateRepeatResult expression(Period value) {
		return new DateRepeatExpression(value);
	}

	@Override
	public DateRepeatResult expression(DateRepeatResult value) {
		return new DateRepeatExpression(value);
	}

	@Override
	public DateRepeatResult expression(DBDateRepeat value) {
		return value;
	}

	public NumberExpression approximateDurationInSeconds() {
		return this.getYears().times(12).plus(this.getMonths()).times(30).plus(this.getDays()).times(24).plus(this.getHours()).times(60).plus(this.getMinutes()).times(60).plus(this.getSeconds());
	}

	public Comparison comparison(DateRepeatExpression right) {
		return new Comparison(this, right);
	}

	private static abstract class DateRepeatDateRepeatWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final DateRepeatExpression first;
		private final DateRepeatExpression second;
		private boolean requiresNullProtection;

		DateRepeatDateRepeatWithBooleanResult(DateRepeatExpression first, DateRepeatResult second) {
			this.first = first;
			this.second = new DateRepeatExpression(second);
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		DateRepeatExpression getFirst() {
			return first;
		}

		DateRepeatExpression getSecond() {
			return second;
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	public static class Comparison extends IntegerExpression {

		private static final long serialVersionUID = 1L;

		private final DateRepeatExpression first;
		private final DateRepeatExpression second;
		private boolean requiresNullProtection;

		Comparison(DateRepeatExpression first, DateRepeatResult second) {
			this.first = first;
			this.second = new DateRepeatExpression(second);
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		DateRepeatExpression getFirst() {
			return first;
		}

		DateRepeatExpression getSecond() {
			return second;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
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

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}

		protected String doExpressionTransform(DBDefinition db) {
			final DateRepeatExpression firstExpr = getFirst();
			final DateRepeatExpression secondExpr = getSecond();
			final IntegerExpression minusOne = IntegerExpression.value(-1);
			final IntegerExpression zero = IntegerExpression.value(0);
			final IntegerExpression one = IntegerExpression.value(1);
			return firstExpr.isLessThan(secondExpr)
					.ifTrueFalseNull(
							minusOne,
							firstExpr.is(secondExpr)
									.ifTrueFalseNull(
											zero,
											one,
											nullInteger()
									),
							nullInteger()
					).toSQLString(db);
		}

		public BooleanExpression ifLesserEqualGreaterOrNull(BooleanExpression lesserResult, BooleanExpression equalResult, BooleanExpression greaterResult, BooleanExpression nullResult) {
			return CaseExpression
					.when(this.is(-1), lesserResult)
					.when(this.is(0), equalResult)
					.when(this.is(1), greaterResult)
					.defaultValue(nullResult);
		}

	}

	private static abstract class DateRepeatWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		private final DateRepeatExpression first;
		private boolean requiresNullProtection;

		DateRepeatWithNumberResult(DateRepeatExpression first) {
			this.first = first;
		}

		protected abstract String doExpressionTransform(DBDefinition db);

		DateRepeatExpression getFirst() {
			return first;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class DateRepeatWithIntegerResult extends IntegerExpression {

		private static final long serialVersionUID = 1L;

		private final DateRepeatExpression first;
		private boolean requiresNullProtection;

		DateRepeatWithIntegerResult(DateRepeatExpression first) {
			this.first = first;
		}

		protected abstract String doExpressionTransform(DBDefinition db);

		DateRepeatExpression getFirst() {
			return first;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class DateRepeatWithStringResult extends StringExpression {

		private static final long serialVersionUID = 1L;

		private final DateRepeatExpression first;
		private boolean requiresNullProtection;

		DateRepeatWithStringResult(DateRepeatExpression first) {
			this.first = first;
		}

		protected abstract String doExpressionTransform(DBDefinition db);

		DateRepeatExpression getFirst() {
			return first;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static class IsLessThanExpression extends DateRepeatDateRepeatWithBooleanResult {

		public IsLessThanExpression(DateRepeatExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			final DateRepeatExpression firstExpr = getFirst();
			final DateRepeatExpression secondExpr = getSecond();
			final String firstSQL = firstExpr.toSQLString(db);
			final String secondSQL = secondExpr.toSQLString(db);
			try {
				return db.doDateRepeatLessThanTransform(firstSQL, secondSQL);
			} catch (UnsupportedOperationException exp) {
				return CaseExpression
						.when(firstExpr.isNull().or(secondExpr.isNull()), nullInteger())
						.when(firstExpr.stringResult().isEmpty().or(secondExpr.stringResult().isEmpty()), nullInteger())
						.when(firstExpr.getYears().isLessThan(secondExpr.getYears()), ONE)
						.when(firstExpr.getYears().isGreaterThan(secondExpr.getYears()), ZERO)
						.when(firstExpr.getMonths().isLessThan(secondExpr.getMonths()), ONE)
						.when(firstExpr.getMonths().isGreaterThan(secondExpr.getMonths()), ZERO)
						.when(firstExpr.getDays().isLessThan(secondExpr.getDays()), ONE)
						.when(firstExpr.getDays().isGreaterThan(secondExpr.getDays()), ZERO)
						.when(firstExpr.getHours().isLessThan(secondExpr.getHours()), ONE)
						.when(firstExpr.getHours().isGreaterThan(secondExpr.getHours()), ZERO)
						.when(firstExpr.getMinutes().isLessThan(secondExpr.getMinutes()), ONE)
						.when(firstExpr.getMinutes().isGreaterThan(secondExpr.getMinutes()), ZERO)
						.when(firstExpr.getSeconds().isLessThan(secondExpr.getSeconds()), ONE)
						.when(firstExpr.getSeconds().isGreaterThan(secondExpr.getSeconds()), ZERO)
						.defaultValue(ZERO)
						.is(ONE)
						.toSQLString(db);
			}
		}

		@Override
		public IsLessThanExpression copy() {
			return new IsLessThanExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class IsGreaterThanExpression extends DateRepeatDateRepeatWithBooleanResult {

		public IsGreaterThanExpression(DateRepeatExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doDateRepeatGreaterThanTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				final DateRepeatExpression firstExpr = this.getFirst();
				final DateRepeatExpression secondExpr = this.getSecond();
				return CaseExpression
						.when(firstExpr.isNull().or(secondExpr.isNull()), nullInteger())
						.when(firstExpr.stringResult().isEmpty().or(secondExpr.stringResult().isEmpty()), nullInteger())
						.when(firstExpr.getYears().isLessThan(secondExpr.getYears()), ZERO)
						.when(firstExpr.getYears().isGreaterThan(secondExpr.getYears()), ONE)
						.when(firstExpr.getMonths().isLessThan(secondExpr.getMonths()), ZERO)
						.when(firstExpr.getMonths().isGreaterThan(secondExpr.getMonths()), ONE)
						.when(firstExpr.getDays().isLessThan(secondExpr.getDays()), ZERO)
						.when(firstExpr.getDays().isGreaterThan(secondExpr.getDays()), ONE)
						.when(firstExpr.getHours().isLessThan(secondExpr.getHours()), ZERO)
						.when(firstExpr.getHours().isGreaterThan(secondExpr.getHours()), ONE)
						.when(firstExpr.getMinutes().isLessThan(secondExpr.getMinutes()), ZERO)
						.when(firstExpr.getMinutes().isGreaterThan(secondExpr.getMinutes()), ONE)
						.when(firstExpr.getSeconds().isLessThan(secondExpr.getSeconds()), ZERO)
						.when(firstExpr.getSeconds().isGreaterThan(secondExpr.getSeconds()), ONE)
						.defaultValue(ZERO)
						.is(ONE)
						.toSQLString(db);

			}
		}

		@Override
		public IsGreaterThanExpression copy() {
			return new IsGreaterThanExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class IsLessThanOrEqualExpression extends DateRepeatDateRepeatWithBooleanResult {

		public IsLessThanOrEqualExpression(DateRepeatExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doDateRepeatLessThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				final DateRepeatExpression firstExpr = this.getFirst();
				final DateRepeatExpression secondExpr = this.getSecond();
				return firstExpr.isGreaterThan(secondExpr).not().toSQLString(db);
			}
		}

		@Override
		public IsLessThanOrEqualExpression copy() {
			return new IsLessThanOrEqualExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class IsGreaterThanOrEqualExpression extends DateRepeatDateRepeatWithBooleanResult {

		public IsGreaterThanOrEqualExpression(DateRepeatExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doDateRepeatGreaterThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				final DateRepeatExpression firstExpr = this.getFirst();
				final DateRepeatExpression secondExpr = this.getSecond();
				return firstExpr.isLessThan(secondExpr).not().toSQLString(db);
			}
		}

		@Override
		public IsGreaterThanOrEqualExpression copy() {
			return new IsGreaterThanOrEqualExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class IsExpression extends DateRepeatDateRepeatWithBooleanResult {

		public IsExpression(DateRepeatExpression first, DateRepeatResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doDateRepeatEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				final DateRepeatExpression firstExpr = this.getFirst();
				final DateRepeatExpression secondExpr = this.getSecond();
				return CaseExpression
						.when(firstExpr.isNull().or(secondExpr.isNull()), nullInteger())
						.when(firstExpr.stringResult().isEmpty().or(secondExpr.stringResult().isEmpty()), nullInteger())
						.when(firstExpr.getYears().is(secondExpr.getYears())
								.and(firstExpr.getMonths().is(secondExpr.getMonths()))
								.and(firstExpr.getDays().is(secondExpr.getDays()))
								.and(firstExpr.getHours().is(secondExpr.getHours()))
								.and(firstExpr.getMinutes().is(secondExpr.getMinutes()))
								.and(firstExpr.getSeconds().is(secondExpr.getSeconds()))
								, ONE)
						.defaultValue(ZERO)
						.is(ONE)
						.toSQLString(db);
			}
		}

		@Override
		public DateRepeatExpression.IsExpression copy() {
			return new DateRepeatExpression.IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class GetYearsExpression extends DateRepeatWithIntegerResult {

		public GetYearsExpression(DateRepeatExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateRepeatGetYearsTransform(getFirst().toSQLString(db));
			} else {
				return BooleanExpression.isNull(getFirst()).ifThenElse(
						nullNumber(),
						getFirst().stringResult().substringBefore(YEAR_SUFFIX).substringAfter(INTERVAL_PREFIX).numberResult()
				).toSQLString(db);
			}
		}

		@Override
		public GetYearsExpression copy() {
			return new GetYearsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class GetMonthsExpression extends DateRepeatWithIntegerResult {

		public GetMonthsExpression(DateRepeatExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateRepeatGetMonthsTransform(getFirst().toSQLString(db));
			} else {
				return BooleanExpression.isNull(getFirst()).ifThenElse(
						nullNumber(),
						getFirst().stringResult().substringBefore(MONTH_SUFFIX).substringAfter(YEAR_SUFFIX).numberResult()
				).toSQLString(db);
			}
		}

		@Override
		public GetMonthsExpression copy() {
			return new GetMonthsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class GetDaysExpression extends DateRepeatWithIntegerResult {

		public GetDaysExpression(DateRepeatExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateRepeatGetDaysTransform(getFirst().toSQLString(db));
			} else {
				return BooleanExpression.isNull(getFirst()).ifThenElse(
						nullNumber(),
						getFirst().stringResult().substringBefore(DAY_SUFFIX).substringAfter(MONTH_SUFFIX).numberResult()
				).toSQLString(db);
			}
		}

		@Override
		public GetDaysExpression copy() {
			return new GetDaysExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class GetHoursExpression extends DateRepeatWithIntegerResult {

		public GetHoursExpression(DateRepeatExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateRepeatGetHoursTransform(getFirst().toSQLString(db));
			} else {
				return BooleanExpression.isNull(getFirst()).ifThenElse(
						nullNumber(),
						getFirst().stringResult().substringBefore(HOUR_SUFFIX).substringAfter(DAY_SUFFIX).numberResult()
				).toSQLString(db);
			}
		}

		@Override
		public GetHoursExpression copy() {
			return new GetHoursExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class GetMinutesExpression extends DateRepeatWithIntegerResult {

		public GetMinutesExpression(DateRepeatExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateRepeatGetMinutesTransform(getFirst().toSQLString(db));
			} else {
				return BooleanExpression.isNull(getFirst()).ifThenElse(
						nullNumber(),
						getFirst().stringResult().substringBefore(MINUTE_SUFFIX).substringAfter(HOUR_SUFFIX).numberResult()
				).toSQLString(db);
			}
		}

		@Override
		public GetMinutesExpression copy() {
			return new GetMinutesExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class GetSecondsExpression extends DateRepeatWithNumberResult {

		public GetSecondsExpression(DateRepeatExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			if (db.supportsDateRepeatDatatypeFunctions()) {
				return db.doDateRepeatGetSecondsTransform(getFirst().toSQLString(db));
			} else {
				return BooleanExpression.isNull(getFirst()).ifThenElse(
						nullNumber(),
						getFirst().stringResult().substringBefore(SECOND_SUFFIX).substringAfter(MINUTE_SUFFIX).numberResult()
				).toSQLString(db);
			}
		}

		@Override
		public GetSecondsExpression copy() {
			return new GetSecondsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class StringResultExpression extends DateRepeatWithStringResult {

		public StringResultExpression(DateRepeatExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.doDateRepeatToStringTransform(getFirst().toSQLString(db));
		}

		@Override
		public StringResultExpression copy() {
			return new StringResultExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class NullExpression extends DateRepeatExpression {

		public NullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public NullExpression copy() {
			return new NullExpression();
		}
	}

	public static WindowFunctionFramable<DateRepeatExpression> firstValue() {
		return new FirstValueExpression().over();
	}

	public static class FirstValueExpression extends DateRepeatExpression implements CanBeWindowingFunctionWithFrame<DateRepeatExpression> {

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
		public WindowFunctionFramable<DateRepeatExpression> over() {
			return new WindowFunctionFramable<DateRepeatExpression>(new DateRepeatExpression(this));
		}

	}

	public static WindowFunctionFramable<DateRepeatExpression> lastValue() {
		return new LastValueExpression().over();
	}

	public static class LastValueExpression extends DateRepeatExpression implements CanBeWindowingFunctionWithFrame<DateRepeatExpression> {

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
		public LastValueExpression copy() {
			return new LastValueExpression();
		}

		@Override
		public WindowFunctionFramable<DateRepeatExpression> over() {
			return new WindowFunctionFramable<DateRepeatExpression>(new DateRepeatExpression(this));
		}

	}

	public static WindowFunctionFramable<DateRepeatExpression> nthValue(IntegerExpression indexExpression) {
		return new NthValueExpression(indexExpression).over();
	}

	public static class NthValueExpression extends DateRepeatExpression implements CanBeWindowingFunctionWithFrame<DateRepeatExpression> {

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
		public WindowFunctionFramable<DateRepeatExpression> over() {
			return new WindowFunctionFramable<DateRepeatExpression>(new DateRepeatExpression(this));
		}
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
	public WindowFunctionFramable<DateRepeatExpression> lag() {
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
	public WindowFunctionFramable<DateRepeatExpression> lag(IntegerExpression offset) {
		return lag(offset, NumberExpression.nullDateRepeat());
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
	public WindowFunctionFramable<DateRepeatExpression> lag(IntegerExpression offset, DateRepeatExpression defaultExpression) {
		return new LagExpression(this, offset, defaultExpression).over();
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
	public WindowFunctionFramable<DateRepeatExpression> lead() {
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
	public WindowFunctionFramable<DateRepeatExpression> lead(IntegerExpression offset) {
		return lead(offset, nullDateRepeat());
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
	public WindowFunctionFramable<DateRepeatExpression> lead(IntegerExpression offset, DateRepeatExpression defaultExpression) {
		return new LeadExpression(this, offset, defaultExpression).over();
	}

	private static abstract class LagLeadFunction extends DateRepeatExpression implements CanBeWindowingFunctionWithFrame<DateRepeatExpression> {

		private static final long serialVersionUID = 1L;

		protected DateRepeatExpression first;
		protected IntegerExpression second;
		protected DateRepeatExpression third;

		LagLeadFunction(DateRepeatExpression first, IntegerExpression second, DateRepeatExpression third) {
			this.first = first;
			this.second = second == null ? value(1) : second;
			this.third = third == null ? nullDateRepeat() : third;
		}

		@Override
		public DBDateRepeat getQueryableDatatypeForExpressionValue() {
			return new DBDateRepeat();
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
		protected DateRepeatExpression getFirst() {
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
		protected DateRepeatExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
		}

		@Override
		public WindowFunctionFramable<DateRepeatExpression> over() {
			return new WindowFunctionFramable<>(new DateRepeatExpression(this));
		}
	}

	public class LagExpression extends LagLeadFunction {

		private static final long serialVersionUID = 1L;

		public LagExpression(DateRepeatExpression first, IntegerExpression second, DateRepeatExpression third) {
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

	public class LeadExpression extends LagLeadFunction {

		private static final long serialVersionUID = 1L;

		public LeadExpression(DateRepeatExpression first, IntegerExpression second, DateRepeatExpression third) {
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
