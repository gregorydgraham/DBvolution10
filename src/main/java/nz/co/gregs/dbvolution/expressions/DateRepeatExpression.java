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
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDateRepeat;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.StringResult;
import org.joda.time.Period;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param interval
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
	 * @param anotherInstance
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new IsLessThanExpression(this, anotherInstance));
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
	 * @param anotherInstance
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
			return db.doDateRepeatLessThanTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
			return db.doDateRepeatGreaterThanTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
			return db.doDateRepeatLessThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
			return db.doDateRepeatGreaterThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
				final DateRepeatExpression left = this.getFirst();
				final DateRepeatExpression right = this.getSecond();
				return BooleanExpression.allOf(
						left.getYears().is(right.getYears()),
						left.getMonths().is(right.getMonths()),
						left.getDays().is(right.getDays()),
						left.getHours().is(right.getHours()),
						left.getMinutes().is(right.getMinutes()),
						left.getSeconds().is(right.getSeconds())
				).toSQLString(db);
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
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
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
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
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
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
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
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
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
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
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
			if (db instanceof SupportsDateRepeatDatatypeFunctions) {
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
}
