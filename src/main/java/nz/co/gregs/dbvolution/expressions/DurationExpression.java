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

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDuration;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.DurationResult;
import nz.co.gregs.dbvolution.results.StringResult;

/**
 *
 * @author gregory.graham
 */
public class DurationExpression extends RangeExpression<Duration, DurationResult, DBDuration> implements DurationResult {

	private final static long serialVersionUID = 1l;

	/**
	 * DateRepeat values are often stored as Strings of the format
	 * P2015Y12M30D23h59n59.0s
	 */
	public static final String INTERVAL_PREFIX = "P";
	/**
	 * DateRepeat values are often stored as Strings of the format P20D23h59n59.0s
	 */
	public static final String DAY_SUFFIX = "DT";
	/**
	 * DateRepeat values are often stored as Strings of the format P20D23h59n59.0s
	 */
	public static final String HOUR_SUFFIX = "H";
	/**
	 * DateRepeat values are often stored as Strings of the format P20D23h59n59.0s
	 */
	public static final String MINUTE_SUFFIX = "M";
	/**
	 * DateRepeat values are often stored as Strings of the format P20D23h59n59.0s
	 */
	public static final String SECOND_SUFFIX = "S";

	/**
	 * Default constructor
	 *
	 */
	protected DurationExpression() {
		super();
	}

	/**
	 * Creates a new DateRepeatExression that represents the value supplied.
	 *
	 * @param interval the value to use in the expression
	 */
	public DurationExpression(Duration interval) {
		super(new DBDuration(interval));
	}

	/**
	 * Creates a new DateRepeatExression that represents the DateRepeat value
	 * supplied.
	 *
	 * @param interval the time period from which to create a DateRepeat value
	 */
	public DurationExpression(DurationResult interval) {
		super(interval);
	}

	/**
	 * Creates a new DateRepeatExression that represents the DateRepeat value
	 * supplied.
	 *
	 * @param interval the time period from which to create a DateRepeat value
	 */
	protected DurationExpression(AnyResult<?> interval) {
		super(interval);
	}

	@Override
	public DurationExpression copy() {
		return isNullSafetyTerminator()
				? nullDuration()
				: new DurationExpression((AnyResult<?>) getInnerResult().copy());
	}

	@Override
	public DBDuration getQueryableDatatypeForExpressionValue() {
		return new DBDuration();
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
	public DurationExpression modeSimple() {
		@SuppressWarnings("unchecked")
		DurationExpression modeExpr = new DurationExpression(
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
	public DurationExpression modeStrict() {
		@SuppressWarnings("unchecked")
		DurationExpression modeExpr = new DurationExpression(
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
	public BooleanExpression isLessThan(Duration period) {
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
	public BooleanExpression isLessThan(DurationResult anotherInstance) {
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
	public BooleanExpression isGreaterThan(Duration period) {
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
	public BooleanExpression isGreaterThan(DurationResult anotherInstance) {
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
	public BooleanExpression isLessThanOrEqual(Duration period) {
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
	public BooleanExpression isLessThanOrEqual(DurationResult anotherInstance) {
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
	public BooleanExpression isGreaterThanOrEqual(Duration period) {
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
	public BooleanExpression isGreaterThanOrEqual(DurationResult anotherInstance) {
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
	public BooleanExpression isLessThan(Duration period, BooleanExpression fallBackWhenEqual) {
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
	public BooleanExpression isLessThan(DurationResult anotherInstance, BooleanExpression fallBackWhenEqual) {
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
	public BooleanExpression isGreaterThan(Duration period, BooleanExpression fallBackWhenEqual) {
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
	public BooleanExpression isGreaterThan(DurationResult anotherInstance, BooleanExpression fallBackWhenEqual) {
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
	public BooleanExpression is(Duration period) {
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
	public BooleanExpression is(DurationResult anotherInstance) {
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
	public BooleanExpression isNot(Duration anotherInstance) {
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
	public BooleanExpression isNot(DurationResult anotherInstance) {
		return BooleanExpression.allOf(
				is(anotherInstance).not(),
				isNotNull());
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
	public DBDuration asExpressionColumn() {
		return new DBDuration(this);
	}

	@Override
	public BooleanExpression isBetween(DurationResult anotherInstance, DurationResult largerExpression) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(anotherInstance),
				this.isLessThan(anotherInstance)
		);
	}

	@Override
	public BooleanExpression isBetweenInclusive(DurationResult anotherInstance, DurationResult largerExpression) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(anotherInstance),
				this.isLessThanOrEqual(anotherInstance)
		);
	}

	@Override
	public BooleanExpression isBetweenExclusive(DurationResult anotherInstance, DurationResult largerExpression) {
		return BooleanExpression.allOf(
				this.isGreaterThan(anotherInstance),
				this.isLessThan(anotherInstance)
		);
	}

	@Override
	public BooleanExpression isInCollection(Collection<DurationResult> otherInstances) {
		List<StringResult> values = otherInstances.stream().map(v->v.stringResult()).collect(Collectors.toList());
		return this.stringResult().isInCollection(values);
	}

	@Override
	public BooleanExpression isNotInCollection(Collection<DurationResult> otherInstances) {
		List<StringResult> collect = otherInstances.stream().map(v-> v.stringResult()).collect(Collectors.toList());
		return this.stringResult().isNotInCollection(collect);
	}

	@Override
	public DurationExpression nullExpression() {
		return new NullExpression();
	}

	@Override
	public DurationResult expression(Duration value) {
		return new DurationExpression(value);
	}

	@Override
	public DurationResult expression(DurationResult value) {
		return new DurationExpression(value);
	}

	@Override
	public DurationResult expression(DBDuration value) {
		return value;
	}

	private static abstract class DateRepeatDateRepeatWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final DurationExpression first;
		private final DurationExpression second;
		private boolean requiresNullProtection;

		DateRepeatDateRepeatWithBooleanResult(DurationExpression first, DurationResult second) {
			this.first = first;
			this.second = new DurationExpression(second);
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		DurationExpression getFirst() {
			return first;
		}

		DurationExpression getSecond() {
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

		private final DurationExpression first;
		private boolean requiresNullProtection;

		DateRepeatWithNumberResult(DurationExpression first) {
			this.first = first;
		}

		protected abstract String doExpressionTransform(DBDefinition db);

		DurationExpression getFirst() {
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

		private final DurationExpression first;
		private boolean requiresNullProtection;

		DateRepeatWithIntegerResult(DurationExpression first) {
			this.first = first;
		}

		protected abstract String doExpressionTransform(DBDefinition db);

		DurationExpression getFirst() {
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

		private final DurationExpression first;
		private boolean requiresNullProtection;

		DateRepeatWithStringResult(DurationExpression first) {
			this.first = first;
		}

		protected abstract String doExpressionTransform(DBDefinition db);

		DurationExpression getFirst() {
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

		public IsLessThanExpression(DurationExpression first, DurationResult second) {
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

		public IsGreaterThanExpression(DurationExpression first, DurationResult second) {
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

		public IsLessThanOrEqualExpression(DurationExpression first, DurationResult second) {
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

		public IsGreaterThanOrEqualExpression(DurationExpression first, DurationResult second) {
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

		public IsExpression(DurationExpression first, DurationResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doDateRepeatEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				final DurationExpression left = this.getFirst();
				final DurationExpression right = this.getSecond();
				return BooleanExpression.allOf(
						left.getDays().is(right.getDays()),
						left.getHours().is(right.getHours()),
						left.getMinutes().is(right.getMinutes()),
						left.getSeconds().is(right.getSeconds())
				).toSQLString(db);
			}
		}

		@Override
		public DurationExpression.IsExpression copy() {
			return new DurationExpression.IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class GetDaysExpression extends DateRepeatWithIntegerResult {

		public GetDaysExpression(DurationExpression first) {
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
						getFirst()
								.stringResult()
								.substringBefore(DAY_SUFFIX)
								.substringAfter(INTERVAL_PREFIX)
								.numberResult()
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

		public GetHoursExpression(DurationExpression first) {
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

		public GetMinutesExpression(DurationExpression first) {
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

		public GetSecondsExpression(DurationExpression first) {
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

		public StringResultExpression(DurationExpression first) {
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

	private static class NullExpression extends DurationExpression {

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

	public static WindowFunctionFramable<DurationExpression> firstValue() {
		return new FirstValueExpression().over();
	}

	public static class FirstValueExpression extends DurationExpression implements CanBeWindowingFunctionWithFrame<DurationExpression> {

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
		public WindowFunctionFramable<DurationExpression> over() {
			return new WindowFunctionFramable<DurationExpression>(new DurationExpression(this));
		}

	}

	public static WindowFunctionFramable<DurationExpression> lastValue() {
		return new LastValueExpression().over();
	}

	public static class LastValueExpression extends DurationExpression implements CanBeWindowingFunctionWithFrame<DurationExpression> {

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
		public WindowFunctionFramable<DurationExpression> over() {
			return new WindowFunctionFramable<DurationExpression>(new DurationExpression(this));
		}

	}

	public static WindowFunctionFramable<DurationExpression> nthValue(IntegerExpression indexExpression) {
		return new NthValueExpression(indexExpression).over();
	}

	public static class NthValueExpression extends DurationExpression implements CanBeWindowingFunctionWithFrame<DurationExpression> {

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
		public WindowFunctionFramable<DurationExpression> over() {
			return new WindowFunctionFramable<DurationExpression>(new DurationExpression(this));
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
	public WindowFunctionFramable<DurationExpression> lag() {
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
	public WindowFunctionFramable<DurationExpression> lag(IntegerExpression offset) {
		return lag(offset, nullDuration());
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
	public WindowFunctionFramable<DurationExpression> lag(IntegerExpression offset, DurationExpression defaultExpression) {
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
	public WindowFunctionFramable<DurationExpression> lead() {
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
	public WindowFunctionFramable<DurationExpression> lead(IntegerExpression offset) {
		return lead(offset, nullDuration());
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
	public WindowFunctionFramable<DurationExpression> lead(IntegerExpression offset, DurationExpression defaultExpression) {
		return new LeadExpression(this, offset, defaultExpression).over();
	}

	private static abstract class LagLeadFunction extends DurationExpression implements CanBeWindowingFunctionWithFrame<DurationExpression> {

		private static final long serialVersionUID = 1L;

		protected DurationExpression first;
		protected IntegerExpression second;
		protected DurationExpression third;

		LagLeadFunction(DurationExpression first, IntegerExpression second, DurationExpression third) {
			this.first = first;
			this.second = second == null ? value(1) : second;
			this.third = third == null ? nullDuration() : third;
		}

		@Override
		public DBDuration getQueryableDatatypeForExpressionValue() {
			return new DBDuration();
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
		protected DurationExpression getFirst() {
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
		protected DurationExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
		}

		@Override
		public WindowFunctionFramable<DurationExpression> over() {
			return new WindowFunctionFramable<>(new DurationExpression(this));
		}
	}

	public class LagExpression extends LagLeadFunction {

		private static final long serialVersionUID = 1L;

		public LagExpression(DurationExpression first, IntegerExpression second, DurationExpression third) {
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

		public LeadExpression(DurationExpression first, IntegerExpression second, DurationExpression third) {
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
