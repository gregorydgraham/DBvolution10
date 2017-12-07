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

import nz.co.gregs.dbvolution.results.RangeComparable;
import nz.co.gregs.dbvolution.results.DateRepeatResult;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDateRepeat;
import org.joda.time.Period;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class DateRepeatExpression implements DateRepeatResult, RangeComparable<DateRepeatResult>, ExpressionColumn<DBDateRepeat> {

	DateRepeatResult innerDateRepeatResult = null;
	private boolean nullProtectionRequired = false;

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
	}

	/**
	 * Creates a new DateRepeatExression that represents the value supplied.
	 *
	 * @param interval
	 */
	public DateRepeatExpression(Period interval) {
		innerDateRepeatResult = new DBDateRepeat(interval);
		if (interval == null || innerDateRepeatResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Creates a new DateRepeatExression that represents the DateRepeat value
	 * supplied.
	 *
	 * @param interval the time period from which to create a DateRepeat value
	 */
	public DateRepeatExpression(DateRepeatResult interval) {
		innerDateRepeatResult = interval;
		if (interval == null || innerDateRepeatResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Creates a new DateRepeatExression that represents the value supplied.
	 *
	 * <p>
	 * Equivalent to {@code new DateRepeatExpression(period)}
	 *
	 * @param period the time period from which to create a DateRepeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateRepeat expression representing the value supplied.
	 */
	public static DateRepeatExpression value(Period period) {
		return new DateRepeatExpression(period);
	}

	/**
	 * Creates a new DateRepeatExpression that represents the value supplied.
	 *
	 * <p>
	 * Equivalent to {@code new DateRepeatExpression(period)}
	 *
	 * @param period the time period from which to create a DateRepeat value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DateRepeat expression representing the value supplied.
	 */
	public static DateRepeatExpression value(DateRepeatResult period) {
		return new DateRepeatExpression(period);
	}

	@Override
	public DateRepeatExpression copy() {
		return new DateRepeatExpression(innerDateRepeatResult);
	}

	@Override
	public DBDateRepeat getQueryableDatatypeForExpressionValue() {
		return new DBDateRepeat();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return innerDateRepeatResult.toSQLString(db);
	}

	@Override
	public boolean isAggregator() {
		return innerDateRepeatResult.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return innerDateRepeatResult.getTablesInvolved();
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerDateRepeatResult == null ? true : innerDateRepeatResult.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired || innerDateRepeatResult.getIncludesNull();
	}

	/**
	 * Returns TRUE if this expression evaluates to NULL, otherwise FALSE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression
	 */
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
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatLessThanTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
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
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatGreaterThanTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
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
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatLessThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
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
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatGreaterThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}

		});
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
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doDateRepeatEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
		});
	}

	/**
	 * Returns FALSE if this expression and the provided value are the same.
	 *
	 * @param anotherInstance a value that the expression should not match.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
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
	public NumberExpression getYears() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetYearsTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().substringBefore(YEAR_SUFFIX).substringAfter(INTERVAL_PREFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
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
	public NumberExpression getMonths() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetMonthsTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().substringBefore(MONTH_SUFFIX).substringAfter(YEAR_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
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
	public NumberExpression getDays() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetDaysTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().substringBefore(DAY_SUFFIX).substringAfter(MONTH_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
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
	public NumberExpression getHours() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetHoursTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().substringBefore(HOUR_SUFFIX).substringAfter(DAY_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
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
	public NumberExpression getMinutes() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetMinutesTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().substringBefore(MINUTE_SUFFIX).substringAfter(HOUR_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
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
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetSecondsTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().substringBefore(SECOND_SUFFIX).substringAfter(MINUTE_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
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
		return new StringExpression(new DateRepeatWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatToStringTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public DBDateRepeat asExpressionColumn() {
		return new DBDateRepeat(this);
	}

	private static abstract class DateRepeatDateRepeatWithBooleanResult extends BooleanExpression {

		private DateRepeatExpression first;
		private DateRepeatExpression second;
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
		public String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public DateRepeatDateRepeatWithBooleanResult copy() {
			DateRepeatDateRepeatWithBooleanResult newInstance;
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

		protected abstract String doExpressionTransform(DBDatabase db);

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

		private DateRepeatExpression first;
//		private DateRepeatResult second;
		private boolean requiresNullProtection;

		DateRepeatWithNumberResult(DateRepeatExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		DateRepeatExpression getFirst() {
			return first;
		}

//		DateRepeatResult getSecond() {
//			return second;
//		}
		@Override
		public String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public DateRepeatWithNumberResult copy() {
			DateRepeatWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class DateRepeatWithStringResult extends StringExpression {

		private DateRepeatExpression first;
//		private DateRepeatResult second;
		private boolean requiresNullProtection;

		DateRepeatWithStringResult(DateRepeatExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		DateRepeatExpression getFirst() {
			return first;
		}

//		DateRepeatResult getSecond() {
//			return second;
//		}
		@Override
		public String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public DateRepeatWithStringResult copy() {
			DateRepeatWithStringResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}
}
