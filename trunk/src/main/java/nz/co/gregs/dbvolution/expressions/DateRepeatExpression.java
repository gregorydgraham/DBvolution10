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
 * @author gregory.graham
 */
public class DateRepeatExpression implements DateRepeatResult, RangeComparable<DateRepeatResult> {

	DateRepeatResult innerDateRepeatResult = null;
	private boolean nullProtectionRequired = false;

	public static final String INTERVAL_PREFIX = "P";
	public static final String YEAR_SUFFIX = "Y";
	public static final String MONTH_SUFFIX = "M";
	public static final String DAY_SUFFIX = "D";
	public static final String HOUR_SUFFIX = "h";
	public static final String MINUTE_SUFFIX = "n";
	public static final String SECOND_SUFFIX = "s";

	public DateRepeatExpression() {
	}

	public DateRepeatExpression(Period interval) {
		innerDateRepeatResult = new DBDateRepeat(interval);
		if (interval == null || innerDateRepeatResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public DateRepeatExpression(DateRepeatResult interval) {
		innerDateRepeatResult = interval;
		if (interval == null || innerDateRepeatResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public static DateRepeatExpression value(Period period) {
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
		return innerDateRepeatResult.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired || innerDateRepeatResult.getIncludesNull();
	}

	/**
	 * Returns TRUE if this expression evaluates to NULL, otherwise FALSE.
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	public BooleanExpression isLessThan(Period period) {
		return this.isLessThan(value(period));
	}

	@Override
	public BooleanExpression isLessThan(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatLessThanTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression isGreaterThan(Period period) {
		return this.isGreaterThan(value(period));
	}

	@Override
	public BooleanExpression isGreaterThan(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatGreaterThanTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression isLessThanOrEqual(Period period) {
		return this.isLessThanOrEqual(value(period));
	}

	@Override
	public BooleanExpression isLessThanOrEqual(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatLessThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression isGreaterThanOrEqual(Period period) {
		return this.isGreaterThanOrEqual(value(period));
	}

	@Override
	public BooleanExpression isGreaterThanOrEqual(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatGreaterThanEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}

		});
	}

	public BooleanExpression isLessThan(Period period, BooleanExpression fallBackWhenEqual) {
		return this.isLessThan(value(period), fallBackWhenEqual);
	}

	@Override
	public BooleanExpression isLessThan(DateRepeatResult anotherInstance, BooleanExpression fallBackWhenEqual) {
		return this.isLessThan(anotherInstance).or(this.is(anotherInstance).and(fallBackWhenEqual));
	}

	public BooleanExpression isGreaterThan(Period period, BooleanExpression fallBackWhenEqual) {
		return this.isGreaterThan(value(period), fallBackWhenEqual);
	}

	@Override
	public BooleanExpression isGreaterThan(DateRepeatResult anotherInstance, BooleanExpression fallBackWhenEqual) {
		return this.isGreaterThan(anotherInstance).or(this.is(anotherInstance).and(fallBackWhenEqual));
	}

	public BooleanExpression is(Period period) {
		return this.is(value(period));
	}

	@Override
	public BooleanExpression is(DateRepeatResult anotherInstance) {
		return new BooleanExpression(new DateRepeatDateRepeatWithBooleanResult(this, anotherInstance) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public NumberExpression getYears() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetYearsTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().stringBefore(YEAR_SUFFIX).stringAfter(INTERVAL_PREFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
		);
	}

	public NumberExpression getMonths() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetMonthsTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().stringBefore(MONTH_SUFFIX).stringAfter(YEAR_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
		);
	}

	public NumberExpression getDays() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetDaysTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().stringBefore(DAY_SUFFIX).stringAfter(MONTH_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
		);
	}

	public NumberExpression getHours() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetHoursTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().stringBefore(HOUR_SUFFIX).stringAfter(DAY_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
		);
	}

	public NumberExpression getMinutes() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetMinutesTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().stringBefore(MINUTE_SUFFIX).stringAfter(HOUR_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
		);
	}

	public NumberExpression getSeconds() {
		return new NumberExpression(new DateRepeatWithNumberResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
					return db.getDefinition().doDateRepeatGetSecondsTransform(getFirst().toSQLString(db));
				} else {
					return BooleanExpression.isNull(getFirst()).ifThenElse(
							NumberExpression.nullExpression(),
							getFirst().stringResult().stringBefore(SECOND_SUFFIX).stringAfter(MINUTE_SUFFIX).numberResult()
					).toSQLString(db);
				}
			}
		}
		);
	}

//	public NumberExpression getMilliseconds() {
//		return new NumberExpression(new DateRepeatWithNumberResult(this) {
//
//			@Override
//			protected String doExpressionTransform(DBDatabase db) {
//				if (db instanceof SupportsDateRepeatDatatypeFunctions) {
//					return db.getDefinition().doDateRepeatGetMillisecondsTransform(getFirst().toSQLString(db));
//				} else {
//					return BooleanExpression.isNull(getFirst()).ifThenElse(
//							NumberExpression.nullExpression(),
//							getFirst().stringResult().stringBefore(SECOND_SUFFIX).stringAfter(MINUTE_SUFFIX).numberResult().bracket().decimalPart().bracket().times(1000).bracket())
//							.toSQLString(db);
//				}
//			}
//		}
//		);
//	}

	/**
	 * Converts the interval expression into a string/character expression.
	 *
	 * @return a StringExpression of the interval expression.
	 */
	public StringExpression stringResult() {
		return new StringExpression(new DateRepeatWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doDateRepeatToStringTransform(getFirst().toSQLString(db));
			}
		});
	}

	private static abstract class DateRepeatDateRepeatWithBooleanResult extends BooleanExpression {

		private DateRepeatExpression first;
		private DateRepeatResult second;
		private boolean requiresNullProtection;

		DateRepeatDateRepeatWithBooleanResult(DateRepeatExpression first, DateRepeatResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		DateRepeatExpression getFirst() {
			return first;
		}

		DateRepeatResult getSecond() {
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
