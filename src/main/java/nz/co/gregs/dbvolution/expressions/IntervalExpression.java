/*
 * Copyright 2015 gregory.graham.
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
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInterval;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import org.joda.time.Period;

/**
 *
 * @author gregory.graham
 */
public class IntervalExpression  implements IntervalResult, RangeComparable<IntervalResult> {
	
	IntervalResult innerIntervalResult = null;
	private boolean nullProtectionRequired = false;
	
	protected IntervalExpression(){
	}
	
	public IntervalExpression(Period interval){
		innerIntervalResult = new DBInterval(interval);
		if (interval == null || innerIntervalResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public IntervalExpression(IntervalResult interval){
		innerIntervalResult = interval;
		if (interval == null || innerIntervalResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	@Override
	public IntervalExpression copy() {
		return new IntervalExpression(innerIntervalResult);
	}

	@Override
	public DBInterval getQueryableDatatypeForExpressionValue() {
		return new DBInterval();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isAggregator() {
		return innerIntervalResult.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return innerIntervalResult.getTablesInvolved();
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerIntervalResult.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired||innerIntervalResult.getIncludesNull();
	}

	@Override
	public BooleanExpression isLessThan(IntervalResult anotherInstance) {
		return new BooleanExpression(new IntervalIntervalBooleanArithmetic(this, anotherInstance) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (this.getIncludesNull()) {
					return BooleanExpression.isNull(getFirst()).toSQLString(db);
				} else {
					return db.getDefinition().doNumberEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				}
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " < ";
			}
		});
	}

	@Override
	public BooleanExpression isGreaterThan(IntervalResult anotherInstance) {
		return new BooleanExpression(new IntervalIntervalBooleanArithmetic(this, anotherInstance) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (this.getIncludesNull()) {
					return BooleanExpression.isNull(getFirst()).toSQLString(db);
				} else {
					return db.getDefinition().doNumberEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				}
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " > ";
			}
		});
	}

	@Override
	public BooleanExpression isLessThanOrEqual(IntervalResult anotherInstance) {
		return new BooleanExpression(new IntervalIntervalBooleanArithmetic(this, anotherInstance) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (this.getIncludesNull()) {
					return BooleanExpression.isNull(getFirst()).toSQLString(db);
				} else {
					return db.getDefinition().doNumberEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				}
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " <= ";
			}
		});
	}

	@Override
	public BooleanExpression isGreaterThanOrEqual(IntervalResult anotherInstance) {
		return new BooleanExpression(new IntervalIntervalBooleanArithmetic(this, anotherInstance) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (this.getIncludesNull()) {
					return BooleanExpression.isNull(getFirst()).toSQLString(db);
				} else {
					return db.getDefinition().doNumberEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				}
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " >= ";
			}
		});
	}

	@Override
	public BooleanExpression isLessThan(IntervalResult anotherInstance, BooleanExpression fallBackWhenEqual) {
		return this.isLessThan(anotherInstance).or(this.is(anotherInstance).and(fallBackWhenEqual));
	}

	@Override
	public BooleanExpression isGreaterThan(IntervalResult anotherInstance, BooleanExpression fallBackWhenEqual) {
		return this.isGreaterThan(anotherInstance).or(this.is(anotherInstance).and(fallBackWhenEqual));
	}

	@Override
	public BooleanExpression is(IntervalResult anotherInstance) {
		return new BooleanExpression(new IntervalIntervalBooleanArithmetic(this, anotherInstance) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (this.getIncludesNull()) {
					return BooleanExpression.isNull(getFirst()).toSQLString(db);
				} else {
					return db.getDefinition().doNumberEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				}
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}
	

	private static abstract class IntervalIntervalBooleanArithmetic extends BooleanExpression {

		private IntervalExpression first;
		private IntervalResult second;
		private boolean requiresNullProtection;

		IntervalIntervalBooleanArithmetic(IntervalExpression first, IntervalResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		IntervalExpression getFirst() {
			return first;
		}

		IntervalResult getSecond() {
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
				return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
			}
		}

		@Override
		public IntervalIntervalBooleanArithmetic copy() {
			IntervalIntervalBooleanArithmetic newInstance;
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

		protected abstract String getEquationOperator(DBDatabase db);

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
}
