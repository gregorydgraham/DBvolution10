/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.BooleanArrayResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;

/**
 * The Expression object for bit array columns.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class BooleanArrayExpression extends AnyExpression<Boolean[], BooleanArrayResult, DBBooleanArray> implements BooleanArrayResult, EqualComparable<Boolean[], BooleanArrayResult>, ExpressionColumn<DBBooleanArray> {

	private final static long serialVersionUID = 1l;

	public static BooleanArrayExpression value(Boolean[] i) {
		return new BooleanArrayExpression(i);
	}

	private final BooleanArrayResult innerBooleanArrayResult;

	/**
	 * Default Constructor.
	 */
	protected BooleanArrayExpression() {
		this.innerBooleanArrayResult = new DBBooleanArray();
	}

	/**
	 * Create a BitsExpression from an existing BitResult object.
	 *
	 * @param bitResult	bitResult
	 */
	public BooleanArrayExpression(BooleanArrayResult bitResult) {
		this.innerBooleanArrayResult = bitResult;
	}

	private BooleanArrayExpression(Boolean[] bools) {
		this.innerBooleanArrayResult = new DBBooleanArray(bools);
	}

	@Override
	public BooleanArrayExpression copy() {
		return new BooleanArrayExpression(this.getInnerBooleanArrayResult().copy());
	}

	@Override
	public DBBooleanArray getQueryableDatatypeForExpressionValue() {
		return new DBBooleanArray();
	}

	@Override
	public String toSQLString(DBDefinition db) {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.toSQLString(db);
		} else {
			return "";
		}
	}

	@Override
	public boolean isAggregator() {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.isAggregator();
		} else {
			return false;
		}
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.getTablesInvolved();
		} else {
			return new HashSet<>();
		}
	}

	@Override
	public boolean getIncludesNull() {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.getIncludesNull();
		} else {
			return false;
		}
	}

	@Override
	public boolean isPurelyFunctional() {
		if (innerBooleanArrayResult == null) {
			return true;
		} else {
			return innerBooleanArrayResult.isPurelyFunctional();
		}
	}

	/**
	 * Return the BooleanArrayResult held internally in this class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return The BooleanArrayResult used internally.
	 */
	protected BooleanArrayResult getInnerBooleanArrayResult() {
		return innerBooleanArrayResult;
	}

	/**
	 * Create a BooleanExpression that will compare the BooleanArrayResult
	 * provided to this BooleanArrayExpression using the equivalent of the EQUALS
	 * operator.
	 *
	 * @param bools the value to compare this expression to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpresson of the Bit comparison of the number and this
	 * expression.
	 */
	@Override
	public BooleanExpression is(Boolean[] bools) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, new BooleanArrayExpression(bools)) {
			private final static long serialVersionUID = 1l;

			@Override
			protected String getEquationOperator(DBDefinition db) {
				return " = ";
			}
		});
	}

	/**
	 * Create a BooleanExpression that will compare the BooleanArrayResult
	 * provided to this BooleanArrayExpression using the equivalent of the EQUALS
	 * operator.
	 *
	 * @param i the value to compare this expression to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpresson of the Bit comparison of the number and this
	 * expression.
	 */
	@Override
	public BooleanExpression is(BooleanArrayResult i) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, i) {
			private final static long serialVersionUID = 1l;

			@Override
			protected String getEquationOperator(DBDefinition db) {
				return " = ";
			}
		});
	}

	/**
	 * Create a BooleanExpression that will compare the BooleanArrayResult
	 * provided to this BooleanArrayExpression using the equivalent of the NOT
	 * EQUALS operator.
	 *
	 * @param i the value to compare this expression to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpresson of the Bit comparison of the number and this
	 * expression.
	 */
	@Override
	public BooleanExpression isNot(Boolean[] i) {
		return this.isNot(BooleanArrayExpression.value(i));
	}

	/**
	 * Create a BooleanExpression that will compare the BooleanArrayResult
	 * provided to this BooleanArrayExpression using the equivalent of the NOT
	 * EQUALS operator.
	 *
	 * @param i the value to compare this expression to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpresson of the Bit comparison of the number and this
	 * expression.
	 */
	@Override
	public BooleanExpression isNot(BooleanArrayResult i) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, i) {
			private final static long serialVersionUID = 1l;

			@Override
			protected String getEquationOperator(DBDefinition db) {
				return " <> ";
			}
		});
	}

	@Override
	public DBBooleanArray asExpressionColumn() {
		return new DBBooleanArray(this);
	}

	@Override
	public BooleanArrayResult expression(Boolean[] value) {
		return new BooleanArrayExpression(value);
	}

	@Override
	public BooleanArrayResult expression(BooleanArrayResult value) {
		return new BooleanArrayExpression(value);
	}

	@Override
	public BooleanArrayResult expression(DBBooleanArray value) {
		return new BooleanArrayExpression(value);
	}

	@Override
	public StringExpression stringResult() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	private static abstract class DBBinaryBooleanArithmetic extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private BooleanArrayExpression first;
		private BooleanArrayResult second;
		private boolean requiresNullProtection;

		DBBinaryBooleanArithmetic(BooleanArrayExpression first, BooleanArrayResult second) {
			this.first = first;
			this.second = second;
			if (this.first == null || this.first.getIncludesNull() || this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return "(" + BooleanExpression.isNull(first).toSQLString(db) + ")";
			} else {
				return "(" + first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db) + ")";
			}
		}

		@Override
		public DBBinaryBooleanArithmetic copy() {
			DBBinaryBooleanArithmetic newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String getEquationOperator(DBDefinition db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
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
			if (first == null && second == null) {
				return false;
			} else if (first == null) {
				return second.isAggregator();
			} else if (second == null) {
				return first.isAggregator();
			} else {
				return first.isAggregator() || second.isAggregator();
			}
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
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

	@Override
	public String createSQLForFromClause(DBDatabase database) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isComplexExpression() {
		return false;
	}
}
