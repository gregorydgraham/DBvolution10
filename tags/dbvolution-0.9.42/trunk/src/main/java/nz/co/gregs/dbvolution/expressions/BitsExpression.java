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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBits;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * The Expression object for bit array columns.
 *
 * @author gregory.graham
 */
public class BitsExpression implements BitsResult {

	private final BitsResult innerBitsResult;

	/**
	 * Default Constructor.
	 */
	protected BitsExpression() {
		this.innerBitsResult = new DBBits();
	}

	/**
	 * Create a BitsExpression from an existing BitResult object.
	 *
	 * @param bitResult
	 */
	public BitsExpression(BitsResult bitResult) {
		this.innerBitsResult = bitResult;
	}

	@Override
	public BitsExpression copy() {
		return new BitsExpression(this.getInnerBitsResult());
	}

	/**
	 * Create An Appropriate BitsExpression Object For This Object.
	 *
	 * <p>
	 * The expression framework requires an Expression to work with. The easiest
	 * way to get that are the {@code DBRow.column()} methods.
	 *
	 * <p>
	 * However if you wish your expression to start with a literal value it is
	 * even easier.
	 *
	 * <p>
	 * This method provides the easy route to an expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param number integer to base the BitsExpression on.
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static BitsExpression value(int number) {
		return NumberExpression.value(number).convertToBits();
	}

	/**
	 * Create An Appropriate BitsExpression Object For This Object.
	 *
	 * <p>
	 * The expression framework requires an Expression to work with. The easiest
	 * way to get that are the {@code DBRow.column()} methods.
	 *
	 * <p>
	 * However if you wish your expression to start with a literal value it is
	 * even easier.
	 *
	 * <p>
	 * This method provides the easy route to an expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param number integer to base the BitsExpression on.
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static BitsExpression value(long number) {
		return NumberExpression.value(number).convertToBits();
	}

	/**
	 * Create An Appropriate BitsExpression Object For This Object.
	 *
	 * <p>
	 * The expression framework requires an Expression to work with. The easiest
	 * way to get that are the {@code DBRow.column()} methods.
	 *
	 * <p>
	 * However if you wish your expression to start with a literal value it is
	 * even easier.
	 *
	 * <p>
	 * This method provides the easy route to an expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param number integer to base the BitsExpression on.
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static BitsExpression value(double number) {
		return NumberExpression.value(number).convertToBits();
	}

	/**
	 * Create An Appropriate BitsExpression Object For This Object.
	 *
	 * <p>
	 * The expression framework requires an Expression to work with. The easiest
	 * way to get that are the {@code DBRow.column()} methods.
	 *
	 * <p>
	 * However if you wish your expression to start with a literal value it is
	 * even easier.
	 *
	 * <p>
	 * This method provides the easy route to an expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param number integer to base the BitsExpression on.
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static BitsExpression value(Number number) {
		return NumberExpression.value(number).convertToBits();
	}

	@Override
	public QueryableDatatype getQueryableDatatypeForExpressionValue() {
		return new DBBits();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerBitsResult != null) {
			return innerBitsResult.toSQLString(db);
		} else {
			return "";
		}
	}

	@Override
	public boolean isAggregator() {
		if (innerBitsResult != null) {
			return innerBitsResult.isAggregator();
		} else {
			return false;
		}
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		if (innerBitsResult != null) {
			return innerBitsResult.getTablesInvolved();
		} else {
			return new HashSet<DBRow>();
		}
	}

	@Override
	public boolean getIncludesNull() {
		if (innerBitsResult != null) {
			return innerBitsResult.getIncludesNull();
		} else {
			return false;
		}
	}

	/**
	 * Return the BitsResult held internally in this class.
	 *
	 * @return The BitsResult used internally.
	 */
	protected BitsResult getInnerBitsResult() {
		return innerBitsResult;
	}

	/**
	 * Create a BooleanExpression that will compare the integer provided to this
	 * BitsExpression using the equivalent of the EQUALS operator.
	 *
	 * @param i
	 * @return a BooleanExpresson of the Bit comparison of the integer and this expression.
	 */
	public BooleanExpression is(int i) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, NumberExpression.value(i).convertToBits()) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	/**
	 * Create a BooleanExpression that will compare the long provided to this
	 * BitsExpression using the equivalent of the EQUALS operator.
	 *
	 * @param i
	 * @return a BooleanExpresson of the Bit comparison of the long and this expression.
	 */
	public BooleanExpression is(long i) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, NumberExpression.value(i).convertToBits()) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	/**
	 * Create a BooleanExpression that will compare the number provided to this
	 * BitsExpression using the equivalent of the EQUALS operator.
	 *
	 * @param i
	 * @return a BooleanExpresson of the Bit comparison of the number and this expression.
	 */
	public BooleanExpression is(Number i) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, NumberExpression.value(i).convertToBits()) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

		private BitsExpression first;
		private BitsResult second;
		private boolean requiresNullProtection;

		DBBinaryBooleanArithmetic(BitsExpression first, BitsResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
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
