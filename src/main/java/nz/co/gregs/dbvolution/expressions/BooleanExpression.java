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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * BooleanExpression implements standard functions that produce a Boolean or
 * TRUE/FALSE result.
 *
 * <p>
 * Most query requirements are provided by {@link QueryableDatatype}s like
 * {@link DBString} or {@link DBInteger} but expressions can provide more
 * functions or more precise control.
 *
 * <p>
 * Use a BooleanExpression to produce a conditional expression as used in
 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}.
 *
 * <p>
 * Generally you get a BooleanExpression using an "is" method from one of the
 * other DBExpressions but you can use
 * {@link BooleanExpression#value(java.lang.Boolean)} or
 * {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBBoolean)} to start
 * your Boolean expression.
 *
 * <p>
 * BooleanExpression also provides the means for grouping BooleanExpressions
 * together with the
 * {@link #allOf(nz.co.gregs.dbvolution.expressions.BooleanExpression...) allOf}
 * or
 * {@link #anyOf(nz.co.gregs.dbvolution.expressions.BooleanExpression...) anyOf}
 * methods.
 *
 * <p>
 * There are also comparisons with NULL, negations, and static true and false
 * expressions.
 *
 * @author Gregory Graham
 */
public class BooleanExpression implements BooleanResult {

	private final BooleanResult onlyBool;
	private boolean includeNulls;

	/**
	 * Default Constructor for creating new BooleanExpressions.
	 *
	 * <p>
	 * The BooleanExpression created has no value or operation and is generally
	 * useless for the end user. however it is required for sub-classing.
	 *
	 */
	protected BooleanExpression() {
		onlyBool = new DBBoolean();
	}

	/**
	 * The normal method for creating a BooleanExpression.
	 *
	 * <p>
	 * BooleanExpressions generally wrap other BooleanExpressions or similar
	 * objects and add functionality to them. Use this constructor to wrap an
	 * existing BooleanExpression.
	 *
	 * @param booleanResult
	 */
	public BooleanExpression(BooleanResult booleanResult) {
		onlyBool = booleanResult;
		if (booleanResult.getIncludesNull()) {
			this.includeNulls = true;
		}
	}

	/**
	 * The easy way to create a BooleanExpression based on a literal value.
	 *
	 * <p>
	 * BooleanExpressions generally wrap other BooleanExpressions or similar
	 * objects and add functionality to them. Use this constructor to wrap a
	 * known value for use in a BooleanExpression.
	 *
	 * @param bool
	 */
	private BooleanExpression(Boolean bool) {
		onlyBool = new DBBoolean(bool);
		if (bool == null) {
			includeNulls = true;
		}
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return onlyBool.toSQLString(db);
	}

	@Override
	public BooleanExpression copy() {
		return new BooleanExpression(this.onlyBool);
	}

	/**
	 * Create An Appropriate BooleanExpression Object For This Object
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
	 * This method provides the easy route to a *Expression from a literal
	 * value. Just call, for instance,
	 * {@code StringExpression.value("STARTING STRING")} to get a
	 * StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param bool
	 * @return a DBExpression instance that is appropriate to the subclass and
	 * the value supplied.
	 */
	public static BooleanExpression value(Boolean bool) {
		return new BooleanExpression(bool);
	}

	/**
	 * Compare this BooleanExpression and the given boolean using the equality
	 * operator, that is "=" or similar.
	 *
	 * @param bool
	 * @return a BooleanExpression that compares the previous BooleanExpression
	 * to the Boolean supplied.
	 */
	public BooleanExpression is(Boolean bool) {
		return is(new BooleanExpression(bool));
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the equality operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool
	 * @return a BooleanExpression that compares the previous BooleanExpression
	 * to the Boolean supplied.
	 */
	public BooleanExpression is(BooleanResult bool) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, bool) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the Exclusive OR operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool
	 * @return a BooleanExpression of an XOR operation.
	 */
	public BooleanExpression xor(BooleanResult bool) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, bool) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (db.getDefinition().supportsXOROperator()) {
					return super.toSQLString(db);
				} else {
					return BooleanExpression.anyOf(
							BooleanExpression.allOf(
									this.getFirst(), this.getSecond().not()
							),
							BooleanExpression.allOf(
									this.getFirst().not(), this.getSecond())
					).toSQLString(db);
				}
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return "^";
			}
		});
	}

	/**
	 * Collects the expressions together and requires them all to be true.
	 *
	 * <p>
	 * Creates a BooleanExpression of several Boolean Expressions by connecting
	 * them using AND repeatedly.
	 *
	 * <p>
	 * This expression returns true if and only if all the component expressions
	 * are true
	 *
	 * @param booleanExpressions
	 * @return a boolean expression that returns true IFF all the
	 * booleanExpressions are true.
	 * @see #anyOf(BooleanExpression...)
	 */
	public static BooleanExpression allOf(final BooleanExpression... booleanExpressions) {
		return new BooleanExpression(new DBNnaryBooleanArithmetic(booleanExpressions) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return db.getDefinition().beginAndLine();
			}
		});
	}

	/**
	 * Collects the expressions together and only requires one to be true.
	 *
	 * <p>
	 * Creates a BooleanExpression of several Boolean Expressions by connecting
	 * them using OR repeatedly.
	 *
	 * <p>
	 * This expression returns true if any of the component expressions is true
	 *
	 * @param booleanExpressions
	 * @return a boolean expression that returns true if any of the
	 * booleanExpressions is true.
	 * @see #allOf(BooleanExpression...)
	 */
	public static BooleanExpression anyOf(final BooleanExpression... booleanExpressions) {
		return new BooleanExpression(new DBNnaryBooleanArithmetic(booleanExpressions) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return db.getDefinition().beginOrLine();
			}
		});
	}

	/**
	 * Returns true only if all of the conditions are FALSE.
	 *
	 * @param booleanExpressions
	 * @return a boolean expression that returns true if all of the
	 * booleanExpressions evaluate to FALSE.
	 * @see #allOf(BooleanExpression...)
	 * @see #anyOf(BooleanExpression...)
	 */
	public static BooleanExpression noneOf(final BooleanExpression... booleanExpressions) {
		return BooleanExpression.anyOf(booleanExpressions).not();
	}

	/**
	 * Negates this BooleanExpression.
	 *
	 * <p>
	 * The 3 main boolean operators are AND, OR, and NOT. This method implements
	 * NOT.
	 *
	 * <p>
	 * The boolean result of the expression will be negated by this call so that
	 * TRUE becomes FALSE and FALSE becomes TRUE.
	 *
	 * <p>
	 * Please note that databases use
	 * <a href="https://en.wikipedia.org/wiki/Three-valued_logic">Three-valued
	 * logic</a>
	 * so {@link QueryableDatatype#isDBNull NULL} is also a valid result of this
	 * expression
	 *
	 * @return a Boolean expression representing the negation of the current
	 * expression.
	 */
	public BooleanExpression negate() {
		return new BooleanExpression(new DBUnaryBinaryFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getNegationFunctionName();
			}
		});
	}

	/**
	 * Converts boolean values to the database integer representation.
	 *
	 * @return a boolean
	 */
	public NumberExpression convertToInteger() {
		return new NumberExpression() {
			BooleanExpression innerBool = new BooleanExpression(onlyBool);

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doBitsToIntegerTransform(this.innerBool.toSQLString(db));
			}

			@Override
			public NumberExpression copy() {
				try {
					return (NumberExpression) this.clone();
				} catch (CloneNotSupportedException ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public boolean getIncludesNull() {
				return innerBool.getIncludesNull();
			}

			@Override
			public DBNumber getQueryableDatatypeForExpressionValue() {
				return new DBNumber();
			}

			@Override
			public boolean isAggregator() {
				return false;
			}

			@Override
			public Set<DBRow> getTablesInvolved() {
				return innerBool.getTablesInvolved();
			}
		};
	}

	/**
	 * Returns FALSE if this expression is TRUE, or TRUE if it is FALSE.
	 *
	 * <p>
	 * Synonym for {@link #negate() the negate() method}
	 *
	 * <p>
	 * The 3 main boolean operators are AND, OR, and NOT. This method implements
	 * NOT.
	 *
	 * <p>
	 * The boolean result of the expression will be negated by this call so that
	 * TRUE becomes FALSE and FALSE becomes TRUE.
	 *
	 * <p>
	 * Please note that databases use
	 * <a href="https://en.wikipedia.org/wiki/Three-valued_logic">Three-valued
	 * logic</a>
	 * so {@link QueryableDatatype#isDBNull NULL} is also a valid result of this
	 * expression
	 *
	 * @return a Boolean expression representing the negation of the current
	 * expression.
	 */
	public BooleanExpression not() {
		return this.negate();
	}

	/**
	 * Returns FALSE if the given {@link DBExpression} evaluates to NULL,
	 * otherwise TRUE.
	 *
	 * <p>
	 * DBExpression subclasses include
	 * {@link QueryableDatatype QueryableDatatypes} like {@link DBString} and
	 * {@link DBInteger} as well as
	 * {@link NumberExpression}, {@link StringExpression}, {@link DateExpression},
	 * and {@link LargeObjectExpression}.
	 *
	 * @param possibleNullExpression
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNotNull(DBExpression possibleNullExpression) {
		return new BooleanExpression(new DBUnaryBooleanArithmetic(possibleNullExpression) {

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " IS NOT " + db.getDefinition().getNull();
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Returns FALSE if the given {@link ColumnProvider} evaluates to NULL, otherwise
	 * TRUE.
	 * 
	 * <p>
	 * Obtain a {@link ColumnProvider} by using the column method of {@link DBRow}.
	 *
	 * @param possibleNullExpression
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNotNull(ColumnProvider possibleNullExpression) {
		return isNotNull(possibleNullExpression.getColumn().asExpression());
	}

	/**
	 * Returns TRUE if the given {@link ColumnProvider} evaluates to NULL, otherwise
	 * FALSE.
	 * 
	 * <p>
	 * Obtain a {@link ColumnProvider} by using the column method of {@link DBRow}.
	 *
	 * @param possibleNullExpression
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNull(ColumnProvider possibleNullExpression) {
		return isNull(possibleNullExpression.getColumn().asExpression());
	}

	/**
	 * Returns TRUE if the given {@link DBExpression} evaluates to NULL,
	 * otherwise FALSE.
	 *
	 * <p>
	 * DBExpression subclasses include
	 * {@link QueryableDatatype QueryableDatatypes} like {@link DBString} and
	 * {@link DBInteger} as well as
	 * {@link NumberExpression}, {@link StringExpression}, {@link DateExpression},
	 * and {@link LargeObjectExpression}.
	 *
	 * @param possibleNullExpression
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNull(DBExpression possibleNullExpression) {
		return new BooleanExpression(new DBUnaryBooleanArithmetic(possibleNullExpression) {

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " IS " + db.getDefinition().getNull();
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}

		});
	}

	/**
	 * Creates an Aggregate function that counts the rows returned by the query.
	 * 
	 * <p>
	 * For use within a {@link DBReport}
	 *
	 * @return a NumberExpression to add to a DBReport field.
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
	 * Creates an expression that will always return FALSE.
	 *
	 * @return an expression that will always evaluate to TRUE.
	 */
	public static BooleanExpression falseExpression() {
		return new BooleanExpression() {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().getFalseOperation();
			}

			@Override
			public boolean isAggregator() {
				return false;
			}
		};
	}

	/**
	 * Creates an expression that will always return TRUE.
	 *
	 * @return an expression that will always evaluate to TRUE.
	 */
	public static BooleanExpression trueExpression() {
		return new BooleanExpression() {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().getTrueOperation();
			}

			@Override
			public boolean isAggregator() {
				return false;
			}
		};
	}

	@Override
	public DBBoolean getQueryableDatatypeForExpressionValue() {
		return new DBBoolean();
	}

	@Override
	public boolean isAggregator() {
		if (onlyBool != null) {
			return onlyBool.isAggregator();
		} else {
			return false;
		}
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return onlyBool == null ? new HashSet<DBRow>() : onlyBool.getTablesInvolved();
	}

	/**
	 * Indicates if this expression is a relationship between 2, or more,
	 * tables.
	 *
	 * @return the relationship
	 */
	public boolean isRelationship() {
		return this.getTablesInvolved().size() > 1;
	}

	@Override
	public boolean getIncludesNull() {
		return includeNulls || onlyBool.getIncludesNull();
	}

	private static abstract class DBUnaryBooleanArithmetic implements BooleanResult {

		private DBExpression onlyBool;

		DBUnaryBooleanArithmetic(DBExpression bool) {
			this.onlyBool = bool.copy();
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			String op = this.getEquationOperator(db);
			String returnStr = onlyBool.toSQLString(db) + " " + op;
			return returnStr;
		}

		@Override
		public DBUnaryBooleanArithmetic copy() {
			DBUnaryBooleanArithmetic newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.onlyBool = onlyBool.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool == null ? new HashSet<DBRow>() : onlyBool.getTablesInvolved();
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator();
		}

		protected abstract String getEquationOperator(DBDatabase db);
	}

	private static abstract class DBNnaryBooleanArithmetic implements BooleanResult {

		private BooleanResult[] bools;
		private boolean includeNulls;

		DBNnaryBooleanArithmetic(BooleanResult... bools) {
			this.bools = bools;
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			String returnStr = "";
			String separator = "";
			String op = this.getEquationOperator(db);
			for (BooleanResult boo : bools) {
				returnStr += separator + boo.toSQLString(db);
				separator = op;
			}
			return returnStr;
		}

		@Override
		public DBNnaryBooleanArithmetic copy() {
			DBNnaryBooleanArithmetic newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.bools = new BooleanResult[bools.length];
			for (int i = 0; i < newInstance.bools.length; i++) {
				newInstance.bools[i] = bools[i].copy();
			}
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			for (BooleanResult boo : bools) {
				hashSet.addAll(boo.getTablesInvolved());
			}
			return hashSet;
		}

		protected abstract String getEquationOperator(DBDatabase db);

		@Override
		public boolean isAggregator() {
			boolean result = false;
			for (BooleanResult boex : bools) {
				result = result || boex.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return this.includeNulls;
		}

//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			this.includeNulls = nullsAreIncluded;
//		}
	}

	private static abstract class DBUnaryNumberFunction implements NumberResult {

		protected BooleanExpression onlyBool;

		DBUnaryNumberFunction() {
			this.onlyBool = null;
		}

		DBUnaryNumberFunction(BooleanExpression only) {
			this.onlyBool = only;
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
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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
			newInstance.onlyBool = (onlyBool == null ? null : onlyBool.copy());
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool.getTablesInvolved();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class DBUnaryBinaryFunction implements BooleanResult {

		protected BooleanExpression onlyBool;
		private boolean includeNulls;

		DBUnaryBinaryFunction() {
			this.onlyBool = null;
		}

		DBUnaryBinaryFunction(BooleanExpression only) {
			this.onlyBool = only;
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
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DBUnaryBinaryFunction copy() {
			DBUnaryBinaryFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.onlyBool = (onlyBool == null ? null : onlyBool.copy());
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool.getTablesInvolved();
		}

		@Override
		public boolean getIncludesNull() {
			return this.includeNulls;
		}

//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			this.includeNulls = nullsAreIncluded;
//		}
	}

	private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

		private BooleanExpression first;
		private BooleanExpression second;

		DBBinaryBooleanArithmetic(BooleanExpression first, BooleanExpression second) {
			this.first = first;
			this.second = second;
		}

		DBBinaryBooleanArithmetic(BooleanResult first, BooleanExpression second) {
			this.first = new BooleanExpression(first);
			this.second = second;
		}

		DBBinaryBooleanArithmetic(BooleanExpression first, BooleanResult second) {
			this.first = first;
			this.second = new BooleanExpression(second);
		}

		DBBinaryBooleanArithmetic(BooleanResult first, BooleanResult second) {
			this.first = new BooleanExpression(first);
			this.second = new BooleanExpression(second);
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			String sqlString = getFirst().toSQLString(db) + this.getEquationOperator(db) + getSecond().toSQLString(db);
			if (getFirst().getIncludesNull()) {
				final DBDefinition defn = db.getDefinition();
				sqlString = getSecond().toSQLString(db) + " IS " + defn.getNull() + defn.beginOrLine() + sqlString;
			}
			if (getSecond().getIncludesNull()) {
				final DBDefinition defn = db.getDefinition();
				sqlString = getFirst().toSQLString(db) + " IS " + defn.getNull() + defn.beginOrLine() + sqlString;
			}
			return "(" + sqlString + ")";
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
			newInstance.first = getFirst().copy();
			newInstance.second = getSecond().copy();
			return newInstance;
		}

		protected abstract String getEquationOperator(DBDatabase db);

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
			return false;
		}

//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			;
//		}
		/**
		 * @return the first
		 */
		protected BooleanExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		protected BooleanExpression getSecond() {
			return second;
		}
	}
}
