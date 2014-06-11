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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

public class BooleanExpression implements BooleanResult {

	private final BooleanResult onlyBool;
	private boolean includeNulls;

	protected BooleanExpression() {
		onlyBool = new DBBoolean();
	}

	public BooleanExpression(BooleanResult booleanResult) {
		onlyBool = booleanResult;
		if (booleanResult.getIncludesNull()) {
			this.includeNulls = true;
		}
	}

	public BooleanExpression(Boolean bool) {
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

	public BooleanExpression is(Boolean bool) {
		return is(new BooleanExpression(bool));
	}

	public BooleanExpression is(BooleanResult bool) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, bool) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	public BooleanExpression xor(BooleanResult bool) {
		return this.is(bool).not();
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
	 * @see #anyOf(nz.co.gregs.dbvolution.expressions.BooleanExpression...)
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
	 * @see #allOf(nz.co.gregs.dbvolution.expressions.BooleanExpression...)
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

	public static BooleanExpression isNotNull(DBExpression possibleNullExpression) {
		return new BooleanExpression(new DBUnaryBooleanArithmetic(possibleNullExpression) {

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " IS NOT " + db.getDefinition().getNull();
			}

			@Override
			public void setIncludesNull(boolean nullsAreIncluded) {
				;
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	public static BooleanExpression isNull(DBExpression possibleNullExpression) {
		return new BooleanExpression(new DBUnaryBooleanArithmetic(possibleNullExpression) {

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " IS " + db.getDefinition().getNull();
			}

			@Override
			public void setIncludesNull(boolean nullsAreIncluded) {
				;
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}

		});
	}

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

	@Override
	public void setIncludesNull(boolean nullsAreIncluded) {
		this.includeNulls = nullsAreIncluded;
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

		@Override
		public void setIncludesNull(boolean nullsAreIncluded) {
			this.includeNulls = nullsAreIncluded;
		}
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

		@Override
		public void setIncludesNull(boolean nullsAreIncluded) {
			this.includeNulls = nullsAreIncluded;
		}
	}

	private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

		private BooleanResult first;
		private BooleanResult second;

		DBBinaryBooleanArithmetic(BooleanResult first, BooleanResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			String sqlString = first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
			if (first.getIncludesNull()) {
				final DBDefinition defn = db.getDefinition();
				sqlString = second.toSQLString(db) + " IS " + defn.getNull() + defn.beginOrLine() + sqlString;
			}
			if (second.getIncludesNull()) {
				final DBDefinition defn = db.getDefinition();
				sqlString = first.toSQLString(db) + " IS " + defn.getNull() + defn.beginOrLine() + sqlString;
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
			return false;
		}

		@Override
		public void setIncludesNull(boolean nullsAreIncluded) {
			;
		}
	}
}
