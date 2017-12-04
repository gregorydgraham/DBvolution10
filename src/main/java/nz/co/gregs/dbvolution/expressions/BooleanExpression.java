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

import nz.co.gregs.dbvolution.results.RangeComparable;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.results.BooleanResult;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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
public class BooleanExpression implements BooleanResult, EqualComparable<BooleanResult>, ExpressionColumn<DBBoolean> {

	static BooleanExpression nullExpression() {
		return new BooleanExpression() {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().getNull();
			}

		};
	}


	private final BooleanResult onlyBool;
	private boolean includeNulls = false;

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
	 * @param booleanResult	booleanResult
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
	 * objects and add functionality to them. Use this constructor to wrap a known
	 * value for use in a BooleanExpression.
	 *
	 *
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
	 * This method provides the easy route to a *Expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param bool the boolean value to be tested
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static BooleanExpression value(Boolean bool) {
		return new BooleanExpression(bool);
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
	 * This method provides the easy route to a *Expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param bool the boolean value to be tested
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static BooleanExpression value(BooleanResult bool) {
		return new BooleanExpression(bool);
	}

	/**
	 * Compare this BooleanExpression and the given boolean using the equality
	 * operator, that is "=" or similar.
	 *
	 * @param bool the boolean value to be tested
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	public BooleanExpression is(Boolean bool) {
		return is(new BooleanExpression(bool));
//		return this.is(BooleanExpression.value(bool));
//		if (bool == null) {
//			return this.isNull();
//		} else if (bool) {
//			return this;
//		} else {
//			return this.not();
//		}
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the equality operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	public BooleanExpression is(BooleanExpression bool) {
		return is((BooleanResult) bool);
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the equality operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	@Override
	public BooleanExpression is(BooleanResult bool) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, bool) {

			@Override
			public String toSQLString(DBDatabase db) {
				DBDefinition defn = db.getDefinition();
				if (defn.supportsComparingBooleanResults()) {
					return super.toSQLString(db);
				} else {
					BooleanExpression first = this.getFirst();
					BooleanExpression second = this.getSecond();
					String firstSQL;
					String secondSQL;
					boolean firstIsStatement = first.isBooleanStatement();
					if (firstIsStatement) {
						firstSQL = defn.doBooleanStatementToBooleanComparisonValueTransform(first.toSQLString(db));
					} else {
						firstSQL = defn.doBooleanValueToBooleanComparisonValueTransform(first.toSQLString(db));
					}
					boolean secondIsStatement = second.isBooleanStatement();
					if (secondIsStatement) {
						secondSQL = defn.doBooleanStatementToBooleanComparisonValueTransform(second.toSQLString(db));
					} else {
						secondSQL = defn.doBooleanValueToBooleanComparisonValueTransform(second.toSQLString(db));
					}
					String returnString = "(" + firstSQL + ")" + getEquationOperator(db) + "(" + secondSQL + ")";
					return returnString;
				}
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the inequality operator, that is "&lt;&gt;" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	@Override
	public BooleanExpression isNot(BooleanResult bool) {

		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, bool) {

			@Override
			public String toSQLString(DBDatabase db) {
				DBDefinition defn = db.getDefinition();
				if (defn.supportsComparingBooleanResults()) {
					return super.toSQLString(db);
				} else {
					BooleanExpression first = this.getFirst();
					BooleanExpression second = this.getSecond();
					String firstSQL;
					String secondSQL;
					boolean firstIsStatement = first.isBooleanStatement();
					if (firstIsStatement) {
						firstSQL = defn.doBooleanStatementToBooleanComparisonValueTransform(first.toSQLString(db));
					} else {
						firstSQL = defn.doBooleanValueToBooleanComparisonValueTransform(first.toSQLString(db));
					}
					boolean secondIsStatement = second.isBooleanStatement();
					if (secondIsStatement) {
						secondSQL = defn.doBooleanStatementToBooleanComparisonValueTransform(second.toSQLString(db));
					} else {
						secondSQL = defn.doBooleanValueToBooleanComparisonValueTransform(second.toSQLString(db));
					}
					String returnString = "(" + firstSQL + ")" + getEquationOperator(db) + "(" + secondSQL + ")";
					return returnString;
				}
			}
//			return new BooleanExpression(new DBBinaryBooleanArithmetic(this, bool) {
//
//			@Override
//			public String toSQLString(DBDatabase db) {
//				DBDefinition defn = db.getDefinition();
//				if (defn.supportsComparingBooleanResults()) {
//					return super.toSQLString(db);
//				} else {
//					BooleanExpression first = this.getFirst();
//					BooleanExpression second = this.getSecond();
//					String firstSQL;
//					String secondSQL;
//					boolean firstIsStatement = first.isBooleanStatement();
//					if (firstIsStatement) {
//						firstSQL = defn.doBooleanStatementToBooleanComparisonValueTransform(first.toSQLString(db));
//					} else {
//						firstSQL = defn.doBooleanValueToBooleanComparisonValueTransform(first.toSQLString(db));
//					}
//					boolean secondIsStatement = second.isBooleanStatement();
//					if (secondIsStatement) {
//						secondSQL = defn.doBooleanStatementToBooleanComparisonValueTransform(second.toSQLString(db));
//					} else {
//						secondSQL = defn.doBooleanValueToBooleanComparisonValueTransform(second.toSQLString(db));
//					}
//					String returnString = "(" + firstSQL + ")" + getEquationOperator(db) + "(" + secondSQL + ")";
//					return returnString;
//				}
//			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " <> ";
			}
		});
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the inequality operator, that is "&lt;&gt;" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	public BooleanExpression isNot(Boolean bool) {
		return isNot(new BooleanExpression(bool));
//		return this.isNot(BooleanExpression.value(bool));
//		if (bool == null) {
//			return this.isNotNull();
//		} else if (bool) {
//			return this.not();
//		} else {
//			return this;
//		}
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the Exclusive OR operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
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
	 * are true.
	 *
	 * @param booleanExpressions the boolean expressions to be tested
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
	 * Collects the expressions together and requires that at least one of them to
	 * be false.
	 *
	 * <p>
	 * Please note that this expression does not exclude the cases where all tests
	 * fail. To exclude the ALL and NONE cases, use
	 * {@link #someButNotAllOf(nz.co.gregs.dbvolution.expressions.BooleanExpression...) the SOME method}.
	 *
	 * <p>
	 * Creates a BooleanExpression of several Boolean Expressions by connecting
	 * them using NOT and OR repeatedly.
	 *
	 * <p>
	 * This expression returns true if and only if some of the component
	 * expressions are false.
	 *
	 * @param booleanExpressions the boolean expressions to be tested
	 * @return a boolean expression that returns true IFF some of the
	 * booleanExpressions are false.
	 * @see #anyOf(BooleanExpression...)
	 * @see #allOf(BooleanExpression...)
	 */
	public static BooleanExpression notAllOf(final BooleanExpression... booleanExpressions) {
		List<BooleanExpression> notBools = new ArrayList<>();
		for (BooleanExpression booleanExpression : booleanExpressions) {
			notBools.add(booleanExpression.not());
		}
		return anyOf(notBools.toArray(booleanExpressions));
	}

	/**
	 * Collects the expressions together and requires that at least one of them to
	 * be false and at least one to be false.
	 *
	 * <p>
	 * This expression specifically excludes the cases where ALL and NONE of the
	 * tests pass.
	 *
	 * <p>
	 * This expression facilties finding partial matches that may need to be
	 * handled separately.
	 *
	 * <p>
	 * This expression returns true if and only if some of the component
	 * expressions are false and some are true.
	 *
	 * @param booleanExpressions the boolean expressions to be tested
	 * @return a boolean expression that returns true IFF some of the
	 * booleanExpressions are false AND some of the booleanExpressions are true
	 * @see #anyOf(BooleanExpression...)
	 * @see #allOf(BooleanExpression...)
	 */
	public static BooleanExpression someButNotAllOf(final BooleanExpression... booleanExpressions) {
		return allOf(
				BooleanExpression.allOf(booleanExpressions).not(),
				BooleanExpression.noneOf(booleanExpressions).not()
		);
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
	 * @param booleanExpressions the boolean expressions to be tested
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
	 * @param booleanExpressions the boolean expressions to be tested
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
	 * <p>
	 * TRUE values will become 1 and FALSE values will become 0.
	 *
	 * @return a 0 or 1 depending on the expression
	 */
	public NumberExpression convertToInteger() {
		return new NumberExpression() {
			BooleanExpression innerBool = new BooleanExpression(onlyBool);

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doBooleanToIntegerTransform(this.innerBool.toSQLString(db));
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
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
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
	 * @param possibleNullExpression the expression to be tested
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
	 * Returns FALSE if the given {@link ColumnProvider} evaluates to NULL,
	 * otherwise TRUE.
	 *
	 * <p>
	 * Obtain a {@link ColumnProvider} by using the column method of
	 * {@link DBRow}.
	 *
	 * @param possibleNullExpression the expression to be tested
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNotNull(ColumnProvider possibleNullExpression) {
		return isNotNull(possibleNullExpression.getColumn().asExpression());
	}

	/**
	 * Returns TRUE if the given {@link ColumnProvider} evaluates to NULL,
	 * otherwise FALSE.
	 *
	 * <p>
	 * Obtain a {@link ColumnProvider} by using the column method of
	 * {@link DBRow}.
	 *
	 * @param possibleNullExpression the expression to be tested
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNull(ColumnProvider possibleNullExpression) {
		return isNull(possibleNullExpression.getColumn().asExpression());
	}

	/**
	 * Returns TRUE if the given {@link DBExpression} evaluates to NULL, otherwise
	 * FALSE.
	 *
	 * <p>
	 * DBExpression subclasses include
	 * {@link QueryableDatatype QueryableDatatypes} like {@link DBString} and
	 * {@link DBInteger} as well as
	 * {@link NumberExpression}, {@link StringExpression}, {@link DateExpression},
	 * and {@link LargeObjectExpression}.
	 *
	 * @param possibleNullExpression the expression to be tested
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
	 * Returns TRUE if this expression evaluates to NULL, otherwise FALSE.
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public StringExpression ifThenElse(String thenExpr, String elseExpr) {
		return this.ifThenElse(new StringExpression(thenExpr), new StringExpression(elseExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public StringExpression ifThenElse(StringExpression thenExpr, StringExpression elseExpr) {
		return new StringExpression(new DBBooleanStringStringFunction(this, thenExpr, elseExpr) {

			@Override
			public boolean getIncludesNull() {
				return false;
			}

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "";
			}

		});
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public NumberExpression ifThenElse(Number thenExpr, Number elseExpr) {
		return this.ifThenElse(new NumberExpression(thenExpr), new NumberExpression(elseExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public NumberExpression ifThenElse(NumberResult thenExpr, NumberResult elseExpr) {
		return new NumberExpression(new DBBooleanNumberNumberFunction(this, thenExpr, elseExpr) {

			@Override
			public boolean getIncludesNull() {
				return false;
			}

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "";
			}
		});
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public DateExpression ifThenElse(Date thenExpr, Date elseExpr) {
		return this.ifThenElse(new DateExpression(thenExpr), new DateExpression(elseExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public DateExpression ifThenElse(DateExpression thenExpr, DateExpression elseExpr) {
		return new DateExpression(new DBBinaryDateDateFunction(this, thenExpr, elseExpr) {

			@Override
			public boolean getIncludesNull() {
				return false;
			}

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "";
			}

		});
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public Polygon2DExpression ifThenElse(Polygon thenExpr, Polygon elseExpr) {
		return this.ifThenElse(new Polygon2DExpression(thenExpr), new Polygon2DExpression(elseExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, otherwise the
	 * second is returned.
	 *
	 * @param thenExpr expression to use when this expression is TRUE
	 * @param elseExpr expression to use when this expression is FALSE
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public Polygon2DExpression ifThenElse(Polygon2DExpression thenExpr, Polygon2DExpression elseExpr) {
		return new Polygon2DExpression(new DBBinaryGeometryGeometryFunction(this, thenExpr, elseExpr) {

			@Override
			public boolean getIncludesNull() {
				return false;
			}

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
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
		return new NumberExpression(new DBBooleanAggregatorFunctionReturningNumber(this) {

			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getCountFunctionName();
			}
		});
	}

	/**
	 * Creates an expression that will always return FALSE.
	 *
	 * @return an expression that will always evaluate to FALSE.
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

	/**
	 * Specifies that the expression will return TRUE if this OR the specified
	 * expression are TRUE.
	 *
	 * <p>
	 * This is a convenience method that wraps this and anotherBooleanExpr in {@link BooleanExpression#anyOf(nz.co.gregs.dbvolution.expressions.BooleanExpression...)
	 * }.
	 *
	 * @param anotherBooleanExpr if this expression does not evaluate to TRUE,
	 * return the value of anotherBooleanExpression.
	 * @return a expression that will evaluate to TRUE if either of the
	 * expressions are TRUE.
	 */
	public BooleanExpression or(BooleanExpression anotherBooleanExpr) {
		return BooleanExpression.anyOf(
				this,
				anotherBooleanExpr);
	}

	/**
	 * Specifies that the expression will return TRUE only if this AND the
	 * specified expression are TRUE.
	 *
	 * <p>
	 * This is a convenience method that wraps this and anotherBooleanExpr in {@link BooleanExpression#allOf(nz.co.gregs.dbvolution.expressions.BooleanExpression...)
	 * }.
	 *
	 * @param anotherBooleanExpr only return TRUE if both this expression and
	 * anotherBooleanExpr evaluate to TRUE.
	 * @return a expression that will evaluate to TRUE only if both of the
	 * expressions are TRUE.
	 */
	public BooleanExpression and(BooleanExpression anotherBooleanExpr) {
		return BooleanExpression.allOf(
				this,
				anotherBooleanExpr);
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * below both.
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <A> a value that can be compared to Z, probably StringResult,
	 * NumberResult, or DateResult
	 * @param <Z> an expression or column that implements RangeComparable,
	 * probably StringExpression, NumberExpression, DateExpression or a column
	 * type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param whenEqualsFallbackComparison the comparison used when the ColumnA
	 * and ValueA are equal.
	 * @return a BooleanExpression
	 */
	public static <A extends DBExpression, Z extends RangeComparable<? super A>>
			BooleanExpression seekLessThan(Z columnA, A valueA, BooleanExpression whenEqualsFallbackComparison) {
		return columnA.isLessThan(valueA).or(columnA.is(valueA).and(whenEqualsFallbackComparison));
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * above both.
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <A> a value that can be compared to Z, probably StringResult,
	 * NumberResult, or DateResult
	 * @param <Z> an expression or column that implements RangeComparable,
	 * probably StringExpression, NumberExpression, DateExpression or a column
	 * type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param whenEqualsFallbackComparison the comparison used when the ColumnA
	 * and ValueA are equal.
	 * @return a BooleanExpression
	 */
	public static <A extends DBExpression, Z extends RangeComparable<? super A>>
			BooleanExpression seekGreaterThan(Z columnA, A valueA, BooleanExpression whenEqualsFallbackComparison) {
		return columnA.isGreaterThan(valueA).or(columnA.is(valueA).and(whenEqualsFallbackComparison));
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * below both.
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <RangeComparableY> a value that can be compared to Z, probably
	 * StringResult, NumberResult, or DateResult
	 * @param <RangeComparableZ> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison and ValueA are
	 * equal.
	 * @return a BooleanExpression
	 */
	public static
			<RangeComparableZ extends RangeComparable<? super RangeComparableZ>, RangeComparableY extends RangeComparable<? super RangeComparableY>>
			BooleanExpression
			seekLessThan(RangeComparableZ columnA, RangeComparableZ valueA, RangeComparableY columnB, RangeComparableY valueB) {
		return BooleanExpression.anyOf(
				columnA.isLessThan(valueA),
				BooleanExpression.allOf(columnA.is(valueA), columnB.isLessThanOrEqual(valueB)));
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * above both.
	 *
	 * <p>
	 * This version provides a second level of sorting. If you only need to seek
	 * on one column/value use {@link #seekGreaterThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.expressions.DBExpression, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <RangeComparableZ> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param <RangeComparableY> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @return a BooleanExpression
	 */
	public static
			<RangeComparableZ extends RangeComparable<? super RangeComparableZ>, RangeComparableY extends RangeComparable<? super RangeComparableY>>
			BooleanExpression
			seekGreaterThan(RangeComparableZ columnA, RangeComparableZ valueA, RangeComparableY columnB, RangeComparableY valueB) {
		return BooleanExpression.anyOf(
				columnA.isGreaterThan(valueA),
				BooleanExpression.allOf(columnA.is(valueA), columnB.isGreaterThanOrEqual(valueB)));
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * below both.
	 *
	 * <p>
	 * This version provides three levels of sorting. If you only need to seek on
	 * one column/value use {@link #seekGreaterThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.expressions.DBExpression, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <RangeComparableZ> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param <RangeComparableY> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param <RangeComparableX> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * @return a BooleanExpression
	 */
	public static
			<RangeComparableZ extends RangeComparable<? super RangeComparableZ>, RangeComparableY extends RangeComparable<? super RangeComparableY>, RangeComparableX extends RangeComparable<? super RangeComparableX>>
			BooleanExpression
			seekLessThan(RangeComparableZ columnA, RangeComparableZ valueA, RangeComparableY columnB, RangeComparableY valueB, RangeComparableX columnC, RangeComparableX valueC) {
		return BooleanExpression.anyOf(
				columnA.isLessThan(valueA),
				BooleanExpression.allOf(columnA.is(valueA), BooleanExpression.seekLessThan(columnB, valueB, columnC, valueC)));
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * below both.
	 *
	 * <p>
	 * This version provides four levels of sorting. If you only need to seek on
	 * one column/value use {@link #seekLessThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.expressions.DBExpression, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <RangeComparableZ> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param <RangeComparableY> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param <RangeComparableX> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * @param <RangeComparableW> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnD the left side of the internal comparison
	 * @param valueD the right side of the internal comparison
	 * @return a BooleanExpression
	 */
	public static
			<RangeComparableZ extends RangeComparable<? super RangeComparableZ>, RangeComparableY extends RangeComparable<? super RangeComparableY>, RangeComparableX extends RangeComparable<? super RangeComparableX>, RangeComparableW extends RangeComparable<? super RangeComparableW>>
			BooleanExpression
			seekLessThan(RangeComparableZ columnA, RangeComparableZ valueA, RangeComparableY columnB, RangeComparableY valueB, RangeComparableX columnC, RangeComparableX valueC, RangeComparableW columnD, RangeComparableW valueD) {
		return BooleanExpression.anyOf(
				columnA.isLessThan(valueA),
				BooleanExpression.allOf(columnA.is(valueA), BooleanExpression.seekLessThan(columnB, valueB, columnC, valueC, columnD, valueD)));
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * below both.
	 *
	 * <p>
	 * This version provides 3 levels of sorting. If you only need to seek on one
	 * column/value use {@link #seekLessThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.expressions.DBExpression, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <RangeComparableZ> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param <RangeComparableY> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param <RangeComparableX> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * @return a BooleanExpression
	 */
	public static
			<RangeComparableZ extends RangeComparable<? super RangeComparableZ>, RangeComparableY extends RangeComparable<? super RangeComparableY>, RangeComparableX extends RangeComparable<? super RangeComparableX>>
			BooleanExpression
			seekGreaterThan(RangeComparableZ columnA, RangeComparableZ valueA, RangeComparableY columnB, RangeComparableY valueB, RangeComparableX columnC, RangeComparableX valueC) {
		return BooleanExpression.anyOf(
				columnA.isGreaterThan(valueA),
				BooleanExpression.allOf(columnA.is(valueA), BooleanExpression.seekGreaterThan(columnB, valueB, columnC, valueC)));
	}

	/**
	 * Implements the little-known (and implemented) SQL Row Value syntax.
	 *
	 * <p>
	 * in PostgreSQL you can do (colA, colB) &lt; (valA, valB). In other databases
	 * you need to write: ((colA &lt; valA) OR (colA = valA AND colB &lt; valB)).
	 * Similarly for &gt;.
	 *
	 * <p>
	 * Essentially seek looks at both parameters and returns the rows that sort
	 * above both.
	 *
	 * <p>
	 * This version provides four levels of sorting. If you only need to seek on
	 * one column/value use {@link #seekLessThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.expressions.DBExpression, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <RangeComparableZ> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param <RangeComparableY> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param <RangeComparableX> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * @param <RangeComparableW> an expression or column that implements
	 * RangeComparable, probably StringExpression, NumberExpression,
	 * DateExpression or a column type of the same.
	 * @param columnD the left side of the internal comparison
	 * @param valueD the right side of the internal comparison
	 * @return a BooleanExpression
	 */
	public static
			<RangeComparableZ extends RangeComparable<? super RangeComparableZ>, RangeComparableY extends RangeComparable<? super RangeComparableY>, RangeComparableX extends RangeComparable<? super RangeComparableX>, RangeComparableW extends RangeComparable<? super RangeComparableW>>
			BooleanExpression
			seekGreaterThan(RangeComparableZ columnA, RangeComparableZ valueA, RangeComparableY columnB, RangeComparableY valueB, RangeComparableX columnC, RangeComparableX valueC, RangeComparableW columnD, RangeComparableW valueD) {
		return BooleanExpression.anyOf(
				columnA.isGreaterThan(valueA),
				BooleanExpression.allOf(columnA.is(valueA), BooleanExpression.seekGreaterThan(columnB, valueB, columnC, valueC, columnD, valueD)));
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
	public boolean isPurelyFunctional() {
		if (onlyBool == null) {
			return true;
		} else {
			return onlyBool.isPurelyFunctional();
		}
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return onlyBool == null ? new HashSet<DBRow>() : onlyBool.getTablesInvolved();
	}

	/**
	 * Indicates if this expression is a relationship between 2, or more, tables.
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
	public StringExpression stringResult() {
		return this.ifThenElse("TRUE", "FALSE");
	}

	@Override
	public DBBoolean asExpressionColumn() {
		return new DBBoolean(this);
	}

	@Override
	public boolean isBooleanStatement() {
		BooleanResult onlyBool1 = this.onlyBool;
		return onlyBool1.isBooleanStatement();
	}

	private static abstract class DBUnaryBooleanArithmetic extends BooleanExpression {

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

		@Override
		public boolean isPurelyFunctional() {
			if (onlyBool == null) {
				return true;
			} else {
				return onlyBool.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBNnaryBooleanArithmetic extends BooleanExpression {

		private BooleanResult[] bools;
//		private boolean includeNulls;

		DBNnaryBooleanArithmetic(BooleanResult... bools) {
			this.bools = bools;
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			StringBuilder returnStr = new StringBuilder();
			String separator = "";
			String op = this.getEquationOperator(db);
			for (BooleanResult boo : bools) {
				returnStr.append(separator).append(boo.toSQLString(db));
				separator = op;
			}
			return "(" + returnStr + ")";
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
			HashSet<DBRow> hashSet = new HashSet<>();
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
		public boolean isPurelyFunctional() {
			if (bools.length == 0) {
				return true;
			} else {
				boolean result = true;
				for (BooleanResult bool : bools) {
					result = result && bool.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class DBBooleanAggregatorFunctionReturningNumber extends NumberExpression {

		protected BooleanExpression onlyBool = null;

		DBBooleanAggregatorFunctionReturningNumber() {
			this.onlyBool = null;
		}

		DBBooleanAggregatorFunctionReturningNumber(BooleanExpression only) {
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
			String valueToCount = db.getDefinition().transformToStorableType(onlyBool).toSQLString(db);
			return this.beforeValue(db) + (onlyBool == null ? "" : valueToCount) + this.afterValue(db);
		}

		@Override
		public DBBooleanAggregatorFunctionReturningNumber copy() {
			DBBooleanAggregatorFunctionReturningNumber newInstance;
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
			return true;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool.getTablesInvolved();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (onlyBool == null) {
				return true;
			} else {
				return onlyBool.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBUnaryBinaryFunction extends BooleanExpression {

		protected BooleanExpression onlyBool;
//		private boolean includeNulls;

		DBUnaryBinaryFunction() {
			this.onlyBool = null;
		}

		DBUnaryBinaryFunction(BooleanExpression only) {
			this.onlyBool = only;
		}

//		@Override
//		public DBNumber getQueryableDatatypeForExpressionValue() {
//			return new DBNumber();
//		}
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
		public boolean isPurelyFunctional() {
			if (onlyBool == null) {
				return true;
			} else {
				return onlyBool.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBBinaryBooleanArithmetic extends BooleanExpression {

		private BooleanExpression first;
		private BooleanExpression second;

		DBBinaryBooleanArithmetic(BooleanExpression first, BooleanExpression second) {
			this.first = first;
			this.second = second;
		}

		DBBinaryBooleanArithmetic(BooleanExpression first, BooleanResult second) {
			this(first, new BooleanExpression(second));
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
//			} else if (first == null && second != null) {
//				return second.isPurelyFunctional();
//			} else if (first != null && second == null) {
//				return first.isPurelyFunctional();
			} else {
				return (first==null?true:first.isPurelyFunctional()) 
						&& (second==null?true:second.isPurelyFunctional());
			}
		}
	}

	private static abstract class DBBooleanStringStringFunction extends StringExpression {

		protected BooleanExpression onlyBool = null;
		protected StringResult first = null;
		protected StringResult second = null;
//		private boolean includeNulls;

		DBBooleanStringStringFunction() {
		}

		DBBooleanStringStringFunction(BooleanExpression only, StringResult first, StringResult second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
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
		@SuppressWarnings("unchecked")
		public DBBooleanStringStringFunction copy() {
			DBBooleanStringStringFunction newInstance;
			try {
				newInstance = this.getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.onlyBool = (onlyBool == null ? null : onlyBool.copy());
			newInstance.first = (first == null ? null : first.copy());
			newInstance.second = (second == null ? null : second.copy());
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator() || first.isAggregator() || second.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool.getTablesInvolved();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (onlyBool == null) {
				return true;
			} else {
				return onlyBool.isPurelyFunctional() && first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBBooleanNumberNumberFunction extends NumberExpression {

		protected BooleanExpression onlyBool = null;
		protected NumberResult first = null;
		protected NumberResult second = null;
//		private boolean includeNulls;

		DBBooleanNumberNumberFunction() {
		}

		DBBooleanNumberNumberFunction(BooleanExpression only, NumberResult first, NumberResult second) {
			this.onlyBool = (only==null?BooleanExpression.nullExpression():only);
			this.first = (first==null?NumberExpression.nullExpression():first);
			this.second = (second==null?NumberExpression.nullExpression():second);
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
		public DBBooleanNumberNumberFunction copy() {
			DBBooleanNumberNumberFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.onlyBool = (onlyBool == null ? null : onlyBool.copy());
			newInstance.first = (first == null ? null : first.copy());
			newInstance.second = (second == null ? null : second.copy());
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator() || first.isAggregator() || second.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool.getTablesInvolved();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (onlyBool == null) {
				return true;
			} else {
				return onlyBool.isPurelyFunctional() && first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBBinaryDateDateFunction extends DateExpression {

		protected BooleanExpression onlyBool = null;
		protected DateResult first = null;
		protected DateResult second = null;
//		private boolean includeNulls;

		DBBinaryDateDateFunction() {
		}

		DBBinaryDateDateFunction(BooleanExpression only, DateExpression first, DateExpression second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
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
		public DBBinaryDateDateFunction copy() {
			DBBinaryDateDateFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.onlyBool = (onlyBool == null ? null : onlyBool.copy());
			newInstance.first = (first == null ? null : first.copy());
			newInstance.second = (second == null ? null : second.copy());
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator() || first.isAggregator() || second.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool.getTablesInvolved();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (onlyBool == null) {
				return true;
			} else {
				return onlyBool.isPurelyFunctional() && first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBBinaryGeometryGeometryFunction extends Polygon2DExpression {

		protected BooleanExpression onlyBool = null;
		protected Polygon2DExpression first = null;
		protected Polygon2DExpression second = null;
//		private boolean includeNulls;

		DBBinaryGeometryGeometryFunction() {
		}

		DBBinaryGeometryGeometryFunction(BooleanExpression only, Polygon2DExpression first, Polygon2DExpression second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
		}

		@Override
		public DBBinaryGeometryGeometryFunction copy() {
			DBBinaryGeometryGeometryFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.onlyBool = (onlyBool == null ? null : onlyBool.copy());
			newInstance.first = (first == null ? null : first.copy());
			newInstance.second = (second == null ? null : second.copy());
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator() || first.isAggregator() || second.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool.getTablesInvolved();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (onlyBool == null) {
				return true;
			} else {
				return onlyBool.isPurelyFunctional() && first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}
}
