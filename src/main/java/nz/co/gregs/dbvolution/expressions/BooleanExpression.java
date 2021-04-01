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

import nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpression;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.results.BooleanResult;
import com.vividsolutions.jts.geom.Polygon;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.RangeComparable;
import nz.co.gregs.dbvolution.results.RangeResult;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionRequiresOrderBy;

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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class BooleanExpression extends EqualExpression<Boolean, BooleanResult, DBBoolean> implements BooleanResult {

	private final static long serialVersionUID = 1l;

	/**
	 * Default Constructor for creating new BooleanExpressions.
	 *
	 * <p>
	 * The BooleanExpression created has no value or operation and is generally
	 * useless for the end user. however it is required for sub-classing.
	 *
	 */
	protected BooleanExpression() {
		super();
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
		super(booleanResult);
	}

	public BooleanExpression(AnyResult<?> booleanResult) {
		super(booleanResult);
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
	 * @param bool the boolean to be to be used in the expression
	 */
	public BooleanExpression(Boolean bool) {
		super(new DBBoolean(bool));
	}

	@Override
	public BooleanExpression copy() {
		return isNullSafetyTerminator() ? nullBoolean() : new BooleanExpression((AnyResult<?>) this.getInnerResult().copy());
	}

	/**
	 * Returns a value of the required type that will evaluate to NULL.
	 *
	 * @return a NULL for use in boolean statements.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public BooleanExpression nullExpression() {
		return new NullExpression();
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
	public BooleanExpression modeSimple() {
		BooleanExpression modeExpr = new BooleanExpression(
				new ModeSimpleExpression<>(this));

		return modeExpr;
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	@Override
	public BooleanExpression expression(Boolean bool) {
		return value(bool);
	}

	@Override
	public BooleanExpression expression(DBBoolean bool) {
		return value(bool);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	@Override
	public BooleanExpression expression(BooleanResult bool) {
		return new BooleanExpression(bool);
	}

	/**
	 * Compare this BooleanExpression and the given boolean using the equality
	 * operator, that is "=" or similar.
	 *
	 * @param bool the boolean value to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	@Override
	public BooleanExpression is(Boolean bool) {

		return bool == null ? isNull() : is(new BooleanExpression(bool));
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the equality operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	public BooleanExpression is(BooleanExpression bool) {
		return bool.getIncludesNull() ? isNull() : is((BooleanResult) bool);
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the equality operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	@Override
	public BooleanExpression is(BooleanResult bool) {
		return new BooleanExpression(new IsExpression(this, bool));
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the inequality operator, that is "&lt;&gt;" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	@Override
	public BooleanExpression isNot(BooleanResult bool) {

		return new BooleanExpression(new IsNotExpression(this, bool));
	}

	/**
	 * Creates an expression that will count all rows with non-null values in the
	 * column supplied.
	 *
	 * <p>
	 * Count is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * @return a number expression.
	 */
	@Override
	public AnyExpression.CountExpression countNotNull() {
		return this.integerValue().count();
	}

	protected String getComparableBooleanSQL(DBDefinition db) {
		return getComparableBooleanSQL(db, this.toSQLString(db));
	}

	protected String getComparableBooleanSQL(DBDefinition db, String preparedSQL) {
		if (db.supportsComparingBooleanResults()) {
			return preparedSQL;
		} else {
			String firstSQL;
			boolean firstIsStatement = this.isBooleanStatement();
			if (firstIsStatement) {
				firstSQL = db.doBooleanStatementToBooleanComparisonValueTransform(preparedSQL);
			} else {
				firstSQL = db.doBooleanValueToBooleanComparisonValueTransform(preparedSQL);
			}
			return "(" + firstSQL + ")";
		}
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the inequality operator, that is "&lt;&gt;" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that compares the previous BooleanExpression to
	 * the Boolean supplied.
	 */
	@Override
	public BooleanExpression isNot(Boolean bool) {
		return isNot(new BooleanExpression(bool));
	}

	/**
	 * Compare this BooleanExpression and the given {@link BooleanResult} using
	 * the Exclusive OR operator, that is "=" or similar.
	 *
	 * <p>
	 * BooleanResult includes {@link BooleanExpression} and {@link DBBoolean}.
	 *
	 * @param bool the boolean value to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of an XOR operation.
	 */
	public BooleanExpression xor(BooleanResult bool) {
		return new BooleanExpression(new XorExpression(this, bool));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression that returns true IFF all the
	 * booleanExpressions are true.
	 * @see #anyOf(BooleanExpression...)
	 */
	public static BooleanExpression allOf(final BooleanExpression... booleanExpressions) {
		return new BooleanExpression(new AllOfExpression(booleanExpressions));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * This expression facilitates finding partial matches that may need to be
	 * handled separately.
	 *
	 * <p>
	 * This expression returns true if and only if some of the component
	 * expressions are false and some are true.
	 *
	 * @param booleanExpressions the boolean expressions to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression that returns true if any of the
	 * booleanExpressions is true.
	 * @see #allOf(BooleanExpression...)
	 */
	public static BooleanExpression anyOf(final BooleanExpression... booleanExpressions) {
		return new BooleanExpression(new AnyOfExpression(booleanExpressions));
	}

	/**
	 * Returns true only if all of the conditions are FALSE.
	 *
	 * @param booleanExpressions the boolean expressions to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Boolean expression representing the negation of the current
	 * expression.
	 */
	public BooleanExpression negate() {
		return new BooleanExpression(new NegateExpression(this));
	}

	/**
	 * Converts boolean values to the database integer representation.
	 *
	 * <p>
	 * TRUE values will become 1 and FALSE values will become 0.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a 0 or 1 depending on the expression
	 */
	public IntegerExpression integerValue() {
		return new IntegerValueFunction(this);
	}

	/**
	 * Converts boolean values to the database number representation.
	 *
	 * <p>
	 * TRUE values will become 1.0 and FALSE values will become 0.0.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a 0 or 1 depending on the expression
	 */
	public NumberExpression numberValue() {
		return new NumberExpression(new BooleanToNumberExpression(this));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Boolean expression representing the negation of the current
	 * expression.
	 */
	public BooleanExpression not() {
		return this.negate();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Boolean expression representing the negation of the current
	 * expression.
	 */
	public BooleanExpression isFalse() {
		return this.negate();
	}

	public BooleanExpression isTrue() {
		return this;
	}

	/**
	 * Returns FALSE if the given {@link DBExpression} evaluates to NULL,
	 * otherwise TRUE.
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
		if (possibleNullExpression instanceof StringExpression) {
			return isNotNull((StringResult) possibleNullExpression);
		} else {
			return new BooleanExpression(new IsNotNullExpression(possibleNullExpression));
		}
	}

	/**
	 * Returns FALSE if the given {@link StringExpression} evaluates to NULL,
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
	public static BooleanExpression isNotNull(StringResult possibleNullExpression) {
		return new BooleanExpression(new IsNotNullStringExpression(possibleNullExpression));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNull(DBExpression possibleNullExpression) {
		if (possibleNullExpression instanceof StringExpression) {
			return isNull((StringExpression) possibleNullExpression);
		} else {
			return new BooleanExpression(new IsNullExpression(possibleNullExpression));
		}
	}

	/**
	 * Returns TRUE if the given {@link StringExpression} evaluates to NULL,
	 * otherwise FALSE.
	 *
	 * <p>
	 * DBExpression subclasses include
	 * {@link QueryableDatatype QueryableDatatypes} like {@link DBString} and
	 * {@link DBInteger} as well as
	 * {@link NumberExpression}, {@link StringExpression}, {@link DateExpression},
	 * and {@link LargeObjectExpression}.
	 *
	 * @param possibleNullExpression the expression to be tested
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public static BooleanExpression isNull(StringExpression possibleNullExpression) {
		return new BooleanExpression(new IsNullStringExpression(possibleNullExpression));
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
	public BooleanExpression ifThenElse(Boolean thenExpr, Boolean elseExpr) {
		return this.ifThenElse(new BooleanExpression(thenExpr), new BooleanExpression(elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public BooleanExpression ifThenElse(BooleanExpression thenExpr, BooleanExpression elseExpr) {
		return new BooleanExpression(new BooleanIfThenElseExpression(this, thenExpr, elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public StringExpression ifThenElse(String thenExpr, String elseExpr) {
		return this.ifThenElse(value(thenExpr), value(elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public StringExpression ifThenElse(StringExpression thenExpr, StringExpression elseExpr) {
		return new StringExpression(new StringIfThenElseExpression(this, thenExpr, elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public IntegerExpression ifThenElse(Long thenExpr, Long elseExpr) {
		return this.ifThenElse(new IntegerExpression(thenExpr), new IntegerExpression(elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public IntegerExpression ifThenElse(Integer thenExpr, Integer elseExpr) {
		return this.ifThenElse(new IntegerExpression(thenExpr), new IntegerExpression(elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public NumberExpression ifThenElse(NumberResult thenExpr, NumberResult elseExpr) {
		return new NumberExpression(new NumberIfThenElseExpression(this, thenExpr, elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public IntegerExpression ifThenElse(IntegerResult thenExpr, IntegerResult elseExpr) {
		return new IntegerExpression(new IntegerIfThenElseExpression(this, thenExpr, elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public DateExpression ifThenElse(DateExpression thenExpr, DateExpression elseExpr) {
		return new DateExpression(new DateIfThenElseExpression(this, thenExpr, elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateExpression ifThenElse(LocalDate thenExpr, LocalDate elseExpr) {
		return this.ifThenElse(value(thenExpr), new LocalDateExpression(elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateExpression ifThenElse(LocalDateExpression thenExpr, LocalDateExpression elseExpr) {
		return new LocalDateExpression(new LocalDateIfThenElseExpression(this, thenExpr, elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateTimeExpression ifThenElse(LocalDateTime thenExpr, LocalDateTime elseExpr) {
		return this.ifThenElse(value(thenExpr), new LocalDateTimeExpression(elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateTimeExpression ifThenElse(LocalDateTimeExpression thenExpr, LocalDateTimeExpression elseExpr) {
		return new LocalDateTimeExpression(new LocalDateTimeIfThenElseExpression(this, thenExpr, elseExpr));
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
	public InstantExpression ifThenElse(Instant thenExpr, Instant elseExpr) {
		final InstantExpression value = value(thenExpr);
		final InstantExpression otherValue = new InstantExpression(elseExpr);
		return this.ifThenElse(value, otherValue);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public InstantExpression ifThenElse(InstantExpression thenExpr, InstantExpression elseExpr) {
		return new InstantExpression(new InstantIfThenElseExpression(this, thenExpr, elseExpr));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public Polygon2DExpression ifThenElse(Polygon2DExpression thenExpr, Polygon2DExpression elseExpr) {
		return new Polygon2DExpression(new Polygon2DIfThenElseExpression(this, thenExpr, elseExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr expression to use when this expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public BooleanExpression ifTrueFalseNull(Boolean trueExpr, Boolean falseExpr, Boolean nullExpr) {
		return this.ifTrueFalseNull(
				new BooleanExpression(trueExpr),
				new BooleanExpression(falseExpr),
				BooleanExpression.value(nullExpr)
		);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public BooleanExpression ifTrueFalseNull(BooleanExpression trueExpr, BooleanExpression falseExpr, BooleanExpression nullExpr) {
		return CaseExpression
				.when(this, trueExpr)
				.when(this.isFalse(), falseExpr)
				.defaultValue(nullExpr);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueValue expression to use when this expression is TRUE
	 * @param falseValue expression to use when this expression is FALSE
	 * @param nullValue the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public StringExpression ifTrueFalseNull(String trueValue, String falseValue, String nullValue) {
		return this.ifTrueFalseNull(
				value(trueValue),
				value(falseValue),
				StringExpression.value(nullValue)
		);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public StringExpression ifTrueFalseNull(StringExpression trueExpr, StringExpression falseExpr, StringExpression nullExpr) {
		return CaseExpression
				.when(this, trueExpr)
				.when(this.isFalse(), falseExpr)
				.defaultValue(nullExpr);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueValue expression to use when this expression is TRUE
	 * @param falseValue expression to use when this expression is FALSE
	 * @param nullValue the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public NumberExpression ifTrueFalseNull(Number trueValue, Number falseValue, Number nullValue) {
		return this.ifTrueFalseNull(
				new NumberExpression(trueValue),
				new NumberExpression(falseValue),
				NumberExpression.value(nullValue)
		);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueValue expression to use when this expression is TRUE
	 * @param falseValue expression to use when this expression is FALSE
	 * @param nullValue the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public IntegerExpression ifTrueFalseNull(Long trueValue, Long falseValue, Long nullValue) {
		return this.ifTrueFalseNull(
				new IntegerExpression(trueValue),
				new IntegerExpression(falseValue),
				IntegerExpression.value(nullValue)
		);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueValue expression to use when this expression is TRUE
	 * @param falseValue expression to use when this expression is FALSE
	 * @param nullValue the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public IntegerExpression ifTrueFalseNull(Integer trueValue, Integer falseValue, Integer nullValue) {
		return this.ifTrueFalseNull(
				new IntegerExpression(trueValue),
				new IntegerExpression(falseValue),
				IntegerExpression.value(nullValue)
		);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public NumberExpression ifTrueFalseNull(NumberExpression trueExpr, NumberExpression falseExpr, NumberExpression nullExpr) {
		return CaseExpression
				.when(this, trueExpr)
				.when(this.isFalse(), falseExpr)
				.defaultValue(nullExpr);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public InstantExpression ifTrueFalseNull(Instant trueExpr, Instant falseExpr, Instant nullExpr) {
		return CaseExpression
				.when(this, value(trueExpr))
				.when(this.isFalse(), value(falseExpr))
				.defaultValue(value(nullExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public InstantExpression ifTrueFalseNull(InstantExpression trueExpr, InstantExpression falseExpr, InstantExpression nullExpr) {
		return CaseExpression
				.when(this, trueExpr)
				.when(this.isFalse(), falseExpr)
				.defaultValue(nullExpr);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateExpression ifTrueFalseNull(LocalDate trueExpr, LocalDate falseExpr, LocalDate nullExpr) {
		return CaseExpression
				.when(this, value(trueExpr))
				.when(this.isFalse(), value(falseExpr))
				.defaultValue(value(nullExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateTimeExpression ifTrueFalseNull(LocalDateTime trueExpr, LocalDateTime falseExpr, LocalDateTime nullExpr) {
		return CaseExpression
				.when(this, value(trueExpr))
				.when(this.isFalse(), value(falseExpr))
				.defaultValue(value(nullExpr));
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateExpression ifTrueFalseNull(LocalDateExpression trueExpr, LocalDateExpression falseExpr, LocalDateExpression nullExpr) {
		return CaseExpression
				.when(this, trueExpr)
				.when(this.isFalse(), falseExpr)
				.defaultValue(nullExpr);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public LocalDateTimeExpression ifTrueFalseNull(LocalDateTimeExpression trueExpr, LocalDateTimeExpression falseExpr, LocalDateTimeExpression nullExpr) {
		return CaseExpression
				.when(this, trueExpr)
				.when(this.isFalse(), falseExpr)
				.defaultValue(nullExpr);
	}

	/**
	 * Allows you to specify different return values based on the value of this
	 * boolean expression.
	 *
	 * <p>
	 * The first expression is returned if this expression is TRUE, the second
	 * result is returned if the expression is false, and the third is returned if
	 * the expression is NULL.</p>
	 *
	 * @param trueExpr expression to use when this expression is TRUE
	 * @param falseExpr expression to use when this expression is FALSE
	 * @param nullExpr the expression to use if the expression is NULL
	 *
	 * @return an expression that will generate a SQL clause conceptually similar
	 * to "if (this) then thenExpr else elseExpr".
	 */
	public IntegerExpression ifTrueFalseNull(IntegerExpression trueExpr, IntegerExpression falseExpr, IntegerExpression nullExpr) {
		return CaseExpression
				.when(this, trueExpr)
				.when(this.isFalse(), falseExpr)
				.defaultValue(nullExpr);
	}

	/**
	 * Creates an expression that will always return FALSE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an expression that will always evaluate to FALSE.
	 */
	public static BooleanExpression falseExpression() {
		return new FalseExpression();
	}

	/**
	 * Creates an expression that will always return TRUE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an expression that will always evaluate to TRUE.
	 */
	public static BooleanExpression trueExpression() {
		return new TrueExpression();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param <B> a base class like String, Number, or Date
	 * @param <R> a value that can be compared to Z, probably StringResult,
	 * NumberResult, or DateResult
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param whenEqualsFallbackComparison the comparison used when the ColumnA
	 * and ValueA are equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>>
			BooleanExpression seekLessThan(
					RangeComparable<B, R> columnA,
					R valueA,
					BooleanExpression whenEqualsFallbackComparison) {
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
	 * @param <B> a base class like String, Number, or Date
	 * @param <R> a value that can be compared to Z, probably StringResult,
	 * NumberResult, or DateResult
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param whenEqualsFallbackComparison the comparison used when the ColumnA
	 * and ValueA are equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>>
			BooleanExpression seekGreaterThan(RangeComparable<B, R> columnA, R valueA, BooleanExpression whenEqualsFallbackComparison) {
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
	 * @param <B> the base Java type of the first expression, e.g. Integer
	 * @param <R> the Results type of the first expression, e.g. IntegerExpression
	 * @param <C> the base Java type of the second expression, e.g. Integer
	 * @param <S> the Results type of the second expression, e.g.
	 * IntegerExpression
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison and ValueA are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>, C, S extends RangeResult<C>>
			BooleanExpression
			seekLessThan(RangeComparable<B, R> columnA, R valueA, RangeComparable<C, S> columnB, S valueB) {
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
	 * on one column/value use {@link #seekGreaterThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.results.RangeResult, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <B> the base Java type of the first expression, e.g. Integer
	 * @param <R> the Results type of the first expression, e.g. IntegerExpression
	 * @param <C> the base Java type of the second expression, e.g. Integer
	 * @param <S> the Results type of the second expression, e.g.
	 * IntegerExpression
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>, C, S extends RangeResult<C>>
			BooleanExpression
			seekGreaterThan(RangeComparable<B, R> columnA, R valueA, RangeComparable<C, S> columnB, S valueB) {
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
	 * one column/value use {@link #seekLessThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.results.RangeResult, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <B> the base Java type of the first expression, e.g. Integer
	 * @param <R> the Results type of the first expression, e.g. IntegerExpression
	 * @param <C> the base Java type of the second expression, e.g. Integer
	 * @param <S> the Results type of the second expression, e.g.
	 * IntegerExpression
	 * @param <D> the base Java type of the third expression, e.g. Integer
	 * @param <T> the Results type of the third expression, e.g. IntegerExpression
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>, C, S extends RangeResult<C>, D, T extends RangeResult<D>>
			BooleanExpression
			seekLessThan(RangeComparable<B, R> columnA, R valueA, RangeComparable<C, S> columnB, S valueB, RangeComparable<D, T> columnC, T valueC) {
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
	 * one column/value use {@link #seekLessThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.results.RangeResult, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <B> the base Java type of the first expression, e.g. Integer
	 * @param <R> the Results type of the first expression, e.g. IntegerExpression
	 * @param <C> the base Java type of the second expression, e.g. Integer
	 * @param <S> the Results type of the second expression, e.g.
	 * IntegerExpression
	 * @param <D> the base Java type of the third expression, e.g. Integer
	 * @param <T> the Results type of the third expression, e.g. IntegerExpression
	 * @param <E> the base Java type of the third expression, e.g. Integer
	 * @param <U> the Results type of the third expression, e.g. IntegerExpression
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * @param columnD the left side of the internal comparison
	 * @param valueD the right side of the internal comparison
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>, C, S extends RangeResult<C>, D, T extends RangeResult<D>, E, U extends RangeResult<E>>
			BooleanExpression
			seekLessThan(RangeComparable<B, R> columnA, R valueA, RangeComparable<C, S> columnB, S valueB, RangeComparable<D, T> columnC, T valueC, RangeComparable<E, U> columnD, U valueD) {
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
	 * column/value use {@link #seekLessThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.results.RangeResult, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <B> the base Java type of the first expression, e.g. Integer
	 * @param <R> the Results type of the first expression, e.g. IntegerExpression
	 * @param <C> the base Java type of the second expression, e.g. Integer
	 * @param <S> the Results type of the second expression, e.g.
	 * IntegerExpression
	 * @param <D> the base Java type of the third expression, e.g. Integer
	 * @param <T> the Results type of the third expression, e.g. IntegerExpression
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>, C, S extends RangeResult<C>, D, T extends RangeResult<D>>
			BooleanExpression
			seekGreaterThan(RangeComparable<B, R> columnA, R valueA, RangeComparable<C, S> columnB, S valueB, RangeComparable<D, T> columnC, T valueC) {
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
	 * one column/value use {@link #seekGreaterThan(nz.co.gregs.dbvolution.results.RangeComparable, nz.co.gregs.dbvolution.results.RangeResult, nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param <B> the base Java type of the first expression, e.g. Integer
	 * @param <R> the Results type of the first expression, e.g. IntegerExpression
	 * @param <C> the base Java type of the second expression, e.g. Integer
	 * @param <S> the Results type of the second expression, e.g.
	 * IntegerExpression
	 * @param <D> the base Java type of the third expression, e.g. Integer
	 * @param <T> the Results type of the third expression, e.g. IntegerExpression
	 * @param <E> the base Java type of the third expression, e.g. Integer
	 * @param <U> the Results type of the third expression, e.g. IntegerExpression
	 * @param columnA the left side of the internal comparison
	 * @param valueA the right side of the internal comparison
	 * @param columnB the left side of the internal comparison
	 * @param valueB the right side of the internal comparison
	 * @param columnC the left side of the internal comparison
	 * @param valueC the right side of the internal comparison
	 * @param columnD the left side of the internal comparison
	 * @param valueD the right side of the internal comparison
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings("unchecked")
	public static <B, R extends RangeResult<B>, C, S extends RangeResult<C>, D, T extends RangeResult<D>, E, U extends RangeResult<E>>
			BooleanExpression
			seekGreaterThan(RangeComparable<B, R> columnA, R valueA, RangeComparable<C, S> columnB, S valueB, RangeComparable<D, T> columnC, T valueC, RangeComparable<E, U> columnD, U valueD) {
		return BooleanExpression.anyOf(
				columnA.isGreaterThan(valueA),
				BooleanExpression.allOf(columnA.is(valueA), BooleanExpression.seekGreaterThan(columnB, valueB, columnC, valueC, columnD, valueD)));
	}

	@Override
	public DBBoolean getQueryableDatatypeForExpressionValue() {
		return new DBBoolean();
	}

	/**
	 * Indicates if this expression is a relationship between 2, or more, tables.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the relationship
	 */
	public boolean isRelationship() {
		return this.getTablesInvolved().size() > 1;
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
		AnyResult<?> onlyBool1 = this.getInnerResult();
		if (onlyBool1 instanceof BooleanResult) {
			return ((BooleanResult) onlyBool1).isBooleanStatement();
		} else {
			return true;
		}
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
	public BooleanExpression modeStrict() {
		BooleanExpression modeExpr = new BooleanExpression(
				new ModeStrictExpression<>(this));

		return modeExpr;
	}

	private static abstract class DBUnaryBooleanArithmetic extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		protected DBExpression onlyBool;

		DBUnaryBooleanArithmetic(DBExpression bool) {
			this.onlyBool = bool;
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			String op = this.getEquationOperator(db);
			String returnStr = onlyBool.toSQLString(db) + " " + op;
			return returnStr;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return onlyBool == null ? new HashSet<DBRow>() : onlyBool.getTablesInvolved();
		}

		@Override
		public boolean isAggregator() {
			return onlyBool.isAggregator();
		}

		protected abstract String getEquationOperator(DBDefinition db);

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

		private static final long serialVersionUID = 1L;

		protected BooleanResult[] bools;

		DBNnaryBooleanArithmetic(BooleanResult... bools) {
			this.bools = bools;
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDefinition db) {
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
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			for (BooleanResult boo : bools) {
				hashSet.addAll(boo.getTablesInvolved());
			}
			return hashSet;
		}

		protected abstract String getEquationOperator(DBDefinition db);

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

	private static abstract class DBBinaryBooleanArithmetic extends BooleanExpression {

		private static final long serialVersionUID = 1L;

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
		public String toSQLString(DBDefinition db) {
			if (db.supportsComparingBooleanResults()) {
				return simpleToSQLString(db);
			} else {
				BooleanExpression firstParameter = this.getFirst();
				BooleanExpression secondParameter = this.getSecond();
				String returnString
						= firstParameter.getComparableBooleanSQL(db)
						+ getEquationOperator(db)
						+ secondParameter.getComparableBooleanSQL(db);
				return returnString;
			}
		}

		private String simpleToSQLString(DBDefinition db) {
			String sqlString = getFirst().toSQLString(db) + this.getEquationOperator(db) + getSecond().toSQLString(db);
			if (getFirst().getIncludesNull()) {
				sqlString = getSecond().toSQLString(db) + " IS " + db.getNull() + db.beginOrLine() + sqlString;
			}
			if (getSecond().getIncludesNull()) {
				sqlString = getFirst().toSQLString(db) + " IS " + db.getNull() + db.beginOrLine() + sqlString;
			}
			return "(" + sqlString + ")";
		}

		protected abstract String getEquationOperator(DBDefinition db);

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

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		protected BooleanExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected BooleanExpression getSecond() {
			return second;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else {
				return (first == null ? true : first.isPurelyFunctional())
						&& (second == null ? true : second.isPurelyFunctional());
			}
		}
	}

	private static abstract class DBBooleanStringStringFunction extends StringExpression {

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected StringResult first = null;
		protected StringResult second = null;

		DBBooleanStringStringFunction() {
		}

		DBBooleanStringStringFunction(BooleanExpression only, StringResult first, StringResult second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

	private static abstract class DBBooleanBooleanBooleanFunction extends StringExpression {

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected BooleanResult first = null;
		protected BooleanResult second = null;

		DBBooleanBooleanBooleanFunction() {
		}

		DBBooleanBooleanBooleanFunction(BooleanExpression only, BooleanResult first, BooleanResult second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected NumberResult first = null;
		protected NumberResult second = null;

		DBBooleanNumberNumberFunction() {
		}

		DBBooleanNumberNumberFunction(BooleanExpression only, NumberResult first, NumberResult second) {
			this.onlyBool = (only == null ? nullBoolean() : only);
			this.first = (first == null ? nullNumber() : first);
			this.second = (second == null ? nullNumber() : second);
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

	private static abstract class DBBooleanIntegerIntegerFunction extends IntegerExpression {

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected IntegerResult first = null;
		protected IntegerResult second = null;

		DBBooleanIntegerIntegerFunction() {
		}

		DBBooleanIntegerIntegerFunction(BooleanExpression only, IntegerResult first, IntegerResult second) {
			this.onlyBool = (only == null ? nullBoolean() : only);
			this.first = (first == null ? nullInteger() : first);
			this.second = (second == null ? nullInteger() : second);
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected DateExpression first = null;
		protected DateExpression second = null;

		DBBinaryDateDateFunction() {
		}

		DBBinaryDateDateFunction(BooleanExpression only, DateExpression first, DateExpression second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

	private static abstract class DBBinaryLocalDateLocalDateFunction extends LocalDateExpression {

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected LocalDateExpression first = null;
		protected LocalDateExpression second = null;

		DBBinaryLocalDateLocalDateFunction() {
		}

		DBBinaryLocalDateLocalDateFunction(BooleanExpression only, LocalDateExpression first, LocalDateExpression second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

	private static abstract class DBBinaryLocalDateTimeLocalDateTimeFunction extends LocalDateTimeExpression {

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected LocalDateTimeExpression first = null;
		protected LocalDateTimeExpression second = null;

		DBBinaryLocalDateTimeLocalDateTimeFunction() {
		}

		DBBinaryLocalDateTimeLocalDateTimeFunction(BooleanExpression only, LocalDateTimeExpression first, LocalDateTimeExpression second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

	private static abstract class DBBinaryInstantInstantFunction extends InstantExpression {

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected InstantExpression first = null;
		protected InstantExpression second = null;

		DBBinaryInstantInstantFunction() {
		}

		DBBinaryInstantInstantFunction(BooleanExpression only, InstantExpression first, InstantExpression second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + (onlyBool == null ? "" : onlyBool.toSQLString(db)) + this.afterValue(db);
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

		private static final long serialVersionUID = 1L;

		protected BooleanExpression onlyBool = null;
		protected Polygon2DExpression first = null;
		protected Polygon2DExpression second = null;

		DBBinaryGeometryGeometryFunction() {
		}

		DBBinaryGeometryGeometryFunction(BooleanExpression only, Polygon2DExpression first, Polygon2DExpression second) {
			this.onlyBool = only;
			this.first = first;
			this.second = second;
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

	private static class IntegerValueFunction extends IntegerExpression {

		private final static long serialVersionUID = 1l;

		private BooleanExpression innerBool = new BooleanExpression();

		public IntegerValueFunction(BooleanExpression bool) {
			innerBool = bool;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsComparingBooleanResults()) {
				return db.doBooleanToIntegerTransform(this.innerBool.toSQLString(db));
			} else {
				String returnString
						= innerBool.getComparableBooleanSQL(db);
				return returnString;
			}
		}

		@Override
		public IntegerValueFunction copy() {
			return new IntegerValueFunction(innerBool.copy());
		}

		@Override
		public boolean getIncludesNull() {
			return innerBool.getIncludesNull();
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return innerBool.getTablesInvolved();
		}
	}

	private class BooleanToNumberExpression extends NumberExpression {

		public BooleanToNumberExpression(BooleanExpression bool) {
			super(bool);
		}

		private final static long serialVersionUID = 1l;
		BooleanExpression innerBool = new BooleanExpression(getInnerResult());

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doBooleanToIntegerTransform(this.innerBool.toSQLString(db));
		}

		@Override
		public BooleanToNumberExpression copy() {
			return new BooleanToNumberExpression(innerBool == null ? null : innerBool.copy());
		}

		@Override
		public boolean getIncludesNull() {
			return innerBool.getIncludesNull();
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return innerBool.getTablesInvolved();
		}
	}

	private static class NullExpression extends BooleanExpression {

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

	protected static class IsExpression extends DBBinaryBooleanArithmetic {

		public IsExpression(BooleanExpression first, BooleanResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " = ";
		}

		@Override
		public IsExpression copy() {
			return new IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy());
		}

	}

	protected class IsNotExpression extends DBBinaryBooleanArithmetic {

		public IsNotExpression(BooleanExpression first, BooleanResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <> ";
		}

		@Override
		public IsNotExpression copy() {
			return new IsNotExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy());
		}
	}

	protected class XorExpression extends DBBinaryBooleanArithmetic {

		public XorExpression(BooleanExpression first, BooleanResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsXOROperator()) {
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
		protected String getEquationOperator(DBDefinition db) {
			return "^";
		}

		@Override
		public XorExpression copy() {
			return new XorExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy());
		}

	}

	protected static class AllOfExpression extends DBNnaryBooleanArithmetic {

		public AllOfExpression(BooleanResult[] bools) {
			super(bools);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return db.beginAndLine();
		}

		@Override
		public AllOfExpression copy() {
			BooleanResult[] newValues = new BooleanResult[bools.length];
			for (int i = 0; i < newValues.length; i++) {
				newValues[i] = bools[i] == null ? null : bools[i].copy();
			}
			return new AllOfExpression(newValues);
		}

	}

	protected static class AnyOfExpression extends DBNnaryBooleanArithmetic {

		public AnyOfExpression(BooleanResult[] bools) {
			super(bools);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return db.beginOrLine();
		}

		@Override
		public AnyOfExpression copy() {
			BooleanResult[] newValues = new BooleanResult[bools.length];
			for (int i = 0; i < newValues.length; i++) {
				newValues[i] = bools[i] == null ? null : bools[i].copy();
			}
			return new AnyOfExpression(newValues);
		}
	}

	protected class NegateExpression extends BooleanExpression {

		public NegateExpression(BooleanResult booleanResult) {
			super(booleanResult);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNegationFunctionName() + "(" + getInnerResult().toSQLString(db) + ")";
		}

		@Override
		public NegateExpression copy() {
			return new NegateExpression((BooleanResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	protected static class IsNotNullExpression extends BooleanExpression {

		private final DBExpression onlyBool;

		public IsNotNullExpression(DBExpression bool) {
			onlyBool = bool;
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			String returnStr = "";
			final String onlyBoolStr = onlyBool.toSQLString(db);
			if (db.requiredToProduceEmptyStringsForNull()) {
				returnStr = " (" + onlyBoolStr + " IS NOT " + db.getNull() + ") /* generic version */";
			} else {
				returnStr = onlyBoolStr + " IS NOT " + db.getNull();
			}
			return returnStr;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public IsNotNullExpression copy() {
			return new IsNotNullExpression(onlyBool == null ? null : onlyBool.copy());
		}
	}

	protected static class IsNotNullStringExpression extends BooleanExpression {

		private final StringResult onlyBool;

		public IsNotNullStringExpression(StringResult bool) {
			onlyBool = bool;
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			String returnStr;
			final String onlyBoolStr = onlyBool.toSQLString(db);
			if (db.requiredToProduceEmptyStringsForNull() && db.supportsDifferenceBetweenNullAndEmptyStringNatively()) {
				returnStr = " NOT ((" + onlyBoolStr + " IS " + db.getNull() + ") OR (" + onlyBoolStr + " = '')) /* STRING VERSION */ ";
			} else {
				returnStr = onlyBoolStr + " IS NOT " + db.getNull();// for strings
			}
			return returnStr;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public IsNotNullExpression copy() {
			return new IsNotNullExpression(onlyBool == null ? null : onlyBool.copy());
		}
	}

	protected static class IsNullExpression extends BooleanExpression {

		private final DBExpression onlyBool;

		public IsNullExpression(DBExpression bool) {
			onlyBool = bool;
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			String returnStr = "";
			final String onlyBoolStr = onlyBool.toSQLString(db);
			returnStr = onlyBoolStr + " IS " + db.getNull();
			return returnStr;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public IsNullExpression copy() {
			return new IsNullExpression(onlyBool == null ? null : onlyBool.copy());
		}
	}

	protected static class IsNullStringExpression extends BooleanExpression {

		private final StringExpression onlyBool;

		public IsNullStringExpression(StringExpression bool) {
			onlyBool = bool;
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			String returnStr = "";
			final String onlyBoolStr = onlyBool.toSQLString(db);
			returnStr = onlyBoolStr + " IS " + db.getNull();
			if (db.requiredToProduceEmptyStringsForNull()&& db.supportsDifferenceBetweenNullAndEmptyStringNatively()) {
				returnStr = " (" + returnStr + " OR " + onlyBoolStr + " = '') ";
			}
			return returnStr;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public IsNullExpression copy() {
			return new IsNullExpression(onlyBool == null ? null : onlyBool.copy());
		}
	}

	protected class BooleanIfThenElseExpression extends DBBooleanBooleanBooleanFunction {

		public BooleanIfThenElseExpression(BooleanExpression only, BooleanResult first, BooleanResult second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public BooleanIfThenElseExpression copy() {
			return new BooleanIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class StringIfThenElseExpression extends DBBooleanStringStringFunction {

		public StringIfThenElseExpression(BooleanExpression only, StringResult first, StringResult second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public StringIfThenElseExpression copy() {
			return new StringIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class NumberIfThenElseExpression extends DBBooleanNumberNumberFunction {

		public NumberIfThenElseExpression(BooleanExpression only, NumberResult first, NumberResult second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public NumberIfThenElseExpression copy() {
			return new NumberIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class IntegerIfThenElseExpression extends DBBooleanIntegerIntegerFunction {

		public IntegerIfThenElseExpression(BooleanExpression only, IntegerResult first, IntegerResult second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public IntegerIfThenElseExpression copy() {
			return new IntegerIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class DateIfThenElseExpression extends DBBinaryDateDateFunction {

		public DateIfThenElseExpression(BooleanExpression only, DateExpression first, DateExpression second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public DateIfThenElseExpression copy() {
			return new DateIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class LocalDateIfThenElseExpression extends DBBinaryLocalDateLocalDateFunction {

		public LocalDateIfThenElseExpression(BooleanExpression only, LocalDateExpression first, LocalDateExpression second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public LocalDateIfThenElseExpression copy() {
			return new LocalDateIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class LocalDateTimeIfThenElseExpression extends DBBinaryLocalDateTimeLocalDateTimeFunction {

		public LocalDateTimeIfThenElseExpression(BooleanExpression only, LocalDateTimeExpression first, LocalDateTimeExpression second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public LocalDateTimeIfThenElseExpression copy() {
			return new LocalDateTimeIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class InstantIfThenElseExpression extends DBBinaryInstantInstantFunction {

		public InstantIfThenElseExpression(BooleanExpression only, InstantExpression first, InstantExpression second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public InstantIfThenElseExpression copy() {
			return new InstantIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class Polygon2DIfThenElseExpression extends DBBinaryGeometryGeometryFunction {

		public Polygon2DIfThenElseExpression(BooleanExpression only, Polygon2DExpression first, Polygon2DExpression second) {
			super(only, first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIfThenElseTransform(onlyBool.toSQLString(db), first.toSQLString(db), second.toSQLString(db));
		}

		@Override
		public Polygon2DIfThenElseExpression copy() {
			return new Polygon2DIfThenElseExpression(
					onlyBool == null ? null : onlyBool.copy(),
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private static class FalseExpression extends BooleanExpression {

		public FalseExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getFalseOperation();
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public FalseExpression copy() {
			return new FalseExpression();
		}
	}

	private static class TrueExpression extends BooleanExpression {

		public TrueExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getTrueOperation();
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public TrueExpression copy() {
			return new TrueExpression();
		}
	}

	public static WindowFunctionFramable<BooleanExpression> firstValue() {
		return new FirstValueExpression().over();
	}

	public static class FirstValueExpression extends BooleanExpression implements CanBeWindowingFunctionWithFrame<BooleanExpression> {

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
		public WindowFunctionFramable<BooleanExpression> over() {
			return new WindowFunctionFramable<BooleanExpression>(new BooleanExpression(this));
		}

	}

	public static WindowFunctionFramable<BooleanExpression> lastValue() {
		return new LastValueExpression().over();
	}

	public static class LastValueExpression extends IntegerExpression implements CanBeWindowingFunctionWithFrame<BooleanExpression> {

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
		public WindowFunctionFramable<BooleanExpression> over() {
			return new WindowFunctionFramable<BooleanExpression>(new BooleanExpression(this));
		}

	}

	public static WindowFunctionFramable<BooleanExpression> nthValue(IntegerExpression indexExpression) {
		return new NthValueExpression(indexExpression).over();
	}

	public static class NthValueExpression extends BooleanExpression implements CanBeWindowingFunctionWithFrame<BooleanExpression> {

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
		public WindowFunctionFramable<BooleanExpression> over() {
			return new WindowFunctionFramable<BooleanExpression>(new BooleanExpression(this));
		}

	}

	/**
	 * Synonym for the lag method.
	 *
	 * <p>
	 * LAG() is a window function that provides access to a row at a specified
	 * physical offset which comes before the current row.</p>
	 *
	 * <p>
	 * The function will "look" back one row and return the value there. If there
	 * is no previous row NULL will be returned.</p>
	 *
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<BooleanExpression> previousRowValue() {
		return lag(IntegerExpression.value(1));
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
	public WindowFunctionRequiresOrderBy<BooleanExpression> lag() {
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
	public WindowFunctionRequiresOrderBy<BooleanExpression> lag(IntegerExpression offset) {
		return lag(offset, nullExpression());
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
	public WindowFunctionRequiresOrderBy<BooleanExpression> lag(IntegerExpression offset, BooleanExpression defaultExpression) {
		return new LagExpression(this, offset, defaultExpression).over();
	}

	/**
	 * Synonym for the lead method.
	 *
	 * <p>
	 * LEAD() is a window function that provides access to a row at a specified
	 * physical offset which comes after the current row.</p>
	 *
	 * <p>
	 * The function will "look" forward one row and return the value there. If
	 * there is no next row NULL will be returned.</p>
	 *
	 * @return a lag expression ready for additional configuration
	 */
	public WindowFunctionRequiresOrderBy<BooleanExpression> nextRowValue() {
		return lead(value(1));
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
	public WindowFunctionRequiresOrderBy<BooleanExpression> lead() {
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
	public WindowFunctionRequiresOrderBy<BooleanExpression> lead(IntegerExpression offset) {
		return lead(offset, nullBoolean());
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
	public WindowFunctionRequiresOrderBy<BooleanExpression> lead(IntegerExpression offset, BooleanExpression defaultExpression) {
		return new LeadExpression(this, offset, defaultExpression).over();
	}

	private static abstract class LagLeadFunction extends BooleanExpression implements CanBeWindowingFunctionRequiresOrderBy<BooleanExpression> {

		private static final long serialVersionUID = 1L;

		protected BooleanExpression first;
		protected IntegerExpression second;
		protected BooleanExpression third;

		LagLeadFunction(BooleanExpression first, IntegerExpression second, BooleanExpression third) {
			this.first = first == null ? nullBoolean() : first;
			this.second = second == null ? value(1) : second;
			this.third = third == null ? nullBoolean() : third;
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
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
		 * @return the first
		 */
		protected BooleanExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		protected IntegerExpression getSecond() {
			return second;
		}

		/**
		 * @return the second
		 */
		protected BooleanExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
		}

		@Override
		public boolean isBooleanStatement() {
			return true;
		}

		@Override
		public WindowFunctionRequiresOrderBy<BooleanExpression> over() {
			return new WindowFunctionRequiresOrderBy<BooleanExpression>(new BooleanExpression(this));
		}
	}

	public class LagExpression extends LagLeadFunction {

		private static final long serialVersionUID = 1L;

		public LagExpression(BooleanExpression first, IntegerExpression second, BooleanExpression third) {
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

		public LeadExpression(BooleanExpression first, IntegerExpression second, BooleanExpression third) {
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
