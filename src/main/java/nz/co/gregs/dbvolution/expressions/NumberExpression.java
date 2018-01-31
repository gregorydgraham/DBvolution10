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

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.EqualResult;
import nz.co.gregs.dbvolution.results.IntegerResult;

/**
 * NumberExpression implements standard functions that produce a numeric result,
 * including Integer and Real numbers.
 *
 * <p>
 * Most query requirements are provided by {@link QueryableDatatype}s like
 * {@link DBString} or {@link DBInteger} but expressions can provide more
 * functions or more precise control.
 *
 * <p>
 * Use a NumberExpression to produce a number from an existing column,
 * expression or value, and perform arithmetic.
 *
 * <p>
 * Generally you get a NumberExpression from a column or value using {@link NumberExpression#NumberExpression(java.lang.Number)
 * } or {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBInteger) }.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class NumberExpression extends SimpleNumericExpression<Number, NumberResult, DBNumber> implements NumberResult {

	private final static long serialVersionUID = 1l;

	@Override
	public NumberExpression nullExpression() {
		return new NumberExpression() {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.getNull();
			}
		};
	}

	/**
	 * Create An Appropriate Expression Object For This Object
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
	 * @param number a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	@Override
	public NumberExpression expression(NumberResult number) {
		return new NumberExpression(number);
	}

	/**
	 * Create An Appropriate Expression Object For This Object
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
	 * @param number a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	@Override
	public NumberExpression expression(DBNumber number) {
		return new NumberExpression(number);
	}

	/**
	 * Create An Appropriate Expression Object For This Object
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
	 * @param number a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public NumberExpression expression(IntegerResult number) {
		return IntegerExpression.value(number).numberResult();
	}

	/**
	 * Default Constructor
	 *
	 */
	protected NumberExpression() {
		super();
	}

	/**
	 * Create a NumberExpression based on an existing Number.
	 *
	 * <p>
	 * This performs a similar function to {@code NumberExpression(NumberResult)}.
	 *
	 * @param value a literal value to use in the expression
	 */
	public NumberExpression(Number value) {
		super(new DBNumber(value));
	}

	/**
	 * Create a NumberExpression based on an existing {@link NumberResult}.
	 *
	 * <p>
	 * {@link NumberResult} is generally a NumberExpression but it may also be a
	 * {@link DBNumber} or {@link DBInteger}.
	 *
	 * @param value a number expression or QDT
	 */
	public NumberExpression(NumberResult value) {
		super(value);
	}

	/**
	 * Create a NumberExpression based on an existing {@link NumberResult}.
	 *
	 * <p>
	 * {@link NumberResult} is generally a NumberExpression but it may also be a
	 * {@link DBNumber} or {@link DBInteger}.
	 *
	 * @param value a number expression or QDT
	 */
	protected NumberExpression(AnyResult<?> value) {
		super(value);
	}

	@Override
	public NumberExpression copy() {
		return isNullSafetyTerminator() ? nullNumber() : new NumberExpression(getInnerResult());
	}

	/**
	 * Create An Appropriate Expression Object For This Object
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
	 * @param object a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	@Override
	public NumberExpression expression(Number object) {
		final NumberExpression numberExpression = new NumberExpression(object);
		return numberExpression;
	}

//	@Override
//	public boolean isPurelyFunctional() {
//		if (innerNumberResult == null) {
//			return true;
//		} else {
//			return innerNumberResult.isPurelyFunctional();
//		}
//	}
	/**
	 * Converts the number expression into a string/character expression within
	 * the query.
	 *
	 * <p>
	 * Not that this does not produce a String like {@link Object#toString() },
	 * but a {@link StringExpression} for use on the database side.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression of the number expression.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new StringResultFunction(this));
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
	public NumberExpression modeSimple() {
		NumberExpression modeExpr = new NumberExpression(
				new ModeSimpleExpression(this));

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
	public NumberExpression modeStrict() {
		@SuppressWarnings("unchecked")
		NumberExpression modeExpr
				= new NumberExpression(
						new ModeStrictExpression(this));
		return modeExpr;
	}

	/**
	 * Derives the number of digits of this expression.
	 *
	 * <p>
	 * This method is useful to test numbers will fit within a specific field
	 * size</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a the number of digits required to display or store this
	 * expression.
	 */
	public NumberExpression numberOfDigits() {
		return this.is(0).ifThenElse(expression(1), this.abs().logBase10().integerPart().plus(1).numberResult());
	}

	/**
	 * Derives the length in digits of this expression.
	 *
	 * <p>
	 * This method is useful to test numbers will fit within a specific field
	 * size</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a the number of digits required to display or store this
	 * expression.
	 */
	public IntegerExpression lengthOfDecimalPart() {
		return this.stringResult().substringAfter(".").length();
	}

	/**
	 * Tests that a expression is shorter than or equal to the specified lengths.
	 *
	 * <p>
	 * This method is useful to test values will fit within a specific field
	 * size</p>
	 *
	 * @param maxIntegerLength
	 * @param maxDecimals
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public BooleanExpression isShorterThanOrAsLongAs(Number maxIntegerLength, Number maxDecimals) {
		return isShorterThanOrAsLongAs(value(maxIntegerLength), value(maxDecimals));
	}

	/**
	 * Tests that a expression is shorter than or equal to the specified lengths.
	 *
	 * <p>
	 * This method is useful to test values will fit within a specific field
	 * size</p>
	 *
	 * @param maxIntegerLength
	 * @param maxDecimals
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public BooleanExpression isShorterThanOrAsLongAs(IntegerResult maxIntegerLength, IntegerResult maxDecimals) {
		return isShorterThanOrAsLongAs(expression(maxIntegerLength), expression(maxDecimals));
	}

	/**
	 * Tests that a expression is shorter than or equal to the specified lengths.
	 *
	 * <p>
	 * This method is useful to test values will fit within a specific field
	 * size</p>
	 *
	 * @param maxIntegerLength
	 * @param maxDecimals
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public BooleanExpression isShorterThanOrAsLongAs(NumberResult maxIntegerLength, NumberResult maxDecimals) {
		return BooleanExpression.allOf(this.numberOfDigits().isLessThanOrEqual(maxIntegerLength),
				this.lengthOfDecimalPart().isLessThanOrEqual(maxDecimals)
		);
	}

	/**
	 * Converts the number expression to a string and appends the supplied String.
	 *
	 * @param string the string to append
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression append(String string) {
		return this.append(StringExpression.value(string));
	}

	/**
	 * Converts the number expression to a string and appends the supplied
	 * StringResult.
	 *
	 * @param string the string to append
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression append(StringResult string) {
		return this.stringResult().append(string);
	}

	/**
	 * Tests the NumberExpression against the supplied number.
	 *
	 * @param number the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression is(Number number) {
		return is(value(number));
	}

	/**
	 * Tests the NumberExpression against the supplied numberExpression.
	 *
	 * @param numberExpression the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression is(NumberResult numberExpression) {
		return new IsFunction(this, numberExpression);
	}

	/**
	 * Tests the NumberExpression against the supplied numberExpression.
	 *
	 * @param integerExpression the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression is(IntegerResult integerExpression) {
		return is(expression(integerExpression));
	}

	/**
	 * Tests the NumberExpression to see if the result is an even number.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression isEven() {
		return this.mod(2).is(0);
	}

	/**
	 * Tests the NumberExpression to see if the result is an odd number.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression isOdd() {
		return this.mod(2).isNot(0);
	}

	/**
	 * Tests the NumberExpression against the value NULL and returns true if the
	 * Number Expression is not NULL.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	/**
	 * Tests the NumberExpression against the value NULL and returns true if the
	 * Number Expression is NULL.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	/**
	 * Tests the NumberExpression against the number and returns true if the
	 * Number Expression is not equal to the number.
	 *
	 * @param number the expression needs to NOT evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression isNot(Number number) {
		return is(value(number)).not();
	}

	/**
	 * Tests the NumberExpression against the number and returns true if the
	 * Number Expression is not equal to the number.
	 *
	 * @param number the expression needs to NOT evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression isNot(IntegerResult number) {
		return is(value(number)).not();
	}

	/**
	 * Tests the NumberExpression against the {@link NumberResult} and returns
	 * true if the NumberExpression is not equal to the number.
	 *
	 * @param number the expression needs to NOT evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression isNot(NumberResult number) {
		return is(number).not();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(NumberResult lowerBound, NumberResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(IntegerResult lowerBound, IntegerResult upperBound) {
		return isBetween(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(Number lowerBound, NumberResult upperBound) {
		return isBetween(value(lowerBound), upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(Number lowerBound, IntegerResult upperBound) {
		return isBetween(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(NumberResult lowerBound, Number upperBound) {
		return isBetween(lowerBound, value(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(IntegerResult lowerBound, Number upperBound) {
		return isBetween(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(Number lowerBound, Number upperBound) {
		return isBetween(value(lowerBound), value(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(NumberResult lowerBound, NumberResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(IntegerResult lowerBound, IntegerResult upperBound) {
		return isBetweenInclusive(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(Number lowerBound, NumberResult upperBound) {
		return isBetweenInclusive(value(lowerBound), upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(Number lowerBound, IntegerResult upperBound) {
		return isBetweenInclusive(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(NumberResult lowerBound, Number upperBound) {
		return isBetweenInclusive(lowerBound, value(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(IntegerResult lowerBound, Number upperBound) {
		return isBetweenInclusive(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(Number lowerBound, Number upperBound) {
		return isBetweenInclusive(value(lowerBound), value(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(NumberResult lowerBound, NumberResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(IntegerResult lowerBound, IntegerResult upperBound) {
		return isBetweenExclusive(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(Number lowerBound, NumberResult upperBound) {
		return isBetweenExclusive(value(lowerBound), upperBound);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(Number lowerBound, IntegerResult upperBound) {
		return isBetweenExclusive(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(NumberResult lowerBound, Number upperBound) {
		return isBetweenExclusive(lowerBound, value(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(IntegerResult lowerBound, Number upperBound) {
		return isBetweenExclusive(expression(lowerBound), expression(upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound the smallest value
	 * @param upperBound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(Number lowerBound, Number upperBound) {
		return isBetweenExclusive(value(lowerBound), value(upperBound));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is less than number.
	 *
	 * @param number need to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isLessThan(Number number) {
		return isLessThan(value(number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is less than number.
	 *
	 * @param number need to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isLessThan(IntegerResult number) {
		return isLessThan(expression(number));
	}

	/**
	 * Tests the NumberExpression against the {@link NumberResult} and returns
	 * TRUE if the value is less than the value supplied.
	 *
	 * @param numberExpression needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isLessThan(NumberResult numberExpression) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, numberExpression) {
			private final static long serialVersionUID = 1l;

			@Override
			protected String getEquationOperator(DBDefinition db) {
				return " < ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is less than or equal to number.
	 *
	 * @param number needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(Number number) {
		return isLessThanOrEqual(value(number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is less than or equal to number.
	 *
	 * @param number needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isLessThanOrEqual(IntegerResult number) {
		return isLessThanOrEqual(expression(number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is less than or equal to numberExpression.
	 *
	 * @param numberExpression needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(NumberResult numberExpression) {
		return new BooleanExpression(new IsLessThanOrEqualFunction(this, numberExpression));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is greater than number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThan(Number number) {
		return isGreaterThan(value(number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is greater than number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isGreaterThan(IntegerResult number) {
		return isGreaterThan(expression(number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is greater than number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThan(NumberResult number) {
		return new BooleanExpression(new IsGreaterThanFunction(this, number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is greater than or equal to number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(Number number) {
		return isGreaterThanOrEqual(value(number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is greater than or equal to number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isGreaterThanOrEqual(IntegerResult number) {
		return isGreaterThanOrEqual(expression(number));
	}

	/**
	 * Tests the NumberExpression against the number and returns TRUE if the value
	 * is greater than or equal to number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(NumberResult number) {
		return new BooleanExpression(new IsGreaterThanOrEqualFunction(this, number));
	}

	/**
	 * Like LESSTHAN_OR_EQUAL but only includes the EQUAL values if the fallback
	 * matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(Number value, BooleanExpression fallBackWhenEquals) {
		return this.isLessThan(NumberExpression.value(value), fallBackWhenEquals);
	}

	/**
	 * Like LESSTHAN_OR_EQUAL but only includes the EQUAL values if the fallback
	 * matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThan(IntegerResult value, BooleanExpression fallBackWhenEquals) {
		return this.isLessThan(expression(value), fallBackWhenEquals);
	}

	/**
	 * Like LESSTHAN_OR_EQUAL but only includes the EQUAL values if the fallback
	 * matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(Number value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(NumberExpression.value(value), fallBackWhenEquals);
	}

	/**
	 * Like LESSTHAN_OR_EQUAL but only includes the EQUAL values if the fallback
	 * matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThan(IntegerResult value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(expression(value), fallBackWhenEquals);
	}

	/**
	 * Like GREATERTHAN_OR_EQUAL but only includes the EQUAL values if the
	 * fallback matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(NumberResult value, BooleanExpression fallBackWhenEquals) {
		return this.isLessThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Like GREATERTHAN_OR_EQUAL but only includes the EQUAL values if the
	 * fallback matches.
	 *
	 * <p>
	 * Often used to implement efficient paging by using LESSTHAN across 2
	 * columns. For example:
	 * {@code table.column(table.name).isLessThan(5, table.column(table.pkid).isLessThan(1100));}
	 *
	 * <p>
	 * If you are using this for pagination, remember to sort by the columns as
	 * well
	 *
	 * @param value the right side of the internal comparison
	 * @param fallBackWhenEquals the comparison used when the two values are
	 * equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(NumberResult value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Compares the NumberExpression against the list of possible values and
	 * returns true if the NumberExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isIn(Number... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (Number num : possibleValues) {
			if (num == null) {
				possVals.add(null);
			} else {
				possVals.add(value(num));
			}
		}
		return isIn(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Compares the NumberExpression against the list of possible values and
	 * returns true if the NumberExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isIn(IntegerResult... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (IntegerResult num : possibleValues) {
			if (num == null) {
				possVals.add(null);
			} else {
				possVals.add(expression(num));
			}
		}
		return isIn(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Compares the NumberExpression against the list of possible values and
	 * returns true if the NumberExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isIn(Collection<? extends Number> possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return isIn(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Compares the NumberExpression against the list of possible values and
	 * returns true if the NumberExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isIn(NumberResult... possibleValues) {
		BooleanExpression isinExpr
				= new IsInFunction(this, possibleValues);
		if (isinExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isinExpr);
		} else {
			return isinExpr;
		}
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static NumberExpression leastOf(IntegerResult... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (IntegerResult num : possibleValues) {
			possVals.add(new IntegerExpression(num).numberResult());
		}
		return leastOf(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static NumberExpression leastOf(Number... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static NumberExpression leastOf(Collection<? extends NumberResult> possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (NumberResult num : possibleValues) {
			possVals.add(new NumberExpression(num));
		}
		return leastOf(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the least of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the least/smallest value from the list.
	 */
	public static NumberExpression leastOf(NumberResult... possibleValues) {
		NumberExpression leastExpr = new LeastOfFunction(possibleValues);
		return leastExpr;
	}

	/**
	 * Returns the greatest/largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #max() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the largest of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the greatest/largest value from the list.
	 */
	public static NumberExpression greatestOf(Number... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return greatestOf(possVals);
	}

	/**
	 * Returns the greatest/largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #max() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the largest of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the greatest/largest value from the list.
	 */
	public static NumberExpression greatestOf(SimpleNumericExpression... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<>();
		for (SimpleNumericExpression<?, ?, ?> num : possibleValues) {
			possVals.add(num.numberResult());
		}
		return greatestOf(possVals);
	}

	/**
	 * Returns the greatest/largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #max() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the largest of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the greatest/largest value from the list.
	 */
	public static NumberExpression greatestOf(Collection<? extends NumberResult> possibleValues) {
		return greatestOf(possibleValues.toArray(new NumberResult[]{}));
	}

	/**
	 * Returns the greatest/largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #max() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues needs to be the largest of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the greatest/largest value from the list.
	 */
	public static NumberExpression greatestOf(NumberResult... possibleValues) {
		NumberExpression greatestExpr
				= new NumberExpression(new DBNnaryNumberFunction(possibleValues) {
					private final static long serialVersionUID = 1l;

					@Override
					public String toSQLString(DBDefinition db) {
						List<String> strs = new ArrayList<>();
						for (NumberResult num : this.values) {
							strs.add(num.toSQLString(db));
						}
						return db.doGreatestOfTransformation(strs);
					}

					@Override
					protected String getFunctionName(DBDefinition db) {
						return db.getGreatestOfFunctionName();
					}
				});
		return greatestExpr;
	}

	/**
	 * Provides a default option when the NumberExpression resolves to NULL within
	 * the query.
	 *
	 * @param alternative used if the expression is NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression that will substitute the given value when the
	 * NumberExpression resolves to NULL.
	 */
	public NumberExpression ifDBNull(Number alternative) {
		return ifDBNull(NumberExpression.value(alternative));
	}

	/**
	 * Provides a default option when the NumberExpression resolves to NULL within
	 * the query.
	 *
	 * @param alternative used if the expression is NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression that will substitute the given value when the
	 * NumberExpression resolves to NULL.
	 */
	public NumberExpression ifDBNull(IntegerResult alternative) {
		return ifDBNull(IntegerExpression.value(alternative).numberResult());
	}

	/**
	 * Provides a default option when the NumberExpression resolves to NULL within
	 * the query.
	 *
	 * @param alternative used if the expression is NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression that will substitute the given value when the
	 * NumberExpression resolves to NULL.
	 */
	public NumberExpression ifDBNull(NumberResult alternative) {
		return new NumberExpression(
				new NumberNumberFunctionNumberResult(this, alternative) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.doNumberIfNullTransform(this.getFirst().toSQLString(db), getSecond().toSQLString(db));
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getIfNullFunctionName();
			}
		});
	}

	/**
	 * Adds an explicit bracket at this point in the expression chain.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression that will have the existing NumberExpression
	 * wrapped in brackets..
	 */
	public NumberExpression bracket() {
		return new NumberExpression(
				new BracketUnaryFunction(this));
	}

	/**
	 * Provides access to the exponential function.
	 *
	 * <p>
	 * Raises the E (2.718281828) to the power of the current NumberExpression.
	 *
	 * <p>
	 * That is to say, if the number expression equals 2 then 2.exp() =&gt; e^2
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression representing the exponential function of the
	 * current function.
	 */
	public NumberExpression exp() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (!db.supportsExpFunction()) {
					return (new NumberExpression(Math.E)).power(this.only.isGreaterThan(799).ifThenElse(null, this.only)).toSQLString(db);
				} else {
					return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getExpFunctionName();
			}
		});
	}

	/**
	 * Provides access to the database's cosine function.
	 *
	 * <p>
	 * Computes the cosine of the expression assuming that the previous expression
	 * is in RADIANS. Use {@link #radians() } to convert degrees into radians.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression cos() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "cos";
			}
		});
	}

	/**
	 * Provides access to the database's hyperbolic cosine function.
	 *
	 * <p>
	 * Computes the hyperbolic cosine of the expression assuming that the previous
	 * expression is in RADIANS. Use {@link #radians() } to convert degrees into
	 * radians.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the hyperbolic cosine of the
	 * current number expression.
	 */
	public NumberExpression cosh() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsHyperbolicFunctionsNatively()) {
					return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
				} else {
					NumberExpression first = this.only;
					//(ex + e-x)/2
					return first.exp().plus(first.times(-1).exp().bracket()).bracket().dividedBy(2).bracket().toSQLString(db);
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "cosh";
			}
		});
	}

	/**
	 * Provides access to the database's sine function.
	 *
	 * <p>
	 * Computes the sine of the expression assuming that the previous expression
	 * is in RADIANS. Use {@link #radians() } to convert degrees into radians.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the sine of the current number
	 * expression.
	 */
	public NumberExpression sine() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "sin";
			}
		});
	}

	/**
	 * Provides access to the database's hyperbolic sine function.
	 *
	 * <p>
	 * Computes the hyperbolic sine of the expression assuming that the previous
	 * expression is in RADIANS. Use {@link #radians() } to convert degrees into
	 * radians.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the hyperbolic sine of the current
	 * number expression.
	 */
	public NumberExpression sinh() {

		return new SinhFunction(this);
	}

	@Override
	public NumberExpression numberResult() {
		return this;
	}

	public static class SinhFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		public SinhFunction(NumberExpression only) {
			this.only = only.isGreaterThan(700).ifThenElse(nullNumber(), only);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsHyperbolicFunctionsNatively()) {
				return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
			} else {
				NumberExpression first = this.only;
				//(e^x - e^-x)/2
				return first.exp().minus(first.times(-1).exp().bracket()).bracket().dividedBy(2).bracket()
						.toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "sinh";
		}
	};

	/**
	 * Provides access to the database's tangent function.
	 *
	 * <p>
	 * Computes the tangent of the expression assuming that the previous
	 * expression is in RADIANS. Use {@link #radians() } to convert degrees into
	 * radians.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the tangent of the current number
	 * expression.
	 */
	public NumberExpression tan() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "tan";
			}
		});
	}

	/**
	 * Provides access to the database's hyperbolic tangent function.
	 *
	 * <p>
	 * Computes the hyperbolic tangent of the expression assuming that the
	 * previous expression is in RADIANS. Use {@link #radians() } to convert
	 * degrees into radians.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the hyperbolic tangent of the
	 * current number expression.
	 */
	public NumberExpression tanh() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsHyperbolicFunctionsNatively()) {
					return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
				} else {
					NumberExpression first = this.only;
					//(ex - e-x)/(ex + e-x)
					return first.exp().minus(first.times(-1).exp()).bracket().dividedBy(first.exp().plus(first.times(-1).exp()).bracket()).bracket().toSQLString(db);
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "tanh";
			}
		});
	}

	/**
	 * Provides access to the database's absolute value function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the absolute value of the current
	 * number expression.
	 */
	public NumberExpression abs() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "abs";
			}
		});
	}

	/**
	 * Provides access to the database's absolute value function.
	 *
	 * <p>
	 * Synonym for {@link #abs() }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the absolute value of the current
	 * number expression.
	 */
	public NumberExpression absoluteValue() {
		return abs();
	}

	/**
	 * Provides access to the database's inverse cosine function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the inverse cosine of the current
	 * number expression.
	 */
	public NumberExpression arccos() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "acos";
			}
		});
	}

	/**
	 * Provides access to the database's inverse sine function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the inverse sine of the current
	 * number expression.
	 */
	public NumberExpression arcsin() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsArcSineFunction()) {
					return super.toSQLString(db);
				} else {
					return only.dividedBy(value(1.0).minus(only.times(only).bracket()).bracket().squareRoot()).arctan().toSQLString(db);
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "asin";
			}
		});
	}

	/**
	 * Provides access to the database's inverse tangent function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the inverse tangent of the current
	 * number expression.
	 */
	public NumberExpression arctan() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "atan";
			}
		});
	}

	/**
	 * Provides access to the database's inverse tangent function with 2
	 * arguments.
	 *
	 * <p>
	 * In a variety of computer languages, the function arctan2 is the arctangent
	 * function with two arguments. The purpose of using two arguments instead of
	 * one is to gather information on the signs of the inputs in order to return
	 * the appropriate quadrant of the computed angle, which is not possible for
	 * the single-argument arctangent function.
	 *
	 * <p>
	 * For any real number (e.g., floating point) arguments x and y not both equal
	 * to zero, arctan2(y, x) is the angle in radians between the positive x-axis
	 * of a plane and the point given by the coordinates (x, y) on it. The angle
	 * is positive for counter-clockwise angles (upper half-plane, y &gt; 0), and
	 * negative for clockwise angles (lower half-plane, y &lt; 0).
	 *
	 * @param number the ARCTAN2 of this is required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression arctan2(NumberExpression number) {
		return new NumberExpression(new NumberNumberFunctionNumberResult(this, number) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getArctan2FunctionName();
			}
		});
	}

	/**
	 * Provides access to the database's inverse tangent function with 2
	 * arguments.
	 *
	 * <p>
	 * In a variety of computer languages, the function arctan2 is the arctangent
	 * function with two arguments. The purpose of using two arguments instead of
	 * one is to gather information on the signs of the inputs in order to return
	 * the appropriate quadrant of the computed angle, which is not possible for
	 * the single-argument arctangent function.
	 *
	 * <p>
	 * For any real number (e.g., floating point) arguments x and y not both equal
	 * to zero, arctan2(y, x) is the angle in radians between the positive x-axis
	 * of a plane and the point given by the coordinates (x, y) on it. The angle
	 * is positive for counter-clockwise angles (upper half-plane, y &gt; 0), and
	 * negative for clockwise angles (lower half-plane, y &lt; 0).
	 *
	 * @param number the ARCTAN2 of this is required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression arctan2(IntegerExpression number) {
		return this.arctan2(number.numberResult());
	}

	/**
	 * Provides access to the database's inverse tangent function with 2
	 * arguments.
	 *
	 * <p>
	 * In a variety of computer languages, the function arctan2 is the arctangent
	 * function with two arguments. The purpose of using two arguments instead of
	 * one is to gather information on the signs of the inputs in order to return
	 * the appropriate quadrant of the computed angle, which is not possible for
	 * the single-argument arctangent function.
	 *
	 * <p>
	 * For any real number (e.g., floating point) arguments x and y not both equal
	 * to zero, arctan2(y, x) is the angle in radians between the positive x-axis
	 * of a plane and the point given by the coordinates (x, y) on it. The angle
	 * is positive for counter-clockwise angles (upper half-plane, y &gt; 0), and
	 * negative for clockwise angles (lower half-plane, y &lt; 0).
	 *
	 * @param number the ARCTAN2 of this is required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression arctan2(Number number) {
		return arctan2(new NumberExpression(number));
	}

	/**
	 * Provides access to the database's cotangent function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the cotangent of the current number
	 * expression.
	 */
	public NumberExpression cotangent() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsCotangentFunction()) {
					return super.toSQLString(db);
				} else {
					return only.cos().dividedBy(only.sine()).bracket().toSQLString(db);
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "cot";
			}
		});
	}

	/**
	 * Provides access to the database's degrees function.
	 *
	 * <p>
	 * Converts radians to degrees.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the inverse tangent of the current
	 * number expression.
	 */
	public NumberExpression degrees() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsDegreesFunction()) {
					return super.toSQLString(db);
				} else {
					return db.doDegreesTransform(this.only.toSQLString(db));
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "degrees";
			}
		});
	}

	/**
	 * Provides access to the database's radians function.
	 *
	 * <p>
	 * Converts degrees to radians.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression representing the inverse tangent of the current
	 * number expression.
	 */
	public NumberExpression radians() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsRadiansFunction()) {
					return super.toSQLString(db);
				} else {
					return db.doRadiansTransform(this.only.toSQLString(db));
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "radians";
			}
		});
	}

	/**
	 * returns the Natural Logarithm of the current NumberExpression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NimberExpression of the natural logarithm of the current
	 * expression.
	 */
	public NumberExpression logN() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getNaturalLogFunctionName();
			}
		});
	}

	/**
	 * returns the Logarithm Base-10 of the current NumberExpression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NimberExpression of the logarithm base-10 of the current
	 * expression.
	 */
	public NumberExpression logBase10() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.doLogBase10NumberTransform(this.only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getLogBase10FunctionName();
			}
		});
	}

	/**
	 * Provides access to the power (or pow) function of the database.
	 *
	 * <p>
	 * For a NumberExpression x then x.power(n) =&gt; x^n.
	 *
	 * @param n	n
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression power(NumberResult n) {
		return new NumberExpression(new NumberNumberFunctionNumberResult(this, n) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "power";
			}
		});
	}

	/**
	 * Provides access to the power (or pow) function of the database.
	 *
	 * <p>
	 * For a NumberExpression x then x.power(n) =&gt; x^n.
	 *
	 * @param n	n
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression power(Number n) {
		return power(value(n));
	}

	/**
	 * Provides access to a random floating-point value x in the range 0 &lt;= x
	 * &lt; 1.0.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression that provides a random number when used in a
	 * query.
	 */
	static public NumberExpression random() {
		return new NumberExpression(new DBNonaryFunction() {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.doRandomNumberTransform();
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "rand";
			}

		});
	}

	/**
	 * Returns the sign of the argument as -1, 0, or 1, depending on whether X is
	 * negative, zero, or positive.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression sign() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "sign";
			}
		});
	}

	/**
	 * Returns the square root of a nonnegative number X.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression squareRoot() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "sqrt";
			}
		});
	}

	/**
	 * Implements support for CEIL().
	 *
	 * <p>
	 * Returns the smallest integer that is larger than the expression
	 *
	 * <p>
	 * Note:<br>
	 * (new DBNumber( 1.5)).ceil() == 2<br>
	 * (new DBNumber(-1.5)).ceil() == -1
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value of the equation rounded up to the nearest integer.
	 */
	public NumberExpression roundUp() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getRoundUpFunctionName();
			}
		});
	}

	/**
	 * Implements support for ROUND()
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the equation rounded to the nearest integer.
	 */
	public NumberExpression round() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.doRoundTransform(only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "round";
			}
		});
	}

	/**
	 * Implements support for rounding to an arbitrary number of decimal places.
	 *
	 * <p>
	 * For instance if you require numbers like 12.345 you should use .round(3) to
	 * get the 3 digits after the decimal point.
	 *
	 * @param decimalPlaces the number of significant places that are required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the equation rounded to the nearest integer.
	 */
	public NumberExpression round(Integer decimalPlaces) {
		return round(expression(decimalPlaces));
	}

	/**
	 * Implements support for rounding to an arbitrary number of decimal places.
	 *
	 * <p>
	 * For instance if you require numbers like 12.345 you should use .round(3) to
	 * get the 3 digits after the decimal point.
	 *
	 * @param decimalPlaces the number of significant places that are required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the equation rounded to the nearest integer.
	 */
	public NumberExpression round(Long decimalPlaces) {
		return round(expression(decimalPlaces));
	}

	/**
	 * Implements support for rounding to an arbitrary number of decimal places.
	 *
	 * <p>
	 * For instance if you require numbers like 12.345 you should use .round(3) to
	 * get the 3 digits after the decimal point.
	 *
	 * @param decimalPlaces the number of significant places that are required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the equation rounded to the nearest integer.
	 */
	public NumberExpression round(IntegerResult decimalPlaces) {
		return round(expression(decimalPlaces).numberResult());
	}

	/**
	 * Implements support for rounding to an arbitrary number of decimal places.
	 *
	 * <p>
	 * For instance if you require numbers like 12.345 you should use .round(3) to
	 * get the 3 digits after the decimal point.
	 *
	 * @param decimalPlaces the number of significant places that are required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the equation rounded to the nearest integer.
	 */
	public NumberExpression round(NumberResult decimalPlaces) {
		return round(NumberExpression.value(decimalPlaces));
	}

	/**
	 * Implements support for rounding to an arbitrary number of decimal places.
	 *
	 * <p>
	 * For instance if you require numbers like 12.345 you should use .round(3) to
	 * get the 3 digits after the decimal point.
	 *
	 * @param decimalPlaces the number of significant places that are required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the equation rounded to the nearest integer.
	 */
	public NumberExpression round(NumberExpression decimalPlaces) {
		return new NumberExpression(new NumberNumberFunctionNumberResult(this, NumberExpression.value(decimalPlaces)) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				try {
					return db.doRoundWithDecimalPlacesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException exp) {
					NumberExpression power = NumberExpression.value(10).power(getSecond().round());
					return getFirst().times(power).round().dividedBy(power).toSQLString(db);
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "round";
			}
		});
	}

	/**
	 * Implements support for FLOOR().
	 *
	 * <p>
	 * Returns the largest integer that is smaller than the expression
	 *
	 * <p>
	 * note that this is not the same as {@code trunc()} as
	 * {@code roundDown(-1.5) == -2} and {@code trunc(-1.5) == -1}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value of the equation rounded down to the nearest integer.
	 */
	public NumberExpression roundDown() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return "floor";
			}
		});
	}

	/**
	 * Implements support for TRUNC().
	 *
	 * <p>
	 * Removes the decimal part of the expression, leaving an integer.
	 *
	 * <p>
	 * note that this is not the same as roundDown() as
	 * {@code roundDown(-1.5) == -2} and {@code trunc(-1.5) == -1}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value of the equation with the decimal part removed.
	 */
	public IntegerExpression trunc() {
		return new IntegerExpression(new DBUnaryIntegerFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.doTruncTransform(only.toSQLString(db), "0");
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getTruncFunctionName();
			}

//			@Override
//			protected String afterValue(DBDefinition db) {
//				return ", 0) ";
//			}
		});
	}

	/**
	 * Removes the decimal part, if there is any, from this number and returns
	 * only the integer part.
	 *
	 * <p>
	 * For example value(3.5).integerPart() = 3
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	public IntegerExpression integerPart() {
		return this.trunc();
	}

	/**
	 * Removes the decimal part, if there is any, from this number and returns
	 * only the integer part.
	 *
	 * <p>
	 * For example value(3.5).integerPart() = 3
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	@Override
	public IntegerExpression integerResult() {
		return new IntegerExpression(new DBUnaryIntegerFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.doNumberToIntegerTransform(only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getTruncFunctionName();
			}

			@Override
			protected String afterValue(DBDefinition db) {
				return ", 0) ";
			}

		});
	}

	/**
	 * Removes the decimal part, if there is any, from this number and returns
	 * only the integer part.
	 *
	 * <p>
	 * For example value(3.5).floor() = 3
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	public IntegerExpression floor() {
		return this.trunc();
	}

	/**
	 * Removes the integer part, if there is any, from this number and returns
	 * only the decimal part.
	 *
	 * <p>
	 * For example value(3.5).decimalPart() = 0.5
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression decimalPart() {
		return this.minus(this.trunc().numberResult()).bracket();
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a NumberExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression minus(NumberResult number) {
		return new NumberExpression(new MinusBinaryArithmetic(this, number));
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a NumberExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression minus(IntegerResult number) {
		return minus(expression(number));
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a NumberExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression minus(Number num) {
		return minus(value(num));
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a NumberExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression plus(NumberResult number) {
		return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(number)) {
			private final static long serialVersionUID = 1l;

			@Override
			protected String getEquationOperator(DBDefinition db) {
				return " + ";
			}
		});
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a NumberExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression plus(Number num) {
		return plus(value(num));
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a NumberExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression plus(IntegerResult num) {
		return plus(expression(num));
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a NumberExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression times(NumberResult number) {
		return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(number)) {
			private final static long serialVersionUID = 1l;

			@Override
			protected String getEquationOperator(DBDefinition db) {
				return " * ";
			}
		});
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a NumberExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression times(Number num) {
		return times(value(num));
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a NumberExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression times(IntegerResult num) {
		return times(expression(num));
	}

	/**
	 * Provides access to the basic arithmetic operation divide.
	 *
	 * <p>
	 * For a NumberExpression x: x.dividedBy(y) =&gt; x / y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression dividedBy(NumberResult number) {
		return new NumberExpression(new DBBinaryArithmetic(this,
				new NumberExpression(number)) {
			private final static long serialVersionUID = 1l;

			@Override
			protected String getEquationOperator(DBDefinition db) {
				return " / ";
			}

			@Override
			public String toSQLString(DBDefinition db) {
				return "(0.0+" + first.toSQLString(db) + ")" + this.getEquationOperator(db) + second.toSQLString(db);
			}

		}
		);
	}

	/**
	 * Division as represent by x/y.
	 *
	 * <p>
	 * For a NumberExpression x: x.dividedBy(y) =&gt; x / y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression of a division operation.
	 */
	public NumberExpression dividedBy(Number num) {
		return new NumberExpression(new DivisionBinaryArithmetic(this, new NumberExpression(num)));
	}

	/**
	 * Division as represent by x/y.
	 *
	 * <p>
	 * For a NumberExpression x: x.dividedBy(y) =&gt; x / y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression of a division operation.
	 */
	public NumberExpression dividedBy(IntegerResult num) {
		return dividedBy(new IntegerExpression(num).numberResult());
	}

	/**
	 * MOD returns the remainder from integer division.
	 *
	 * <p>
	 * DBvolution implements mod as a function. The two arguments to the function
	 * are evaluated before MOD is applied.
	 *
	 * <p>
	 * This differs from some implementations where MOD is the "%" operator and is
	 * considered analogous to "*" and "/". However databases vary in their
	 * implementation and Wikipedia, as of 11 Sept 2014, does not include "%" in
	 * Arithmetic. So I have decided to err on the side of consistency between
	 * databases and implement it so that mod() will return the same result for
	 * all databases.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression of a Modulus operation.
	 */
	public NumberExpression mod(NumberResult number) {
		return new NumberExpression(new NumberNumberFunctionNumberResult(this, NumberExpression.value(number)) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsModulusFunction()) {
					return db.doModulusTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} else {
					return "((" + getFirst().toSQLString(db) + ") % (" + getSecond().toSQLString(db) + "))";
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return "MOD";
			}
		});
	}

	/**
	 * MOD returns the remainder from integer division.
	 *
	 * <p>
	 * DBvolution implements mod as a function. The two arguments to the function
	 * are evaluated before MOD is applied.
	 *
	 * <p>
	 * This differs from some implementations where MOD is the "%" operator and is
	 * considered analogous to "*" and "/". However databases vary in their
	 * implementation and Wikipedia, as of 11 Sept 2014, does not include "%" in
	 * Arithmetic. So I have decided to err on the side of consistency between
	 * databases and implement it so that mod() will return the same result for
	 * all databases.
	 *
	 * @param num =&gt; MOD(this,num).
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression mod(Number num) {
		return this.mod(value(num));
	}

	/**
	 * MOD returns the remainder from integer division.
	 *
	 * <p>
	 * DBvolution implements mod as a function. The two arguments to the function
	 * are evaluated before MOD is applied.
	 *
	 * <p>
	 * This differs from some implementations where MOD is the "%" operator and is
	 * considered analogous to "*" and "/". However databases vary in their
	 * implementation and Wikipedia, as of 11 Sept 2014, does not include "%" in
	 * Arithmetic. So I have decided to err on the side of consistency between
	 * databases and implement it so that mod() will return the same result for
	 * all databases.
	 *
	 * @param num =&gt; MOD(this,num).
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a NumberExpression
	 */
	public NumberExpression mod(IntegerResult num) {
		return this.mod(expression(num));
	}

	/**
	 * Based on the value of this expression, select a string from the list
	 * provided.
	 *
	 * <p>
	 * Based on the MS SQLServer CHOOSE function, this method will select the
	 * string as though the list was a 0-based array of strings and this
	 * expression were the index.
	 *
	 * Value 0 returns the first string, value 1 returns the second, etc. </p>
	 *
	 * <p>
	 * If the index is too large NULL is returned.</p>
	 *
	 * @param stringsToChooseFrom a list of values that the should replace the
	 * number.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that selects the string from the list based on this expression.
	 */
	public StringExpression choose(String... stringsToChooseFrom) {
		List<StringResult> strResult = new ArrayList<>();
		for (String str : stringsToChooseFrom) {
			strResult.add(new StringExpression(str));
		}
		return choose(strResult.toArray(new StringResult[]{}));
	}

	/**
	 * Based on the value of this expression, select a string from the list
	 * provided.
	 *
	 * <p>
	 * Based on the MS SQLServer CHOOSE function, this method will select the
	 * string as though the list was a 0-based array of strings and this
	 * expression were the index.
	 *
	 * Value 0 returns the first string, value 1 returns the second, etc.</p>
	 *
	 * <p>
	 * If the index is too large NULL is returned.</p>
	 *
	 * @param stringsToChooseFrom a list of values that the should replace the
	 * number.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that selects the string from the list based on this expression.
	 */
	public StringExpression choose(StringResult... stringsToChooseFrom) {
		StringExpression leastExpr
				= new StringExpression(new DBNumberAndNnaryStringFunction(this, stringsToChooseFrom) {
					private final static long serialVersionUID = 1l;

					@Override
					public String toSQLString(DBDefinition db) {
						List<String> strs = new ArrayList<>();
						for (StringResult num : this.values) {
							strs.add(num.toSQLString(db));
						}
						return db.doChooseTransformation(NumberExpression.value(numberExpression).plus(1).bracket().toSQLString(db), strs);
					}

//					@Override
//					protected String getFunctionName(DBDefinition db) {
//						return db.getChooseFunctionName();
//					}
				});
		return leastExpr;
	}

	/**
	 * Based on the value of this expression, select a string from the list
	 * provided.
	 *
	 * <p>
	 * Based on the MS SQLServer CHOOSE function, this method will select the
	 * string as though the list was a 0-based array of strings and this
	 * expression were the index.
	 *
	 * Value 0 returns the first string, value 1 returns the second, etc. </p>
	 *
	 * <p>
	 * If the index is too large the last value is returned.</p>
	 *
	 * @param stringsToChooseFrom a list of values that the should replace the
	 * number.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that selects the string from the list based on this expression.
	 */
	public StringExpression chooseWithDefault(String... stringsToChooseFrom) {
		List<StringResult> strResult = new ArrayList<>();
		for (String str : stringsToChooseFrom) {
			strResult.add(new StringExpression(str));
		}
		return chooseWithDefault(strResult.toArray(new StringResult[]{}));
	}

	/**
	 * Based on the value of this expression, select a string from the list
	 * provided.
	 *
	 * <p>
	 * Based on the MS SQLServer CHOOSE function, this method will select the
	 * string as though the list was a 0-based array of strings and this
	 * expression were the index.
	 *
	 * Value 0 returns the first string, value 1 returns the second, etc.</p>
	 *
	 * <p>
	 * If the index is too large the last value is returned.</p>
	 *
	 * @param stringsToChooseFrom a list of values that the should replace the
	 * number.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that selects the string from the list based on this expression.
	 */
	public StringExpression chooseWithDefault(StringResult... stringsToChooseFrom) {
		StringExpression expr
				= this.choose(stringsToChooseFrom)
						.ifDBNull(stringsToChooseFrom[stringsToChooseFrom.length - 1]);
		return expr;
	}

	/**
	 * Provides access to the database's AVERAGE aggregator.
	 *
	 * <p>
	 * Calculates the average of all rows generated by the query.
	 *
	 * <p>
	 * For use with {@link DBReport}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A number expression representing the average of the grouped rows.
	 */
	public NumberExpression average() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getAverageFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	/**
	 * Synonym for {@link NumberExpression#standardDeviation() }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A number expression representing the standard deviation of the
	 * grouped rows.
	 */
	public NumberExpression stddev() {
		return standardDeviation();
	}

	/**
	 * Synonym for {@link NumberExpression#standardDeviation() }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A number expression representing the standard deviation of the
	 * grouped rows.
	 */
	public NumberExpression standardDeviation() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				if (db.supportsStandardDeviationFunction()) {
					return super.toSQLString(db);
				} else if (this.only != null) {
					NumberExpression numb = this.only;
					return new NumberExpression(numb).max().minus(new NumberExpression(numb).min()).bracket().dividedBy(6).toSQLString(db);
				} else {
					return null;
				}
			}

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getStandardDeviationFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	/**
	 * Returns the greatest/largest value from the column.
	 *
	 * <p>
	 * Similar to
	 * {@link #greatestOf(nz.co.gregs.dbvolution.results.NumberResult...) } but
	 * this aggregates the column or expression provided, rather than scanning a
	 * list.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the greatest/largest value from the column.
	 */
	public NumberExpression max() {
		return new NumberExpression(new MaxUnaryFunction(this));
	}

	/**
	 * Returns the least/smallest value from the column.
	 *
	 * <p>
	 * Similar to {@link #leastOf(nz.co.gregs.dbvolution.results.NumberResult...)
	 * } but this aggregates the column or expression provided, rather than
	 * scanning a list.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the least/smallest value from the column.
	 */
	public NumberExpression min() {
		return new NumberExpression(new MinUnaryFunction(this));
	}

	/**
	 * Returns the sum of all the values from the column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the sum of all the values from the column.
	 */
	public NumberExpression sum() {
		return new NumberExpression(new DBUnaryFunction(this) {
			private final static long serialVersionUID = 1l;

			@Override
			String getFunctionName(DBDefinition db) {
				return db.getSumFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	@Override
	public DBNumber getQueryableDatatypeForExpressionValue() {
		return new DBNumber();
	}

	/**
	 * Multiples this expression by itself to return the value squared.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression squared() {
		return this.bracket().times(this.bracket());
	}

	/**
	 * Multiples this expression by its square to return the value cubed.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression cubed() {
		return this.squared().times(this.bracket());
	}

	@Override
	public DBNumber asExpressionColumn() {
		return new DBNumber(this);
	}

	/**
	 * Returns "+" for all zero or positive numbers and "-" for all negative
	 * numbers.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression that is either "+" or "-"
	 */
	public StringExpression signPlusMinus() {
		return this.isGreaterThanOrEqual(0).ifThenElse("+", "-");
	}

	private static abstract class DBBinaryArithmetic extends NumberExpression {

		public NumberResult first;
		public NumberResult second;

		DBBinaryArithmetic() {
			this.first = null;
			this.second = null;
		}

		DBBinaryArithmetic(NumberResult first, NumberResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
		}

		@Override
		public DBBinaryArithmetic copy() {
			DBBinaryArithmetic newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (IllegalAccessException | InstantiationException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

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

		protected abstract String getEquationOperator(DBDefinition db);

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
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

	private static abstract class DBNonaryFunction extends NumberExpression {

		DBNonaryFunction() {
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return " " + getFunctionName(db) + "";
		}

		protected String afterValue(DBDefinition db) {
			return " ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + this.afterValue(db);
		}

		@Override
		public DBNonaryFunction copy() {
			DBNonaryFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return new HashSet<DBRow>();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			return true;
		}
	}

	private static abstract class DBUnaryFunction extends NumberExpression {

		protected NumberExpression only;

		DBUnaryFunction() {
			this.only = null;
		}

		DBUnaryFunction(NumberExpression only) {
			this.only = only;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
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
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DBUnaryFunction copy() {
			DBUnaryFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = only.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (only != null) {
				hashSet.addAll(only.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (only == null) {
				return true;
			} else {
				return only.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBUnaryIntegerFunction extends IntegerExpression {

		protected NumberExpression only;

		DBUnaryIntegerFunction() {
			this.only = null;
		}

		DBUnaryIntegerFunction(NumberExpression only) {
			this.only = only;
		}

//		DBUnaryFunction(DBExpression only) {
//			this.only = only;
//		}
		@Override
		public DBInteger getQueryableDatatypeForExpressionValue() {
			return new DBInteger();
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
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DBUnaryIntegerFunction copy() {
			DBUnaryIntegerFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = only.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (only != null) {
				hashSet.addAll(only.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (only == null) {
				return true;
			} else {
				return only.isPurelyFunctional();
			}
		}
	}

	private static abstract class NumberNumberFunctionNumberResult extends NumberExpression {

		protected NumberExpression first;
		protected NumberExpression second;

		NumberNumberFunctionNumberResult(NumberExpression first) {
			this.first = first;
			this.second = null;
		}

		NumberNumberFunctionNumberResult(NumberExpression first, NumberExpression second) {
			this.first = first;
			this.second = second;
		}

		NumberNumberFunctionNumberResult(NumberExpression first, NumberResult second) {
			this.first = first;
			this.second = NumberExpression.value(second);
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + getFirst().toSQLString(db) + this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public NumberNumberFunctionNumberResult copy() {
			NumberNumberFunctionNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst().copy();
			newInstance.second = getSecond().copy();
			return newInstance;
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
		protected NumberExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected NumberExpression getSecond() {
			return second;
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

	private static abstract class DBBinaryBooleanArithmetic extends BooleanExpression {

		private NumberExpression first;
		private NumberResult second;
		private boolean requiresNullProtection;

		DBBinaryBooleanArithmetic(NumberExpression first, NumberResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		NumberExpression getFirst() {
			return first;
		}

		NumberResult getSecond() {
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
				return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
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

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional();
		}
	}

	private static abstract class DBNnaryBooleanFunction extends BooleanExpression {

		private NumberExpression column;
		private final List<NumberResult> values = new ArrayList<NumberResult>();
		boolean nullProtectionRequired = false;

		DBNnaryBooleanFunction() {
		}

		DBNnaryBooleanFunction(NumberExpression leftHandSide, NumberResult[] rightHandSide) {
			this.column = leftHandSide;
			for (NumberResult numberResult : rightHandSide) {
				if (numberResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (numberResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					}
					values.add(numberResult);
				}
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			StringBuilder builder = new StringBuilder();
			builder
					.append(getColumn().toSQLString(db))
					.append(this.getFunctionName(db))
					.append(this.beforeValue(db));
			String separator = "";
			for (NumberResult val : getValues()) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public DBNnaryBooleanFunction copy() {
			DBNnaryBooleanFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.getColumn().copy();
			Collections.copy(this.getValues(), newInstance.getValues());
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (getColumn() != null) {
				hashSet.addAll(getColumn().getTablesInvolved());
			}
			for (NumberResult second : getValues()) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = getColumn().isAggregator();
			for (NumberResult numer : getValues()) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean isPurelyFunctional() {
			boolean result = getColumn().isPurelyFunctional();
			for (NumberResult numer : getValues()) {
				result = result && numer.isPurelyFunctional();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			this.nullProtectionRequired = nullsAreIncluded;
//		}
		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the column
		 */
		protected NumberExpression getColumn() {
			return column;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the values
		 */
		protected List<NumberResult> getValues() {
			return values;
		}
	}

	private static abstract class DBNnaryNumberFunction extends NumberExpression {

		protected NumberExpression column;
		protected final List<NumberResult> values = new ArrayList<NumberResult>();
		boolean nullProtectionRequired = false;

		DBNnaryNumberFunction() {
		}

		DBNnaryNumberFunction(NumberResult[] rightHandSide) {
			for (NumberResult numberResult : rightHandSide) {
				if (numberResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (numberResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					}
					values.add(numberResult);
				}
			}
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return "( ";
		}

		protected String afterValue(DBDefinition db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			StringBuilder builder = new StringBuilder();
			builder
					.append(this.getFunctionName(db))
					.append(this.beforeValue(db));
			String separator = "";
			for (NumberResult val : values) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public DBNnaryNumberFunction copy() {
			DBNnaryNumberFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			Collections.copy(this.values, newInstance.values);
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			for (NumberResult second : values) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false;
			if (column != null) {
				result = column.isAggregator();
			}
			for (NumberResult numer : values) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (column == null && values.isEmpty()) {
				return true;
			} else {
				boolean result = column == null ? true : column.isPurelyFunctional();
				for (NumberResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class DBNumberAndNnaryStringFunction extends StringExpression {

		protected NumberResult numberExpression = null;
		protected final List<StringResult> values = new ArrayList<StringResult>();
		boolean nullProtectionRequired = false;

		DBNumberAndNnaryStringFunction() {
		}

		DBNumberAndNnaryStringFunction(NumberResult numberResult, StringResult[] rightHandSide) {
			numberExpression = numberResult;
			for (StringResult stringResult : rightHandSide) {
				if (stringResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (stringResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					}
					values.add(stringResult);
				}
			}
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		abstract public String toSQLString(DBDefinition db);

		@Override
		public DBNumberAndNnaryStringFunction copy() {
			DBNumberAndNnaryStringFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.numberExpression = this.numberExpression.copy();
			Collections.copy(this.values, newInstance.values);
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (numberExpression != null) {
				hashSet.addAll(numberExpression.getTablesInvolved());
			}
			for (StringResult second : values) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = numberExpression.isAggregator();
			for (StringResult numer : values) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (numberExpression == null && values.isEmpty()) {
				return true;
			} else {
				boolean result = numberExpression == null ? true : numberExpression.isPurelyFunctional();
				for (StringResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class DBUnaryStringFunction extends StringExpression {

		protected NumberExpression only;

		DBUnaryStringFunction() {
			this.only = null;
		}

		DBUnaryStringFunction(NumberExpression only) {
			this.only = only;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
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
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DBUnaryStringFunction copy() {
			DBUnaryStringFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = only.copy();
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (only != null) {
				hashSet.addAll(only.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (only == null) {
				return true;
			} else {
				return only.isPurelyFunctional();
			}
		}

	}

	private static class MaxUnaryFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		MaxUnaryFunction(NumberExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getMaxFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class MinUnaryFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		MinUnaryFunction(NumberExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getMinFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class MinusBinaryArithmetic extends DBBinaryArithmetic {

		private final static long serialVersionUID = 1l;

		MinusBinaryArithmetic(NumberResult first, NumberResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " - ";
		}
	}

	private static class BracketUnaryFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		BracketUnaryFunction(NumberExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}
	}

	private static class DivisionBinaryArithmetic extends DBBinaryArithmetic {

		private final static long serialVersionUID = 1l;

		DivisionBinaryArithmetic(NumberResult first, NumberResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " / ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + "(" + second.toSQLString(db) + "+0.0)";
		}
	}

	private class IsInFunction extends DBNnaryBooleanFunction {

		private final static long serialVersionUID = 1l;

		public IsInFunction(NumberExpression leftHandSide, NumberResult[] rightHandSide) {
			super(leftHandSide, rightHandSide);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<>();
			for (NumberResult value : this.getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return " IN ";
		}
	}

	private class StringResultFunction extends DBUnaryStringFunction {

		private final static long serialVersionUID = 1l;

		public StringResultFunction(NumberExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doNumberToStringTransform(super.only.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean getIncludesNull() {
			return only.getIncludesNull();
		}
	}

	private class IsFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsFunction(NumberExpression first, NumberResult second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (super.getIncludesNull()) {
				return BooleanExpression.isNull(getFirst()).toSQLString(db);
			} else {
				return db.doNumberEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " = ";
		}
	}

	private static class IsLessThanOrEqualFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsLessThanOrEqualFunction(NumberExpression first, NumberResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static class IsGreaterThanFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsGreaterThanFunction(NumberExpression first, NumberResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " > ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static class IsGreaterThanOrEqualFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsGreaterThanOrEqualFunction(NumberExpression first, NumberResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " >= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static class LeastOfFunction extends DBNnaryNumberFunction {

		private final static long serialVersionUID = 1l;

		public LeastOfFunction(NumberResult[] rightHandSide) {
			super(rightHandSide);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<>();
			for (NumberResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doLeastOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getLeastOfFunctionName();
		}
	}
}
