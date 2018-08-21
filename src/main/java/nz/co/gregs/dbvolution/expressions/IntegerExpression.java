/*
 * Copyright 2017 Gregory Graham.
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
import nz.co.gregs.dbvolution.results.IntegerResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.results.SimpleNumericResult;

/**
 * IntegerExpression implements standard functions that produce a integer
 * result.
 *
 * <p>
 * Most query requirements are provided by {@link QueryableDatatype}s like
 * {@link DBString} or {@link DBInteger} but expressions can provide more
 * functions or more precise control.
 *
 * <p>
 * Use a IntegerExpression to produce a integer from an existing column,
 * expression or value, and perform arithmetic.
 *
 * <p>
 * Generally you get a IntegerExpression from a column or value using {@link IntegerExpression#IntegerExpression(int)
 * } or {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBInteger) }.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class IntegerExpression extends SimpleNumericExpression<Long, IntegerResult, DBInteger> implements IntegerResult {

	private final static long serialVersionUID = 1l;

	public static IntegerExpression ifThenElse(BooleanExpression test, IntegerExpression trueResult, IntegerExpression falseResult) {
		return test.ifThenElse(trueResult, falseResult);
	}

	@Override
	public final IntegerExpression nullExpression() {
		return new NullExpression();
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
	public IntegerExpression expression(IntegerResult number) {
		return new IntegerExpression(number);
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
	public IntegerExpression expression(NumberResult number) {
		return new NumberExpression(number).integerResult();
	}

	/**
	 * Default Constructor
	 *
	 */
	protected IntegerExpression() {
		super();
	}

	/**
	 * Create a IntegerExpression based on an existing Integer.
	 *
	 * <p>
	 * This performs a similar function to
	 * {@code IntegerExpression(IntegerResult)}.
	 *
	 * @param value a literal value to use in the expression
	 */
	public IntegerExpression(int value) {
		super(new DBInteger(value));
	}

	/**
	 * Create a IntegerExpression based on an existing Long.
	 *
	 * <p>
	 * This performs a similar function to
	 * {@code IntegerExpression(IntegerResult)}.
	 *
	 * @param value a literal value to use in the expression
	 */
	public IntegerExpression(long value) {
		super(new DBInteger(value));
	}

	/**
	 * Create a IntegerExpression based on an existing {@link IntegerResult}.
	 *
	 * <p>
	 * {@link IntegerResult} is generally a IntegerExpression but it may also be a
	 * {@link DBInteger}.
	 *
	 * @param value a number expression or QDT
	 */
	public IntegerExpression(IntegerResult value) {
		super(value);
	}

	/**
	 *
	 * @param only
	 */
	protected IntegerExpression(AnyResult<?> only) {
		super(only);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return getInnerResult().toSQLString(db);
	}

	@Override
	public IntegerExpression copy() {
		if (isNullSafetyTerminator()) {
			return nullInteger();
		} else {
			IntegerExpression expr = new IntegerExpression((AnyResult<?>) getInnerResult().copy());
			return expr;
		}
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
	public IntegerExpression expression(Long object
	) {
		final IntegerExpression integerExpression = new IntegerExpression(object);
		return integerExpression;
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
	public IntegerExpression expression(Number object) {
		if (object == null) {
			return nullExpression();
		} else {
			return value(object.longValue());
		}
	}

	/**
	 * Converts the integer expression into a string/character expression within
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
		return new StringResultFunction(this);
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
	 * @return the number of digits used in the integer part of this number.
	 */
	public IntegerExpression lengthOfIntegerPart() {
		return this.is(0).ifThenElse(value(1), this.abs().logBase10().integerPart().plus(1));
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
	public BooleanExpression isShorterThanOrAsLongAs(int maxIntegerLength, int maxDecimals) {
		return this.lengthOfIntegerPart().isLessThanOrEqual(maxIntegerLength);
	}

	/**
	 * Converts the integer expression to a string and appends the supplied
	 * String.
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
	 * Converts the integer expression to a string and appends the supplied
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
	 * Tests the IntegerExpression against the supplied integer.
	 *
	 * @param number the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression is(Number number) {
		if (number == null) {
			return isNull();
		} else {
			return is(value(number));
		}
	}

	/**
	 * Tests the IntegerExpression against the supplied integer.
	 *
	 * @param number the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression is(Integer number) {
		if (number == null) {
			return isNull();
		} else {
			return is(value(number));
		}
	}

	/**
	 * Tests the IntegerExpression against the supplied integer.
	 *
	 * @param number the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression is(Long number) {
		if (number == null) {
			return isNull();
		} else {
			return is(value(number));
		}
	}

	/**
	 * Tests the IntegerExpression against the supplied integer.
	 *
	 * @param number the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression is(NumberResult number) {
		return is(expression(number));
	}

	/**
	 * Tests the IntegerExpression against the supplied IntegerExpression.
	 *
	 * @param integerExpression the expression needs to evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression is(IntegerResult integerExpression) {
		return new IsFunction(this, integerExpression);
	}

	/**
	 * Tests the IntegerExpression to see if the result is an even number.
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
	 * Tests the IntegerExpression to see if the result is an odd number.
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
	 * Tests the IntegerExpression against the value NULL and returns true if the
	 * Integer Expression is not NULL.
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
	 * Tests the IntegerExpression against the value NULL and returns true if the
	 * Integer Expression is NULL.
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
	 * Tests the IntegerExpression against the supplied number and returns true if
	 * the Integer Expression is not equal to the number.
	 *
	 * @param number the expression needs to NOT evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression isNot(Integer number) {
		return is(value(number)).not();
	}

	/**
	 * Tests the IntegerExpression against the {@link IntegerResult} and returns
	 * true if the IntegerExpression is not equal to the number.
	 *
	 * @param number the expression needs to NOT evaluate to this number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	@Override
	public BooleanExpression isNot(IntegerResult number) {
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
	public BooleanExpression isBetween(IntegerResult lowerBound, IntegerResult upperBound) {
		return BooleanExpression.allOf(
				this.numberResult().isGreaterThan(lowerBound),
				this.numberResult().isLessThanOrEqual(upperBound)
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
	public BooleanExpression isBetween(IntegerResult lowerBound, Number upperBound) {
		return BooleanExpression.allOf(
				this.integerResult().isGreaterThan(lowerBound),
				this.integerResult().isLessThanOrEqual(upperBound)
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
	public BooleanExpression isBetween(Number lowerBound, IntegerResult upperBound) {
		return BooleanExpression.allOf(
				this.integerResult().isGreaterThan(lowerBound),
				this.integerResult().isLessThanOrEqual(upperBound)
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
	 * @param lowerbound the smallest value
	 * @param upperbound the largest value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(int lowerbound, int upperbound) {
		return isBetween(value(lowerbound), value(upperbound));
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
	public BooleanExpression isBetween(IntegerResult lowerBound, NumberResult upperBound) {
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
	public BooleanExpression isBetween(Long lowerBound, IntegerResult upperBound) {
		return isBetween(value(lowerBound), value(upperBound));
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
	public BooleanExpression isBetween(IntegerResult lowerBound, Long upperBound) {
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
	@Override
	public BooleanExpression isBetween(Long lowerBound, Long upperBound) {
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
	public BooleanExpression isBetweenInclusive(Long lowerBound, IntegerResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(Integer lowerBound, IntegerResult upperBound) {
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
	@Override
	public BooleanExpression isBetweenInclusive(IntegerResult lowerBound, Long upperBound) {
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
	public BooleanExpression isBetweenInclusive(IntegerResult lowerBound, Integer upperBound) {
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
	@Override
	public BooleanExpression isBetweenInclusive(Long lowerBound, Long upperBound) {
		return isBetweenInclusive(value(lowerBound), value(upperBound));
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
	public BooleanExpression isBetweenInclusive(Integer lowerBound, Integer upperBound) {
		return isBetweenInclusive(value(lowerBound), value(upperBound));
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
	public BooleanExpression isBetweenInclusive(IntegerResult lowerBound, IntegerResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(NumberResult lowerBound, NumberResult upperBound) {
		return BooleanExpression.allOf(
				this.numberResult().isGreaterThanOrEqual(lowerBound),
				this.numberResult().isLessThanOrEqual(upperBound)
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
	@Override
	public BooleanExpression isBetweenExclusive(IntegerResult lowerBound, IntegerResult upperBound) {
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
	@Override
	public BooleanExpression isBetweenExclusive(Long lowerBound, IntegerResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(Number lowerBound, SimpleNumericResult<?> upperBound) {
		return numberResult().isBetweenExclusive(
				value(lowerBound),
				new IntegerExpression(upperBound).numberResult());
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
	public BooleanExpression isBetweenExclusive(NumberResult lowerBound, Number upperBound) {
		return numberResult().isBetweenExclusive(
				NumberExpression.value(lowerBound),
				NumberExpression.value(upperBound));
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
	public BooleanExpression isBetweenExclusive(IntegerResult lowerBound, Long upperBound) {
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
	public BooleanExpression isBetweenExclusive(IntegerResult lowerBound, Integer upperBound) {
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
	@Override
	public BooleanExpression isBetweenExclusive(Long lowerBound, Long upperBound) {
		return isBetweenExclusive(value(lowerBound), value(upperBound));
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
	public BooleanExpression isBetweenExclusive(Number lowerBound, Number upperBound) {
		return numberResult().isBetweenExclusive(value(lowerBound), value(upperBound));
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
	public BooleanExpression isBetweenExclusive(Number lowerBound, NumberResult upperBound) {
		return numberResult().isBetweenExclusive(value(lowerBound), value(upperBound));
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is less than number.
	 *
	 * @param number need to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isLessThan(Integer number) {
		return isLessThan(value(number));
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is less than number.
	 *
	 * @param number need to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isLessThan(Number number) {
		return isLessThan(value(number));
	}

	/**
	 * Tests the IntegerExpression against the {@link IntegerResult} and returns
	 * TRUE if the value is less than the value supplied.
	 *
	 * @param numberExpression needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isLessThan(IntegerResult numberExpression) {
		return new IsLessThanFunction(this, numberExpression);
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is less than or equal to number.
	 *
	 * @param number needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isLessThanOrEqual(Number number) {
		return isLessThanOrEqual(value(number));
	}

	/**
	 * Tests the IntegerExpression against the {@link IntegerResult} and returns
	 * TRUE if the value is less than the value supplied.
	 *
	 * @param numberExpression needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isLessThan(NumberResult numberExpression) {
		return this.numberResult().isLessThan(numberExpression);
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is less than or equal to numberExpression.
	 *
	 * @param numberExpression needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(IntegerResult numberExpression) {
		return new IsLessThanOrEqualFunction(this, numberExpression);
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is less than or equal to numberExpression.
	 *
	 * @param numberExpression needs to be smaller than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isLessThanOrEqual(NumberResult numberExpression) {
		return this.numberResult().isLessThanOrEqual(numberExpression);
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is greater than number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThan(Long number) {
		return isGreaterThan(value(number));
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is greater than number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isGreaterThan(Number number) {
		return numberResult().isGreaterThan(value(number));
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is greater than number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThan(IntegerResult number) {
		return new IsGreaterThanFunction(this, number);
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is greater than or equal to number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(Long number) {
		return isGreaterThanOrEqual(value(number));
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is greater than or equal to number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isGreaterThanOrEqual(Integer number) {
		return isGreaterThanOrEqual(value(number));
	}

	/**
	 * Tests the IntegerExpression against the number and returns TRUE if the
	 * value is greater than or equal to number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(IntegerResult number) {
		return new IsGreaterThanOrEqualFunction(this, number);
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
	public BooleanExpression isLessThan(Integer value, BooleanExpression fallBackWhenEquals) {
		return this.isLessThan(IntegerExpression.value(value), fallBackWhenEquals);
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
	public BooleanExpression isGreaterThan(Long value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(IntegerExpression.value(value), fallBackWhenEquals);
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
	public BooleanExpression isGreaterThan(Integer value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(IntegerExpression.value(value), fallBackWhenEquals);
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
	public BooleanExpression isLessThan(IntegerResult value, BooleanExpression fallBackWhenEquals) {
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
	public BooleanExpression isGreaterThan(IntegerResult value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Compares the IntegerExpression against the list of possible values and
	 * returns true if the IntegerExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isIn(Integer... possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Integer num : possibleValues) {
			if (num == null) {
				possVals.add(null);
			} else {
				possVals.add(value(num));
			}
		}
		return isIn(possVals.toArray(new IntegerExpression[]{}));
	}

	/**
	 * Compares the IntegerExpression against the list of possible values and
	 * returns true if the IntegerExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isIn(Long... possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Long num : possibleValues) {
			if (num == null) {
				possVals.add(null);
			} else {
				possVals.add(value(num));
			}
		}
		return isIn(possVals.toArray(new IntegerExpression[]{}));
	}

	/**
	 * Compares the IntegerExpression against the list of possible values and
	 * returns true if the IntegerExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isIn(Collection<? extends Number> possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Number num : possibleValues) {
			possVals.add(value(num.longValue()));
		}
		return isIn(possVals.toArray(new IntegerExpression[]{}));
	}

	/**
	 * Compares the IntegerExpression against the list of possible values and
	 * returns true if the IntegerExpression is represented in the list.
	 *
	 * @param possibleValues needs to be one of these
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	@Override
	public BooleanExpression isIn(IntegerResult... possibleValues) {
		BooleanExpression isinExpr = new IsInFunction(this, possibleValues);
		if (isinExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isinExpr);
		} else {
			return new BooleanExpression(isinExpr);
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
	public static IntegerExpression leastOf(Integer... possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Integer num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new IntegerExpression[]{}));
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
	public static IntegerExpression leastOf(Long... possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Long num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new IntegerExpression[]{}));
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
	public static IntegerExpression leastOf(Number... possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Number num : possibleValues) {
			possVals.add(value(num).integerResult());
		}
		return leastOf(possVals.toArray(new IntegerExpression[]{}));
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
	public static IntegerExpression leastOf(Collection<? extends IntegerResult> possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (IntegerResult num : possibleValues) {
			possVals.add(new IntegerExpression(num));
		}
		return leastOf(possVals.toArray(new IntegerExpression[]{}));
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
	public static IntegerExpression leastOf(IntegerResult... possibleValues) {
		return new LeastOfFunction(possibleValues);
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
	public static IntegerExpression greatestOf(Integer... possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Integer num : possibleValues) {
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
	public static IntegerExpression greatestOf(Number... possibleValues) {
		List<IntegerExpression> possVals = new ArrayList<>();
		for (Number num : possibleValues) {
			possVals.add(value(num).integerResult());
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
	public static IntegerExpression greatestOf(Collection<? extends IntegerResult> possibleValues) {
		return greatestOf(possibleValues.toArray(new IntegerResult[]{}));
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
	public static IntegerExpression greatestOf(IntegerResult... possibleValues) {
		return new GreatestOfFunction(possibleValues);
	}

	/**
	 * Provides a default option when the IntegerExpression resolves to NULL
	 * within the query.
	 *
	 * @param alternative used if the expression is NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression that will substitute the given value when the
	 * IntegerExpression resolves to NULL.
	 */
	public IntegerExpression ifDBNull(Integer alternative) {
		return ifDBNull(IntegerExpression.value(alternative));
	}

	/**
	 * Provides a default option when the IntegerExpression resolves to NULL
	 * within the query.
	 *
	 * @param alternative used if the expression is NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression that will substitute the given value when the
	 * IntegerExpression resolves to NULL.
	 */
	public IntegerExpression ifDBNull(Long alternative) {
		return ifDBNull(IntegerExpression.value(alternative));
	}

	/**
	 * Provides a default option when the IntegerExpression resolves to NULL
	 * within the query.
	 *
	 * @param alternative used if the expression is NULL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression that will substitute the given value when the
	 * IntegerExpression resolves to NULL.
	 */
	public IntegerExpression ifDBNull(IntegerResult alternative) {
		return new IfDBNullFunction(this, alternative);
	}

	/**
	 * Adds an explicit bracket at this point in the expression chain.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression that will have the existing IntegerExpression
	 * wrapped in brackets..
	 */
	public IntegerExpression bracket() {
		return new BracketUnaryFunction(this);
	}

	/**
	 * Provides access to the exponential function.
	 *
	 * <p>
	 * Raises the E (2.718281828) to the power of the current IntegerExpression.
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
		return this.numberResult().exp();
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
	 * @return a IntegerExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression cos() {
		return this.numberResult().cos();
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
	 * @return a IntegerExpression representing the hyperbolic cosine of the
	 * current number expression.
	 */
	public NumberExpression cosh() {
		return this.numberResult().cosh();
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
	 * @return a IntegerExpression representing the sine of the current number
	 * expression.
	 */
	public NumberExpression sine() {
		return this.numberResult().sine();
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
	 * @return a IntegerExpression representing the hyperbolic sine of the current
	 * number expression.
	 */
	public NumberExpression sinh() {
		return this.numberResult().sinh();
	}

	@Override
	public NumberExpression numberResult() {
		return new NumberExpression(new NumberResultFunction(this));
	}

	@Override
	public IntegerResult expression(DBInteger value) {
		return new IntegerExpression(value);
	}

	@Override
	public IntegerExpression integerResult() {
		return this;
	}

	BooleanExpression isBetweenInclusive(Number lowerbound, Number upperbound) {
		return this.isBetweenInclusive(value(lowerbound), value(upperbound));
	}

	@Override
	public BooleanExpression isNot(Long integerValue) {
		return this.isNot(expression(integerValue));
	}

	public static class SinhFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		private final IntegerExpression limitedTo700;

		public SinhFunction(IntegerExpression only) {
			super(only);
			limitedTo700 = only.isGreaterThan(700).ifThenElse(nullExpression(), only);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsHyperbolicFunctionsNatively()) {
				return this.beforeValue(db) + (limitedTo700 == null ? "" : limitedTo700.toSQLString(db)) + this.afterValue(db);
			} else {
				IntegerExpression first = this.limitedTo700;
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
	 * @return a IntegerExpression representing the tangent of the current number
	 * expression.
	 */
	public NumberExpression tan() {
		return this.numberResult().tan();
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
	 * @return a IntegerExpression representing the hyperbolic tangent of the
	 * current number expression.
	 */
	public NumberExpression tanh() {
		return this.numberResult().tanh();
	}

	/**
	 * Provides access to the database's absolute value function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression representing the absolute value of the current
	 * number expression.
	 */
	public IntegerExpression abs() {
		return new IntegerExpression(new AbsoluteValueFunction(this));
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
	 * @return a IntegerExpression representing the absolute value of the current
	 * number expression.
	 */
	public IntegerExpression absoluteValue() {
		return abs();
	}

	/**
	 * Provides access to the database's inverse cosine function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression representing the inverse cosine of the current
	 * number expression.
	 */
	public NumberExpression arccos() {
		return new NumberExpression(new ArcCosineFunction(this));
	}

	/**
	 * Provides access to the database's inverse sine function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression representing the inverse sine of the current
	 * number expression.
	 */
	public NumberExpression arcsin() {
		return this.numberResult().arcsin();
	}

	/**
	 * Provides access to the database's inverse tangent function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression representing the inverse tangent of the current
	 * number expression.
	 */
	public NumberExpression arctan() {
		return this.numberResult().arctan();
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
	 * @return a IntegerExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression arctan2(IntegerExpression number) {
		return arctan2(number.numberResult());
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
	 * @return a IntegerExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression arctan2(Number number) {
		return arctan2(NumberExpression.value(number));
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
	 * @return a IntegerExpression representing the cosine of the current number
	 * expression.
	 */
	public NumberExpression arctan2(NumberExpression number) {
		return this.numberResult().arctan2(number);
	}

	/**
	 * Provides access to the database's cotangent function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression representing the cotangent of the current
	 * number expression.
	 */
	public NumberExpression cotangent() {
		return this.numberResult().cotangent();
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
	 * @return a IntegerExpression representing the inverse tangent of the current
	 * number expression.
	 */
	public NumberExpression degrees() {
		return this.numberResult().degrees();
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
	 * @return a IntegerExpression representing the inverse tangent of the current
	 * number expression.
	 */
	public NumberExpression radians() {
		return this.numberResult().radians();
	}

	/**
	 * returns the Natural Logarithm of the current IntegerExpression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NimberExpression of the natural logarithm of the current
	 * expression.
	 */
	public NumberExpression logN() {
		return this.numberResult().logN();
	}

	/**
	 * returns the Logarithm Base-10 of the current IntegerExpression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NimberExpression of the logarithm base-10 of the current
	 * expression.
	 */
	public NumberExpression logBase10() {
		return this.numberResult().logBase10();
	}

	/**
	 * Provides access to the power (or pow) function of the database.
	 *
	 * <p>
	 * For a IntegerExpression x then x.power(n) =&gt; x^n.
	 *
	 * @param n	n
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression power(IntegerExpression n) {
		return new IntegerExpression(new PowerFunction(this, n));
	}

	/**
	 * Provides access to the power (or pow) function of the database.
	 *
	 * <p>
	 * For a IntegerExpression x then x.power(n) =&gt; x^n.
	 *
	 * @param n	n
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression power(Integer n) {
		return power(n.longValue());
	}

	/**
	 * Provides access to the power (or pow) function of the database.
	 *
	 * <p>
	 * For a IntegerExpression x then x.power(n) =&gt; x^n.
	 *
	 * @param n	n
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression power(Long n) {
		return power(value(n));
	}

	/**
	 * Provides access to the power (or pow) function of the database.
	 *
	 * <p>
	 * For a IntegerExpression x then x.power(n) =&gt; x^n.
	 *
	 * @param n	n
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression power(Number n) {
		return this.numberResult().power(NumberExpression.value(n));
	}

	/**
	 * Provides access to the power (or pow) function of the database.
	 *
	 * <p>
	 * For a IntegerExpression x then x.power(n) =&gt; x^n.
	 *
	 * @param n	n
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression power(NumberResult n) {
		return this.numberResult().power(n);
	}

	/**
	 * Returns the sign of the argument as -1, 0, or 1, depending on whether X is
	 * negative, zero, or positive.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression
	 */
	public IntegerExpression sign() {
		return new IntegerExpression(new SignFunction(this));
	}

	/**
	 * Returns the square root of a nonnegative number X.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression
	 */
	public NumberExpression squareRoot() {
		return this.numberResult().squareRoot();
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
	public IntegerExpression round(Number decimalPlaces) {
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
	public IntegerExpression round(IntegerResult decimalPlaces) {
		return round(expression(decimalPlaces));
	}

	/**
	 * Implements support for rounding to an arbitrary number of decimal places.
	 *
	 * <p>
	 * For instance if you require numbers like 12.345 you should use .round(3) to
	 * get the 3 digits after the decimal point.
	 *
	 * <p>
	 * Round supports negative decimal places: use round(-2) to change 1234.56 to
	 * 1200</p>
	 *
	 * @param decimalPlaces the number of significant places that are required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the equation rounded to the nearest integer.
	 */
	public IntegerExpression round(IntegerExpression decimalPlaces) {
		return new IntegerExpression(new RoundToNumberOfDecimalPlacesFunction(this, decimalPlaces));
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression minus(IntegerExpression number) {
		return new IntegerExpression(new MinusBinaryArithmetic(this, number));
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression minus(NumberExpression number) {
		return this.numberResult().minus(number);
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression minus(Integer num) {
		return minus(num.longValue());
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression minus(Long num) {
		final IntegerExpression minusThisExpression = new IntegerExpression(num);
		final DBBinaryArithmetic minusExpression = new MinusBinaryArithmetic(this, minusThisExpression);
		return new IntegerExpression(minusExpression);
	}

	/**
	 * Provides access to the basic arithmetic operation minus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.minus(y) =&gt; x - y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression minus(Number num) {
		return this.minus(NumberExpression.value(num));
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression plus(IntegerResult number) {
		return new IntegerExpression(new PlusFunction(this, new IntegerExpression(number)));
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression plus(NumberResult num) {
		return this.numberResult().plus(num);
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression plus(Number num) {
		return this.numberResult().plus(num);
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression plus(Long num) {
		return plus(value(num));
	}

	/**
	 * Provides access to the basic arithmetic operation plus.
	 *
	 * <p>
	 * For a IntegerExpression x: x.plus(y) =&gt; x + y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression plus(Integer num) {
		return plus(value(num.longValue()));
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a IntegerExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression times(IntegerResult number) {
		return new IntegerExpression(new TimesFunction(this, new IntegerExpression(number)));
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a IntegerExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression times(Number num) {
		return this.numberResult().times(num);
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a IntegerExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression times(Integer num) {
		return this.times(value(num));
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a IntegerExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression times(Long num) {
		return this.times(value(num));
	}

	/**
	 * Provides access to the basic arithmetic operation multiply/times.
	 *
	 * <p>
	 * For a IntegerExpression x: x.times(y) =&gt; x * y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression times(NumberResult num) {
		return this.numberResult().times(num);
	}

	/**
	 * Provides access to the basic arithmetic operation divide.
	 *
	 * <p>
	 * For a IntegerExpression x: x.dividedBy(y) =&gt; x / y.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public NumberExpression dividedBy(IntegerResult number) {
		return new NumberExpression(this).dividedBy(number);
	}

	/**
	 * Division as represent by x/y.
	 *
	 * <p>
	 * For a IntegerExpression x: x.dividedBy(y) =&gt; x / y.
	 *
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression of a division operation.
	 */
	public NumberExpression dividedBy(Number num) {
		return this.numberResult().dividedBy(num);
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
	 * @return a IntegerExpression of a Modulus operation.
	 */
	public IntegerExpression mod(IntegerResult number) {
		return new IntegerExpression(new ModulusFunction(this, expression(number)));
	}

	/**
	 * /**
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
	 * @return a IntegerExpression
	 */
	public IntegerExpression mod(Number num) {
		return this.mod(expression(num));
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
	public IntegerExpression modeSimple() {
		IntegerExpression modeExpr
				= new IntegerExpression(
						new ModeSimpleExpression<>(this)
				);
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
	public IntegerExpression modeStrict() {
		IntegerExpression modeExpr
				= new IntegerExpression(
						new ModeStrictExpression<>(this));
		return modeExpr;
	}

	/**
	 * Creates an expression that will return the median value of the column
	 * supplied.
	 *
	 * <p>
	 * MEDIAN: denoting or relating to a value or quantity lying at the midpoint
	 * of a frequency distribution of observed values or quantities, such that
	 * there is an equal probability of falling above or below it. For example: in
	 * {9,9,7,6,5,3,3} the median is 6.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the median or null if undefined.
	 */
	public IntegerExpression median() {
		IntegerExpression medianExpr
				= new IntegerExpression(
						new MedianExpression<Long, IntegerResult, DBInteger, IntegerExpression>(this));
		return medianExpr;
	}

	/**
	 * Creates an expression that will return the uniqueRanking value of the
	 * column supplied.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the mode or null if undefined.
	 */
	public IntegerExpression uniqueRanking() {
		IntegerExpression medianExpr
				= new IntegerExpression(
						new UniqueRankingExpression<Long, IntegerResult, DBInteger, IntegerExpression>(this));
		return medianExpr;
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
	 * Value 0 returns the first string, value 1 returns the second, etc. If the
	 * index is too large NULL is returned.
	 *
	 * @param stringsToChooseFrom a list of values that the should replace the
	 * number.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL that selects the string from the list based on this expression.
	 */
	public StringExpression choose(StringResult... stringsToChooseFrom) {
		StringExpression leastExpr
				= new StringExpression(new ChooseFunction(this, stringsToChooseFrom));
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
		return this.numberResult().average();
	}

	/**
	 * Synonym for {@link IntegerExpression#standardDeviation() }.
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
	 * Synonym for {@link IntegerExpression#standardDeviation() }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A number expression representing the standard deviation of the
	 * grouped rows.
	 */
	public NumberExpression standardDeviation() {
		return this.numberResult().standardDeviation();
	}

	/**
	 * Returns the greatest/largest value from the column.
	 *
	 * <p>
	 * Similar to
	 * {@link #greatestOf(nz.co.gregs.dbvolution.results.IntegerResult...) } but
	 * this aggregates the column or expression provided, rather than scanning a
	 * list.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the greatest/largest value from the column.
	 */
	public IntegerExpression max() {
		return new IntegerExpression(new MaxUnaryFunction(this));
	}

	/**
	 * Returns the least/smallest value from the column.
	 *
	 * <p>
	 * Similar to {@link #leastOf(nz.co.gregs.dbvolution.results.IntegerResult...)
	 * } but this aggregates the column or expression provided, rather than
	 * scanning a list.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the least/smallest value from the column.
	 */
	public IntegerExpression min() {
		return new IntegerExpression(new MinUnaryFunction(this));
	}

	/**
	 * Aggregator that sum all the values from the column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the sum of all the values from the column.
	 */
	public IntegerExpression sum() {
		return new IntegerExpression(new SumFunction(this));
	}

	@Override
	public DBInteger getQueryableDatatypeForExpressionValue() {
		return new DBInteger();
	}

	@Override
	public boolean isAggregator() {
		final AnyResult<?> innerResult = getInnerResult();
		return getInnerResult() == null ? false : innerResult.isAggregator();
	}

	/**
	 * Multiples this expression by itself to return the value squared.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression
	 */
	public IntegerExpression squared() {
		return this.bracket().times(this.bracket());
	}

	/**
	 * Multiples this expression by its square to return the value cubed.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression
	 */
	public IntegerExpression cubed() {
		return this.squared().times(this.bracket());
	}

	@Override
	public DBInteger asExpressionColumn() {
		return new DBInteger(this);
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

	/**
	 * Returns TRUE for all zero or positive numbers and FALSE for all negative
	 * numbers.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return BooleanExpression
	 */
	public BooleanExpression isPositive() {
		return this.isGreaterThanOrEqual(0);
	}

	/**
	 * Returns FALSE for all zero or positive numbers and TRUE for all negative
	 * numbers.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNegative() {
		return this.isGreaterThanOrEqual(0);
	}

	/**
	 * Returns TRUE for zero and FALSE for all other values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression that is either "+" or "-"
	 */
	public BooleanExpression isZero() {
		return this.is(0).isNotNull();
	}

	/**
	 * Returns FALSE for zero and FALSE for all other values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression that is either "+" or "-"
	 */
	public BooleanExpression isNotZero() {
		return this.isIn(0, null).not();
	}

	private static abstract class DBBinaryArithmetic extends IntegerExpression {

		private static final long serialVersionUID = 1L;

		public IntegerResult first;
		public IntegerResult second;

		DBBinaryArithmetic() {
			this.first = null;
			this.second = null;
		}

		DBBinaryArithmetic(IntegerResult first, IntegerResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBInteger getQueryableDatatypeForExpressionValue() {
			return new DBInteger();
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

	private static abstract class DBUnaryFunction extends IntegerExpression {

		private static final long serialVersionUID = 1L;

		DBUnaryFunction() {
			super();
		}

		DBUnaryFunction(IntegerExpression only) {
			super(only);
		}

		DBUnaryFunction(AnyExpression<?, ?, ?> only) {
			super(only);
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
			final AnyResult<?> only = getInnerResult();
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}
	}

	private static abstract class DBUnaryNumberFunction extends NumberExpression {

		private final static long serialVersionUID = 1l;

		DBUnaryNumberFunction() {
			super();
		}

		DBUnaryNumberFunction(IntegerExpression only) {
			super(only);
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
			final AnyResult<?> only = getInnerResult();
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public boolean isPurelyFunctional() {
			final AnyResult<?> only = getInnerResult();
			if (only == null) {
				return true;
			} else {
				return only.isPurelyFunctional();
			}
		}
	}

	private static abstract class IntegerIntegerFunctionIntegerResult extends IntegerExpression {

		private final static long serialVersionUID = 1l;

		protected IntegerExpression first;
		protected IntegerExpression second;

		IntegerIntegerFunctionIntegerResult(IntegerExpression first) {
			this.first = first;
			this.second = null;
		}

		IntegerIntegerFunctionIntegerResult(IntegerExpression first, IntegerExpression second) {
			this.first = first;
			this.second = second;
		}

		IntegerIntegerFunctionIntegerResult(IntegerExpression first, IntegerResult second) {
			this.first = first;
			this.second = IntegerExpression.value(second);
		}

		@Override
		public DBInteger getQueryableDatatypeForExpressionValue() {
			return new DBInteger();
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
		protected IntegerExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected IntegerExpression getSecond() {
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

		private final static long serialVersionUID = 1l;

		private IntegerExpression first;
		private IntegerResult second;
		private boolean requiresNullProtection;

		DBBinaryBooleanArithmetic(IntegerExpression first, IntegerResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		IntegerExpression getFirst() {
			return first;
		}

		IntegerResult getSecond() {
			return second;
		}

		@Override
		public boolean isBooleanStatement() {
			return true;
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

		private final static long serialVersionUID = 1l;

		private IntegerExpression column;
		private final List<IntegerResult> values = new ArrayList<>();
		boolean nullProtectionRequired = false;

		DBNnaryBooleanFunction() {
		}

		DBNnaryBooleanFunction(IntegerExpression leftHandSide, IntegerResult[] rightHandSide) {
			this.column = leftHandSide;
			for (IntegerResult numberResult : rightHandSide) {
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
			for (IntegerResult val : getValues()) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (getColumn() != null) {
				hashSet.addAll(getColumn().getTablesInvolved());
			}
			for (IntegerResult second : getValues()) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = getColumn().isAggregator();
			for (IntegerResult numer : getValues()) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean isPurelyFunctional() {
			boolean result = getColumn().isPurelyFunctional();
			for (IntegerResult numer : getValues()) {
				result = result && numer.isPurelyFunctional();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the column
		 */
		protected IntegerExpression getColumn() {
			return column;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the values
		 */
		protected List<IntegerResult> getValues() {
			return values;
		}
	}

	private static abstract class DBNnaryIntegerFunction extends IntegerExpression {

		private final static long serialVersionUID = 1l;

		protected IntegerExpression column;
		protected final List<IntegerResult> values = new ArrayList<>();
		boolean nullProtectionRequired = false;

		DBNnaryIntegerFunction() {
		}

		DBNnaryIntegerFunction(IntegerResult[] rightHandSide) {
			for (IntegerResult numberResult : rightHandSide) {
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
		public DBInteger getQueryableDatatypeForExpressionValue() {
			return new DBInteger();
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
			for (IntegerResult val : values) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public DBNnaryIntegerFunction copy() {
			DBNnaryIntegerFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			for (IntegerResult value : this.values) {
				newInstance.values.add(value.copy());
			}
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			for (IntegerResult second : values) {
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
			for (IntegerResult numer : values) {
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
				for (IntegerResult value : values) {
					result &= value.isPurelyFunctional();
				}
				return result;
			}
		}
	}

	private static abstract class DBIntegerAndNnaryStringFunction extends StringExpression {

		private final static long serialVersionUID = 1l;

		protected IntegerResult numberExpression = null;
		protected final List<StringResult> values = new ArrayList<>();
		boolean nullProtectionRequired = false;

		DBIntegerAndNnaryStringFunction() {
		}

		DBIntegerAndNnaryStringFunction(IntegerResult numberResult, StringResult[] rightHandSide) {
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
		public DBIntegerAndNnaryStringFunction copy() {
			DBIntegerAndNnaryStringFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.numberExpression = this.numberExpression.copy();
			for (StringResult value : this.values) {
				newInstance.values.add(value.copy());
			}
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
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

		private final static long serialVersionUID = 1l;

		protected IntegerExpression only;

		DBUnaryStringFunction() {
			this.only = null;
		}

		DBUnaryStringFunction(IntegerExpression only) {
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

		MaxUnaryFunction(IntegerExpression only) {
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

		@Override
		public MaxUnaryFunction copy() {
			return new MaxUnaryFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private static class MinUnaryFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		MinUnaryFunction(IntegerExpression only) {
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

		@Override
		public MinUnaryFunction copy() {
			return new MinUnaryFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private static class MinusBinaryArithmetic extends DBBinaryArithmetic {

		private final static long serialVersionUID = 1l;

		MinusBinaryArithmetic(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " - ";
		}

		@Override
		public MinusBinaryArithmetic copy() {
			return new MinusBinaryArithmetic(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private static class BracketUnaryFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		BracketUnaryFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		public BracketUnaryFunction copy() {
			return new BracketUnaryFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private class StringResultFunction extends DBUnaryStringFunction {

		private final static long serialVersionUID = 1l;

		public StringResultFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIntegerToStringTransform(super.only.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean getIncludesNull() {
			return only.getIncludesNull();
		}

		@Override
		public StringResultFunction copy() {
			return new StringResultFunction(
					only == null ? null : only.copy()
			);
		}
	}

	private class IsFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (super.getIncludesNull()) {
				return BooleanExpression.isNull(getFirst()).toSQLString(db);
			} else {
				return db.doIntegerEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " = ";
		}

		@Override
		public IsFunction copy() {
			return new IsFunction(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class IsLessThanFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsLessThanFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " < ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public IsLessThanFunction copy() {
			return new IsLessThanFunction(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class IsLessThanOrEqualFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsLessThanOrEqualFunction(IntegerExpression first, IntegerResult second) {
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

		@Override
		public IsLessThanOrEqualFunction copy() {
			return new IsLessThanOrEqualFunction(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class IsGreaterThanFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsGreaterThanFunction(IntegerExpression first, IntegerResult second) {
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

		@Override
		public IsGreaterThanFunction copy() {
			return new IsGreaterThanFunction(
					getSecond() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class IsGreaterThanOrEqualFunction extends DBBinaryBooleanArithmetic {

		private final static long serialVersionUID = 1l;

		public IsGreaterThanOrEqualFunction(IntegerExpression first, IntegerResult second) {
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

		@Override
		public IsGreaterThanOrEqualFunction copy() {
			return new IsGreaterThanOrEqualFunction(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class IsInFunction extends DBNnaryBooleanFunction {

		private final static long serialVersionUID = 1l;

		public IsInFunction(IntegerExpression leftHandSide, IntegerResult[] rightHandSide) {
			super(leftHandSide, rightHandSide);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<>();
			for (IntegerResult value : this.getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.doInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return " IN ";
		}

		@Override
		public IsInFunction copy() {
			List<IntegerResult> newValues = new ArrayList<>();
			for (IntegerResult num : this.getValues()) {
				newValues.add(num == null ? null : num.copy());
			}
			return new IsInFunction(
					getColumn() == null ? null : getColumn().copy(),
					newValues.toArray(new IntegerResult[]{})
			);
		}
	}

	private static class LeastOfFunction extends DBNnaryIntegerFunction {

		private final static long serialVersionUID = 1l;

		public LeastOfFunction(IntegerResult[] rightHandSide) {
			super(rightHandSide);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<>();
			for (IntegerResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doLeastOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getLeastOfFunctionName();
		}

		@Override
		public LeastOfFunction copy() {
			List<IntegerResult> newValues = new ArrayList<>();
			for (IntegerResult num : this.values) {
				newValues.add(num == null ? null : num.copy());
			}
			return new LeastOfFunction(
					newValues.toArray(new IntegerResult[]{})
			);
		}
	}

	private static class GreatestOfFunction extends DBNnaryIntegerFunction {

		private final static long serialVersionUID = 1l;

		public GreatestOfFunction(IntegerResult[] rightHandSide) {
			super(rightHandSide);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<>();
			for (IntegerResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doGreatestOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return db.getGreatestOfFunctionName();
		}

		@Override
		public GreatestOfFunction copy() {
			List<IntegerResult> newValues = new ArrayList<>();
			for (IntegerResult num : this.values) {
				newValues.add(num == null ? null : num.copy());
			}
			return new GreatestOfFunction(
					newValues.toArray(new IntegerResult[]{})
			);
		}
	}

	private class IfDBNullFunction extends IntegerIntegerFunctionIntegerResult {

		private final static long serialVersionUID = 1l;

		public IfDBNullFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIntegerIfNullTransform(this.getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getIfNullFunctionName();
		}

		@Override
		public IfDBNullFunction copy() {
			return new IfDBNullFunction(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private class NumberResultFunction extends DBUnaryNumberFunction {

		private final static long serialVersionUID = 1l;

		public NumberResultFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doIntegerToNumberTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public NumberResultFunction copy() {
			return new NumberResultFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private static class AbsoluteValueFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		public AbsoluteValueFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "abs";
		}

		@Override
		public AbsoluteValueFunction copy() {
			return new AbsoluteValueFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private static class ArcCosineFunction extends DBUnaryNumberFunction {

		private final static long serialVersionUID = 1l;

		public ArcCosineFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "acos";
		}

		@Override
		public ArcCosineFunction copy() {
			return new ArcCosineFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private class ArcSineFunction extends DBUnaryNumberFunction {

		private final static long serialVersionUID = 1l;

		public ArcSineFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsArcSineFunction()) {
				return super.toSQLString(db);
			} else {
				IntegerExpression only = (IntegerExpression) getInnerResult();
				return only.numberResult().dividedBy(value(1.0).minus(only.times(only).bracket()).bracket().squareRoot()).arctan().toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "asin";
		}

		@Override
		public ArcSineFunction copy() {
			return new ArcSineFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private static class PowerFunction extends IntegerIntegerFunctionIntegerResult {

		private final static long serialVersionUID = 1l;

		public PowerFunction(IntegerExpression first, IntegerExpression second) {
			super(first, second);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "power";
		}

		@Override
		public PowerFunction copy() {
			return new PowerFunction(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private static class SignFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		public SignFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "sign";
		}

		@Override
		public SignFunction copy() {
			return new SignFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private class RoundToNumberOfDecimalPlacesFunction extends IntegerIntegerFunctionIntegerResult {

		private final static long serialVersionUID = 1l;

		public RoundToNumberOfDecimalPlacesFunction(IntegerExpression first, IntegerExpression second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			try {
				return db.doRoundWithDecimalPlacesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				IntegerExpression power = IntegerExpression.value(10).power(getSecond()).bracket();
				return getFirst()
						.times(power)
						.bracket()
						.numberResult()
						.trunc()
						.dividedBy(power).toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "round";
		}

		@Override
		public RoundToNumberOfDecimalPlacesFunction copy() {
			return new RoundToNumberOfDecimalPlacesFunction(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private static class PlusFunction extends DBBinaryArithmetic {

		private final static long serialVersionUID = 1l;

		public PlusFunction(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " + ";
		}

		@Override
		public PlusFunction copy() {
			return new PlusFunction(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private static class TimesFunction extends DBBinaryArithmetic {

		private final static long serialVersionUID = 1l;

		public TimesFunction(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " * ";
		}

		@Override
		public TimesFunction copy() {
			return new TimesFunction(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private class ModulusFunction extends IntegerIntegerFunctionIntegerResult {

		private final static long serialVersionUID = 1l;

		public ModulusFunction(IntegerExpression first, IntegerExpression second) {
			super(first, second);
		}

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

		@Override
		public ModulusFunction copy() {
			return new ModulusFunction(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	private class ChooseFunction extends DBIntegerAndNnaryStringFunction {

		private final static long serialVersionUID = 1l;

		public ChooseFunction(IntegerResult numberResult, StringResult[] rightHandSide) {
			super(numberResult, rightHandSide);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> strs = new ArrayList<>();
			for (StringResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.doChooseTransformation(IntegerExpression.value(numberExpression).plus(1).bracket().toSQLString(db), strs);
		}

		@Override
		public ChooseFunction copy() {
			StringResult[] newValues = new StringResult[values.size()];
			for (int i = 0; i < newValues.length; i++) {
				final StringResult got = values.get(i);
				newValues[i] = got == null ? null : got.copy();
			}
			return new ChooseFunction(
					numberExpression == null ? null : numberExpression.copy(),
					newValues);
		}
	}

	private static class SumFunction extends DBUnaryFunction {

		private final static long serialVersionUID = 1l;

		public SumFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getSumFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public SumFunction copy() {
			return new SumFunction(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	private static class NullExpression extends IntegerExpression {

		private final static long serialVersionUID = 1l;

		public NullExpression() {
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public NullExpression copy() {
			return new NullExpression();
		}
	}
}
