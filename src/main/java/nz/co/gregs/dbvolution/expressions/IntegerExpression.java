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

import nz.co.gregs.dbvolution.results.RangeComparable;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.BooleanResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.results.InComparable;
import nz.co.gregs.dbvolution.results.NumberResult;

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
 * Generally you get a IntegerExpression from a column or value using {@link IntegerExpression#IntegerExpression(java.lang.Integer)
 * } or {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBInteger) }.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class IntegerExpression implements IntegerResult, RangeComparable<IntegerResult>, InComparable<IntegerResult>, ExpressionColumn<DBInteger> {

	static IntegerExpression nullExpression() {
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
	public static IntegerExpression value(IntegerResult number) {
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
	public static IntegerExpression value(NumberResult number) {
		return new NumberExpression(number).trunc();
	}

	private IntegerResult innerIntegerResult;
	private boolean nullProtectionRequired;

	/**
	 * Default Constructor
	 *
	 */
	protected IntegerExpression() {
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
	public IntegerExpression(Integer value) {
		innerIntegerResult = new DBInteger(value);
		if (value == null || innerIntegerResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
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
	public IntegerExpression(Long value) {
		innerIntegerResult = new DBInteger(value);
		if (value == null || innerIntegerResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
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
		innerIntegerResult = value;
		if (value == null || innerIntegerResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return getInnerIntegerResult().toSQLString(db);
	}

	@Override
	public IntegerExpression copy() {
		return new IntegerExpression(getInnerIntegerResult());
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
	public static IntegerExpression value(Long object) {
		final IntegerExpression integerExpression = new IntegerExpression(object);
		if (object == null) {
			integerExpression.nullProtectionRequired = true;
		}
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
	public static IntegerExpression value(Number object) {
		if (object == null) {
			return nullExpression();
		} else {
			return value(object.longValue());
		}
	}

	@Override
	public boolean isPurelyFunctional() {
		if (innerIntegerResult == null) {
			return true;
		} else {
			return innerIntegerResult.isPurelyFunctional();
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
	public BooleanExpression is(NumberResult number) {
		return is(value(number));
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
	public BooleanExpression isBetween(Integer lowerBound, IntegerResult upperBound) {
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
	public BooleanExpression isBetween(IntegerResult lowerBound, Integer upperBound) {
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
	public BooleanExpression isBetween(Integer lowerBound, Integer upperBound) {
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
	public BooleanExpression isBetweenInclusive(Integer lowerBound, Integer upperBound) {
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
	public BooleanExpression isBetweenExclusive(NumberResult lowerBound, NumberResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(value(lowerBound)),
				this.isLessThan(value(upperBound))
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
	public BooleanExpression isBetweenExclusive(Integer lowerBound, IntegerResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(Integer lowerBound, Integer upperBound) {
		return isBetweenExclusive(value(lowerBound), value(upperBound));
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
	public BooleanExpression isLessThanOrEqual(Integer number) {
		return isLessThanOrEqual(value(number));
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
	 * value is greater than number.
	 *
	 * @param number needs to be greater than this
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression for use in
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)}
	 */
	public BooleanExpression isGreaterThan(Integer number) {
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
	public IntegerExpression exp() {
		return new ExpFunction(this);
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
	public IntegerExpression cos() {
		return new CosFunction(this);
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
	public IntegerExpression cosh() {
		return new CoshFunction(this);
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
	public IntegerExpression sine() {
		return new SineFunction(this);
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
	public IntegerExpression sinh() {
		return new SinhFunction(this);
	}

	public NumberExpression numberResult() {
		return new NumberExpression(new NumberResultFunction(this));
	}

	public static class SinhFunction extends DBUnaryFunction {

		public SinhFunction(IntegerExpression only) {
			this.only = only.isGreaterThan(700).ifThenElse(IntegerExpression.nullExpression(), only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsHyperbolicFunctionsNatively()) {
				return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
			} else {
				IntegerExpression first = this.only;
				//(e^x - e^-x)/2
				return first.exp().minus(first.times(-1).exp().bracket()).bracket().dividedBy(2).bracket()
						.toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
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
	public IntegerExpression tan() {
		return new IntegerExpression(new TanFunction(this));
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
	public IntegerExpression tanh() {
		return new IntegerExpression(new TanhFunction(this));
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
	public IntegerExpression arccos() {
		return new IntegerExpression(new ArcCosineFunction(this));
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
	public IntegerExpression arcsin() {
		return new IntegerExpression(new ArcSineFunction(this));
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
	public IntegerExpression arctan() {
		return new IntegerExpression(new ArcTangentFunction(this));
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
	public IntegerExpression arctan2(IntegerExpression number) {
		return new IntegerExpression(new ArcTan2Function(this, number));
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
	public IntegerExpression cotangent() {
		return new IntegerExpression(new CotangentFunction(this));
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
	public IntegerExpression degrees() {
		return new IntegerExpression(new DegreesFunction(this));
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
	public IntegerExpression radians() {
		return new IntegerExpression(new RadiansFunction(this));
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
	public IntegerExpression logN() {
		return new IntegerExpression(new NaturalLogarithmFunction(this));
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
	public IntegerExpression logBase10() {
		return new IntegerExpression(new LogBase10Function(this));
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
	 * Provides access to a random floating-point value x in the range 0 &lt;= x
	 * &lt; 1.0.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a IntegerExpression that provides a random number when used in a
	 * query.
	 */
	static public IntegerExpression random() {
		return new IntegerExpression(new RandomFunction());
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
	public IntegerExpression squareRoot() {
		return new IntegerExpression(new SquareRootFunction(this));
	}

	/**
	 * Implements support for CEIL().
	 *
	 * <p>
	 * Returns the smallest integer that is larger than the expression
	 *
	 * <p>
	 * Note:<br>
	 * (new DBInteger( 1.5)).ceil() == 2<br>
	 * (new DBInteger(-1.5)).ceil() == -1
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value of the equation rounded up to the nearest integer.
	 */
	public IntegerExpression roundUp() {
		return new IntegerExpression(new RoundUpFunction(this));
	}

	/**
	 * Implements support for ROUND()
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the equation rounded to the nearest integer.
	 */
	public IntegerExpression round() {
		return new IntegerExpression(new RoundFunction(this));
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
		return round(IntegerExpression.value(decimalPlaces));
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
		return round(IntegerExpression.value(decimalPlaces));
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
	public IntegerExpression round(IntegerExpression decimalPlaces) {
		return new IntegerExpression(new RoundToNumberOfDecimalPlacesFunction(this, IntegerExpression.value(decimalPlaces)));
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
	public IntegerExpression roundDown() {
		return new IntegerExpression(new RoundDownFunction(this));
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
		return new IntegerExpression(new TruncFunction(this));
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
	 * @return a IntegerExpression
	 */
	public IntegerExpression integerPart() {
		return this.trunc();
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
	 * @return a IntegerExpression
	 */
	public IntegerExpression floor() {
		return this.trunc();
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
	 * @param num	num
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a IntegerExpression
	 */
	public IntegerExpression minus(Integer num) {
		final IntegerExpression minusThisExpression = new IntegerExpression(num);
		final DBBinaryArithmetic minusExpression = new MinusBinaryArithmetic(this, minusThisExpression);
		return new IntegerExpression(minusExpression);
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
	public IntegerExpression plus(Number num) {
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
	public IntegerExpression times(Number num) {
		return times(value(num));
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
	public IntegerExpression dividedBy(IntegerResult number) {
		return new IntegerExpression(new DivideByFunction(this, value(number))
		);
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
	public NumberExpression dividedBy(Integer num) {
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
		return new IntegerExpression(new ModulusFunction(this, value(number)));
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
		return this.mod(value(num));
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
	 * index is too large the last string is returned.
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
	 * index is too large the last string is returned.
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
	public IntegerExpression average() {
		return new IntegerExpression(new AverageFunction(this));
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
	public IntegerExpression stddev() {
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
	public IntegerExpression standardDeviation() {
		return new IntegerExpression(new StandardDeviation(this));
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

	/**
	 * Aggregrator that counts all the rows of the query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the count of all the values from the column.
	 */
	public static IntegerExpression countAll() {
		return new IntegerExpression(new CountAllFunction());
	}

	/**
	 * Aggregrator that counts this row if the booleanResult is true.
	 *
	 * @param booleanResult an expression that will be TRUE when the row needs to
	 * be counted.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The number of rows where the test is true.
	 */
	public static IntegerExpression countIf(BooleanResult booleanResult) {
		return new BooleanExpression(booleanResult)
				.ifThenElse(value(1), value(0))
				.sum();
	}

	@Override
	public DBInteger getQueryableDatatypeForExpressionValue() {
		return new DBInteger();
	}

	@Override
	public boolean isAggregator() {
		return getInnerIntegerResult() == null ? false : getInnerIntegerResult().isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<>();
		if (getInnerIntegerResult() != null) {
			hashSet.addAll(getInnerIntegerResult().getTablesInvolved());
		}
		return hashSet;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the innerIntegerResult
	 */
	public IntegerResult getInnerIntegerResult() {
		return innerIntegerResult;
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
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

	private static abstract class DBBinaryArithmetic extends IntegerExpression {

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
		public String toSQLString(DBDatabase db) {
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

		protected abstract String getEquationOperator(DBDatabase db);

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

	private static abstract class DBNonaryFunction extends IntegerExpression {

		DBNonaryFunction() {
		}

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return " " + getFunctionName(db) + "";
		}

		protected String afterValue(DBDatabase db) {
			return " ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
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

//		@Override
//		public QueryableDatatype getQueryableDatatypeForExpressionValue() {
//			return new DBInteger();
//		}
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

	private static abstract class DBUnaryFunction extends IntegerExpression {

		protected IntegerExpression only;

		DBUnaryFunction() {
			this.only = null;
		}

		DBUnaryFunction(IntegerExpression only) {
			this.only = only;
		}

//		DBUnaryFunction(DBExpression only) {
//			this.only = only;
//		}
		@Override
		public DBInteger getQueryableDatatypeForExpressionValue() {
			return new DBInteger();
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

	private static abstract class DBUnaryNumberFunction extends NumberExpression {

		protected IntegerExpression only;

		DBUnaryNumberFunction() {
			this.only = null;
		}

		DBUnaryNumberFunction(IntegerExpression only) {
			this.only = only;
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
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public DBUnaryNumberFunction copy() {
			DBUnaryNumberFunction newInstance;
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

	private static abstract class IntegerIntegerFunctionIntegerResult extends IntegerExpression {

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
		public String toSQLString(DBDatabase db) {
			return this.beforeValue(db) + getFirst().toSQLString(db) + this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db)) + this.afterValue(db);
		}

		@Override
		public IntegerIntegerFunctionIntegerResult copy() {
			IntegerIntegerFunctionIntegerResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst().copy();
			newInstance.second = getSecond().copy();
			return newInstance;
		}

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return " " + getFunctionName(db) + "( ";
		}

		protected String getSeparator(DBDatabase db) {
			return ", ";
		}

		protected String afterValue(DBDatabase db) {
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
			} catch (InstantiationException | IllegalAccessException ex) {
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

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional();
		}
	}

	private static abstract class DBNnaryBooleanFunction extends BooleanExpression {

		private IntegerExpression column;
		private final List<IntegerResult> values = new ArrayList<IntegerResult>();
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

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return "( ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
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

		protected IntegerExpression column;
		protected final List<IntegerResult> values = new ArrayList<IntegerResult>();
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

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return "( ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
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
			Collections.copy(this.values, newInstance.values);
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
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

		protected IntegerResult numberExpression = null;
		protected final List<StringResult> values = new ArrayList<StringResult>();
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
		abstract public String toSQLString(DBDatabase db);

		@Override
		public DBIntegerAndNnaryStringFunction copy() {
			DBIntegerAndNnaryStringFunction newInstance;
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

		abstract String getFunctionName(DBDatabase db);

		protected String beforeValue(DBDatabase db) {
			return "" + getFunctionName(db) + "( ";
		}

		protected String afterValue(DBDatabase db) {
			return ") ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
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

		MaxUnaryFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getMaxFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class MinUnaryFunction extends DBUnaryFunction {

		MinUnaryFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getMinFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class MinusBinaryArithmetic extends DBBinaryArithmetic {

		MinusBinaryArithmetic(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " - ";
		}
	}

	private static class BracketUnaryFunction extends DBUnaryFunction {

		BracketUnaryFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "";
		}
	}

	private static class DivisionBinaryArithmetic extends DBBinaryArithmetic {

		DivisionBinaryArithmetic(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " / ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + "(" + second.toSQLString(db) + "+0.0)";
		}
	}

	private class StringResultFunction extends DBUnaryStringFunction {

		public StringResultFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().doIntegerToStringTransform(super.only.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDatabase db) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean getIncludesNull() {
			return only.getIncludesNull();
		}
	}

	private class IsFunction extends DBBinaryBooleanArithmetic {

		public IsFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (super.getIncludesNull()) {
				return BooleanExpression.isNull(getFirst()).toSQLString(db);
			} else {
				return db.getDefinition().doIntegerEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " = ";
		}
	}

	private static class IsLessThanFunction extends DBBinaryBooleanArithmetic {

		public IsLessThanFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " < ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static class IsLessThanOrEqualFunction extends DBBinaryBooleanArithmetic {

		public IsLessThanOrEqualFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " <= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static class IsGreaterThanFunction extends DBBinaryBooleanArithmetic {

		public IsGreaterThanFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " > ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static class IsGreaterThanOrEqualFunction extends DBBinaryBooleanArithmetic {

		public IsGreaterThanOrEqualFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " >= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private class IsInFunction extends DBNnaryBooleanFunction {

		public IsInFunction(IntegerExpression leftHandSide, IntegerResult[] rightHandSide) {
			super(leftHandSide, rightHandSide);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			List<String> sqlValues = new ArrayList<>();
			for (IntegerResult value : this.getValues()) {
				sqlValues.add(value.toSQLString(db));
			}
			return db.getDefinition().doInTransform(getColumn().toSQLString(db), sqlValues);
		}

		@Override
		protected String getFunctionName(DBDatabase db) {
			return " IN ";
		}
	}

	private static class LeastOfFunction extends DBNnaryIntegerFunction {

		public LeastOfFunction(IntegerResult[] rightHandSide) {
			super(rightHandSide);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			List<String> strs = new ArrayList<>();
			for (IntegerResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.getDefinition().doLeastOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDatabase db) {
			return db.getDefinition().getLeastOfFunctionName();
		}
	}

	private static class GreatestOfFunction extends DBNnaryIntegerFunction {

		public GreatestOfFunction(IntegerResult[] rightHandSide) {
			super(rightHandSide);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			List<String> strs = new ArrayList<>();
			for (IntegerResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.getDefinition().doGreatestOfTransformation(strs);
		}

		@Override
		protected String getFunctionName(DBDatabase db) {
			return db.getDefinition().getGreatestOfFunctionName();
		}
	}

	private class IfDBNullFunction extends IntegerIntegerFunctionIntegerResult {

		public IfDBNullFunction(IntegerExpression first, IntegerResult second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().doIntegerIfNullTransform(this.getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getIfNullFunctionName();
		}
	}

	private class ExpFunction extends DBUnaryFunction {

		public ExpFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (!db.getDefinition().supportsExpFunction()) {
				return (new NumberExpression(Math.E)).power(this.only.numberResult().isGreaterThan(799.0).ifThenElse(null, this.only.numberResult())).integerPart().toSQLString(db);
			} else {
				return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getExpFunctionName();
		}
	}

	private static class CosFunction extends DBUnaryFunction {

		public CosFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "cos";
		}
	}

	private class CoshFunction extends DBUnaryFunction {

		public CoshFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsHyperbolicFunctionsNatively()) {
				return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
			} else {
				IntegerExpression first = this.only;
				//(ex + e-x)/2
				return first.exp().plus(first.times(-1).exp().bracket()).bracket().dividedBy(2).bracket().toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "cosh";
		}
	}

	private static class SineFunction extends DBUnaryFunction {

		public SineFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "sin";
		}
	}

	private class NumberResultFunction extends DBUnaryNumberFunction {

		public NumberResultFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().doIntegerToNumberTransform(this.only.toSQLString(db));
		}
	}

	private static class TanFunction extends DBUnaryFunction {

		public TanFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "tan";
		}
	}

	private class TanhFunction extends DBUnaryFunction {

		public TanhFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsHyperbolicFunctionsNatively()) {
				return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
			} else {
				IntegerExpression first = this.only;
				//(ex - e-x)/(ex + e-x)
				return first.exp().minus(first.times(-1).exp()).bracket().dividedBy(first.exp().plus(first.times(-1).exp()).bracket()).bracket().toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "tanh";
		}
	}

	private static class AbsoluteValueFunction extends DBUnaryFunction {

		public AbsoluteValueFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "abs";
		}
	}

	private static class ArcCosineFunction extends DBUnaryFunction {

		public ArcCosineFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "acos";
		}
	}

	private class ArcSineFunction extends DBUnaryFunction {

		public ArcSineFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsArcSineFunction()) {
				return super.toSQLString(db);
			} else {
				return only.dividedBy(value(1.0).minus(only.times(only).bracket()).bracket().squareRoot()).arctan().toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "asin";
		}
	}

	private static class ArcTangentFunction extends DBUnaryFunction {

		public ArcTangentFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "atan";
		}
	}

	private static class ArcTan2Function extends IntegerIntegerFunctionIntegerResult {

		public ArcTan2Function(IntegerExpression first, IntegerExpression second) {
			super(first, second);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getArctan2FunctionName();
		}
	}

	private class CotangentFunction extends DBUnaryFunction {

		public CotangentFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsCotangentFunction()) {
				return super.toSQLString(db);
			} else {
				return only.cos().dividedBy(only.sine()).bracket().toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "cot";
		}
	}

	private class DegreesFunction extends DBUnaryFunction {

		public DegreesFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsDegreesFunction()) {
				return super.toSQLString(db);
			} else {
				return db.getDefinition().doDegreesTransform(this.only.toSQLString(db));
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "degrees";
		}
	}

	private class RadiansFunction extends DBUnaryFunction {

		public RadiansFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsRadiansFunction()) {
				return super.toSQLString(db);
			} else {
				return db.getDefinition().doRadiansTransform(this.only.toSQLString(db));
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "radians";
		}
	}

	private static class NaturalLogarithmFunction extends DBUnaryFunction {

		public NaturalLogarithmFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getNaturalLogFunctionName();
		}
	}

	private class LogBase10Function extends DBUnaryFunction {

		public LogBase10Function(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().doLogBase10IntegerTransform(this.only.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getLogBase10FunctionName();
		}
	}

	private static class PowerFunction extends IntegerIntegerFunctionIntegerResult {

		public PowerFunction(IntegerExpression first, IntegerExpression second) {
			super(first, second);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "power";
		}
	}

	private static class RandomFunction extends DBNonaryFunction {

		public RandomFunction() {
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().doRandomIntegerTransform();
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "rand";
		}
	}

	private static class SignFunction extends DBUnaryFunction {

		public SignFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "sign";
		}
	}

	private static class SquareRootFunction extends DBUnaryFunction {

		public SquareRootFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "sqrt";
		}
	}

	private static class RoundUpFunction extends DBUnaryFunction {

		public RoundUpFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getRoundUpFunctionName();
		}
	}

	private class RoundFunction extends DBUnaryFunction {

		public RoundFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().doRoundTransform(only.toSQLString(db));
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "round";
		}
	}

	private class RoundToNumberOfDecimalPlacesFunction extends IntegerIntegerFunctionIntegerResult {

		public RoundToNumberOfDecimalPlacesFunction(IntegerExpression first, IntegerExpression second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			try {
				return db.getDefinition().doRoundWithDecimalPlacesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				IntegerExpression power = IntegerExpression.value(10).power(getSecond().round());
				return getFirst().times(power).round().dividedBy(power).toSQLString(db);
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "round";
		}
	}

	private static class RoundDownFunction extends DBUnaryFunction {

		public RoundDownFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "floor";
		}
	}

	private class TruncFunction extends DBUnaryFunction {

		public TruncFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().doTruncTransform(only.toSQLString(db), "0");
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getTruncFunctionName();
		}

		@Override
		protected String afterValue(DBDatabase db) {
			return ", 0) ";
		}
	}

	private static class PlusFunction extends DBBinaryArithmetic {

		public PlusFunction(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " + ";
		}
	}

	private static class TimesFunction extends DBBinaryArithmetic {

		public TimesFunction(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " * ";
		}
	}

	private class DivideByFunction extends DBBinaryArithmetic {

		public DivideByFunction(IntegerResult first, IntegerResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " / ";
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return "(0.0+" + first.toSQLString(db) + ")" + this.getEquationOperator(db) + second.toSQLString(db);
		}
	}

	private class ModulusFunction extends IntegerIntegerFunctionIntegerResult {

		public ModulusFunction(IntegerExpression first, IntegerExpression second) {
			super(first, second);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsModulusFunction()) {
				return db.getDefinition().doModulusTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} else {
				return "((" + getFirst().toSQLString(db) + ") % (" + getSecond().toSQLString(db) + "))";
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "MOD";
		}
	}

	private class ChooseFunction extends DBIntegerAndNnaryStringFunction {

		public ChooseFunction(IntegerResult numberResult, StringResult[] rightHandSide) {
			super(numberResult, rightHandSide);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			List<String> strs = new ArrayList<>();
			for (StringResult num : this.values) {
				strs.add(num.toSQLString(db));
			}
			return db.getDefinition().doChooseTransformation(IntegerExpression.value(numberExpression).plus(1).bracket().toSQLString(db), strs);
		}
	}

	private static class AverageFunction extends DBUnaryFunction {

		public AverageFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getAverageFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private class StandardDeviation extends DBUnaryFunction {

		public StandardDeviation(IntegerExpression only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (db.getDefinition().supportsStandardDeviationFunction()) {
				return super.toSQLString(db);
			} else if (this.only != null) {
				IntegerExpression numb = this.only;
				return new IntegerExpression(numb).max().minus(new IntegerExpression(numb).min()).bracket().dividedBy(6).toSQLString(db);
			} else {
				return null;
			}
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getStandardDeviationFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class SumFunction extends DBUnaryFunction {

		public SumFunction(IntegerExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getSumFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class CountAllFunction extends DBNonaryFunction {

		public CountAllFunction() {
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getCountFunctionName();
		}

		@Override
		public IntegerResult getInnerIntegerResult() {
			return this;
		}

		@Override
		protected String afterValue(DBDatabase db) {
			return "(*)";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class NullExpression extends IntegerExpression {

		public NullExpression() {
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return db.getDefinition().getNull();
		}
	}
}
