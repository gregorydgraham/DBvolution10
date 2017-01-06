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

import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.RangeComparable;
import nz.co.gregs.dbvolution.results.NumberResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.results.InComparable;

/**
 * StringExpression implements standard functions that produce a character or
 * string result, including CHAR and VARCHAR.
 *
 * <p>
 * Most query requirements are provided by {@link QueryableDatatype}s like
 * {@link DBString} or {@link DBInteger} but expressions can provide more
 * functions or more precise control.
 *
 * <p>
 * Use a StringExpression to produce a string from an existing column,
 * expression, or value and perform string manipulation.
 *
 * <p>
 * Generally you get a StringExpression from a value or column using {@link StringExpression#value(java.lang.String)
 * } or {@link DBRow#column(nz.co.gregs.dbvolution.datatypes.DBString) }.
 *
 * @author Gregory Graham
 */
public class StringExpression implements StringResult, RangeComparable<StringResult>, InComparable<StringResult>, ExpressionColumn<DBString> {

	/**
	 * Creates a StringExpression that will return a database NULL.
	 *
	 * @return a StringExpression that resolves to NULL within the database
	 */
	public static StringExpression nullExpression() {
		return new StringExpression() {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().getNull();
			}

		};
	}

	private StringResult string1 = null;
	private boolean nullProtectionRequired;

	/**
	 * Default Constructor
	 *
	 */
	protected StringExpression() {
	}

	/**
	 * Creates a StringExpression from an arbitrary StringResult object.
	 *
	 * <p>
	 * {@link StringResult} objects are generally StringExpressions but they can
	 * be {@link DBString}, {@link StringColumn}, or other types.
	 *
	 * @param stringVariable	stringVariable
	 */
	public StringExpression(StringResult stringVariable) {
		string1 = stringVariable;
		if (stringVariable == null || stringVariable.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Creates a StringExpression from an arbitrary String object.
	 *
	 * <p>
	 * Essentially the same as {@link StringExpression#value(java.lang.String) }
	 *
	 * @param stringVariable	stringVariable
	 */
	public StringExpression(String stringVariable) {
		string1 = new DBString(stringVariable);
		if (stringVariable == null || stringVariable.isEmpty()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Creates a StringExpression from an arbitrary DBString object.
	 *
	 * @param stringVariable	stringVariable
	 */
	public StringExpression(DBString stringVariable) {
		if (stringVariable == null) {
			string1 = null;
			nullProtectionRequired = true;
		} else {
			string1 = stringVariable.copy();
			if (stringVariable.getIncludesNull()) {
				nullProtectionRequired = true;
			}
		}
	}

	/**
	 * Creates a StringExpression from an arbitrary Number object.
	 *
	 * <p>
	 * Essentially the same as {@code NumberExpression.value(numberVariable).stringResult()
	 * }.
	 *
	 * <p>
	 * Refer to {@link NumberExpression#NumberExpression(java.lang.Number) } and {@link NumberExpression#stringResult()
	 * } for more information.
	 *
	 * @param numberVariable	numberVariable
	 */
	public StringExpression(NumberResult numberVariable) {
		if (numberVariable == null) {
			string1 = null;
			nullProtectionRequired = true;
		} else {
			string1 = numberVariable.stringResult();
			if (string1.getIncludesNull()) {
				nullProtectionRequired = true;
			}
		}
	}

	/**
	 * Creates a StringExpression from an arbitrary Number object.
	 *
	 * <p>
	 * Essentially the same as {@code NumberExpression.value(numberVariable).stringResult()
	 * }.
	 *
	 * <p>
	 * Refer to {@link NumberExpression#NumberExpression(java.lang.Number) } and {@link NumberExpression#stringResult()
	 * } for more information.
	 *
	 * @param numberVariable	numberVariable
	 */
	public StringExpression(Number numberVariable) {
		string1 = NumberExpression.value(numberVariable).stringResult();
		if (numberVariable == null || string1.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	@Override
	public String toSQLString(DBDatabase db) {
		StringResult stringInput = getStringInput();
		if(stringInput==null){
			stringInput = StringExpression.value("<NULL>");
		}
		return stringInput.toSQLString(db);
	}

	@Override
	public StringExpression copy() {
		return new StringExpression(this);
	}

	@Override
	public boolean isPurelyFunctional() {
		if (string1 == null) {
			return true;
		} else {
			return string1.isPurelyFunctional();
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
	 * @param string	string
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public static StringExpression value(String string) {
		return new StringExpression(string);
	}

	/**
	 * Provides a default option when the StringExpression resolves to NULL within
	 * the query.
	 *
	 * @param alternative	alternative
	 * @return a StringExpression that will substitute to the given value when the
	 * StringExpression resolves to NULL.
	 */
	public StringExpression ifDBNull(String alternative) {
		return this.ifDBNull(new StringExpression(alternative));
	}

	/**
	 * Provides a default option when the StringExpression resolves to NULL within
	 * the query.
	 *
	 * @param alternative	alternative
	 * @return a StringExpression that will substitute to the given value when the
	 * StringExpression resolves to NULL.
	 */
	public StringExpression ifDBNull(StringResult alternative) {
		return new StringExpression(
				new StringExpression.DBBinaryStringFunction(this, new StringExpression(alternative)) {

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doStringIfNullTransform(this.getFirst().toSQLString(db), getSecond().toSQLString(db));
			}

//			@Override
//			String getFunctionName(DBDatabase db) {
//				return db.getDefinition().getIfNullFunctionName();
//			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	/**
	 * Creates a query comparison using the LIKE operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.
	 *
	 * <p>
	 * DBvolution does not process the SQL pattern so please ensure that it
	 * conforms to the database's implementation of LIKE. Most implementations
	 * only provide access to the "_" and "%" wildcards but there may be
	 * exceptions.
	 *
	 * @param sqlPattern	sqlPattern
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLike(String sqlPattern) {
		return isLike(value(sqlPattern));
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
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThan(String value, BooleanExpression fallBackWhenEquals) {
		return this.isLessThan(StringExpression.value(value), fallBackWhenEquals);
//		return this.isLessThan(value).or(this.is(value).and(fallBackWhenEquals));
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
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThan(String value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(StringExpression.value(value), fallBackWhenEquals);
//		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
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
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isLessThan(StringResult value, BooleanExpression fallBackWhenEquals) {
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
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isGreaterThan(StringResult value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(value).or(this.is(value).and(fallBackWhenEquals));
	}

	/**
	 * Creates a query comparison using the LIKE operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.
	 *
	 * <p>
	 * DBvolution does not process the SQL pattern so please ensure that it
	 * conforms to the database's implementation of LIKE. Most implementations
	 * only provide access to the "_" and "%" wildcards but there may be
	 * exceptions.
	 *
	 * @param sqlPattern	sqlPattern
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLike(StringResult sqlPattern) {
		if (sqlPattern.getIncludesNull()) {
			return new BooleanExpression(this.isNull());
		} else {
			return new BooleanExpression(new DBBinaryBooleanArithmetic(this, sqlPattern) {
				@Override
				protected String getEquationOperator(DBDatabase db) {
					return " LIKE ";
				}

				@Override
				public boolean getIncludesNull() {
					return false;
				}
			});
		}
	}

	/**
	 * Creates a query comparison using the LIKE operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern changing both
	 * expressions to lowercase first.
	 *
	 * <p>
	 * DBvolution does not process the SQL pattern so please ensure that it
	 * conforms to the database's implementation of LIKE. Most implementations
	 * only provide access to the "_" and "%" wildcards but there may be
	 * exceptions.
	 *
	 * @param sqlPattern	sqlPattern
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLikeIgnoreCase(String sqlPattern) {
		return isLikeIgnoreCase(value(sqlPattern));
	}

	/**
	 * Creates a query comparison using the LIKE operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern changing both
	 * expressions to lowercase first.
	 *
	 * <p>
	 * DBvolution does not process the SQL pattern so please ensure that it
	 * conforms to the database's implementation of LIKE. Most implementations
	 * only provide access to the "_" and "%" wildcards but there may be
	 * exceptions.
	 *
	 * @param sqlPattern	sqlPattern
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLikeIgnoreCase(StringResult sqlPattern) {
		return this.isLikeIgnoreCase(new StringExpression(sqlPattern));
	}

	/**
	 * Creates a query comparison using the LIKE operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern changing both
	 * expressions to lowercase first.
	 *
	 * <p>
	 * DBvolution does not process the SQL pattern so please ensure that it
	 * conforms to the database's implementation of LIKE. Most implementations
	 * only provide access to the "_" and "%" wildcards but there may be
	 * exceptions.
	 *
	 * @param sqlPattern	sqlPattern
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLikeIgnoreCase(StringExpression sqlPattern) {
		return this.lowercase().isLike(sqlPattern.lowercase());
	}

	/**
	 * Creates a query comparison using the EQUALS operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value changing both expressions to
	 * lowercase first.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIgnoreCase(String equivalentString) {
		return isIgnoreCase(value(equivalentString));
	}

	/**
	 * Creates a query comparison using the EQUALS operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value changing both expressions to
	 * lowercase first.
	 *
	 * @param numberResult	numberResult
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIgnoreCase(NumberResult numberResult) {
		return isIgnoreCase(numberResult.stringResult().lowercase());
	}

	/**
	 * Creates a query comparison using the EQUALS operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value changing both expressions to
	 * lowercase first.
	 *
	 * @param number	number
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIgnoreCase(Number number) {
		return isIgnoreCase(NumberExpression.value(number).stringResult().lowercase());
	}

	/**
	 * Creates a query comparison using the EQUALS operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value changing both expressions to
	 * lowercase first.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIgnoreCase(StringResult equivalentString) {
		return isIgnoreCase(new StringExpression(equivalentString));
	}

	/**
	 * Creates a query comparison using the EQUALS operator and the LOWERCASE
	 * function.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value changing both expressions to
	 * lowercase first.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIgnoreCase(StringExpression equivalentString) {
		return this.lowercase().is(equivalentString.lowercase());
	}

	/**
	 * Creates a query comparison using the EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression is(String equivalentString) {
		if (equivalentString == null) {
			return this.isNull();
		} else {
			return this.is(value(equivalentString));
		}
	}

	/**
	 * Creates a query comparison using the EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param numberResult	numberResult
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression is(NumberResult numberResult) {
		return this.is(numberResult.stringResult());
	}

	/**
	 * Creates a query comparison using the EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param number	number
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression is(Number number) {
		return is(NumberExpression.value(number).stringResult());
	}

	/**
	 * Creates a query comparison using the EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression is(StringExpression equivalentString) {
		return is((StringResult) equivalentString);
	}

	/**
	 * Creates a query comparison using the EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression is(StringResult equivalentString) {
		if (equivalentString == null) {
			return new BooleanExpression(this.isNull());
		} else {
			final BooleanExpression is = new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {

				@Override
				public String toSQLString(DBDatabase db) {
					return db.getDefinition().doStringEqualsTransform(super.first.toSQLString(db), super.second.toSQLString(db));
				}

				@Override
				protected String getEquationOperator(DBDatabase db) {
					return " = ";
				}

				@Override
				public boolean getIncludesNull() {
					return false;
				}
			});
			if (equivalentString.getIncludesNull()) {
				return BooleanExpression.anyOf(this.isNull(), is);
			} else {
				return is;
			}
		}
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isNot(String equivalentString) {
		return this.is(value(equivalentString)).not();
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param numberResult	numberResult
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isNot(NumberResult numberResult) {
		return this.is(numberResult.stringResult()).not();
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param number	number
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isNot(Number number) {
		return is(NumberExpression.value(number).stringResult()).not();
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isNot(StringResult equivalentString) {
		return is(equivalentString).not();
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(StringResult lowerBound, StringResult upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(String lowerBound, StringResult upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(StringResult lowerBound, String upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetween(String lowerBound, String upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(StringResult lowerBound, StringResult upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(String lowerBound, StringResult upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(StringResult lowerBound, String upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenInclusive(String lowerBound, String upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(StringResult lowerBound, StringResult upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(String lowerBound, StringResult upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(StringResult lowerBound, String upperBound) {
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
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(String lowerBound, String upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
	}

	/**
	 * Creates a query comparison using the LESSTHAN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLessThan(String equivalentString) {
		return isLessThan(value(equivalentString));
	}

	/**
	 * Creates a query comparison using the LESSTHAN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isLessThan(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return new BooleanExpression(this.isNull());
		} else {
			return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
				@Override
				protected String getEquationOperator(DBDatabase db) {
					return " < ";
				}

				@Override
				public boolean getIncludesNull() {
					return false;
				}
			});
		}
	}

	/**
	 * Creates a query comparison using the "&lt;=" operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLessThanOrEqual(String equivalentString) {
		return isLessThanOrEqual(value(equivalentString));
	}

	/**
	 * Creates a query comparison using the "&lt;=" operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return new BooleanExpression(this.isNull());
		} else {
			return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
				@Override
				protected String getEquationOperator(DBDatabase db) {
					return " <= ";
				}

				@Override
				public boolean getIncludesNull() {
					return false;
				}
			});
		}
	}

	/**
	 * Creates a query comparison using the GREATERTHAN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isGreaterThan(String equivalentString) {
		return isGreaterThan(value(equivalentString));
	}

	/**
	 * Creates a query comparison using the GREATERTHAN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isGreaterThan(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return new BooleanExpression(this.isNotNull());
		} else {
			return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
				@Override
				protected String getEquationOperator(DBDatabase db) {
					return " > ";
				}

				@Override
				public boolean getIncludesNull() {
					return false;
				}
			});
		}
	}

	/**
	 * Creates a query comparison using the "&gt;=" operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isGreaterThanOrEqual(String equivalentString) {
		return isGreaterThanOrEqual(value(equivalentString));
	}

	/**
	 * Creates a query comparison using the "&gt;=" operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return this.is(equivalentString).not();
		} else {
			return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
				@Override
				protected String getEquationOperator(DBDatabase db) {
					return " >= ";
				}

				@Override
				public boolean getIncludesNull() {
					return false;
				}
			});
		}
	}

	/**
	 * Creates a query comparison using the IN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that indicates whether
	 * the current StringExpression is included in the supplied values.
	 *
	 * @param possibleValues	possibleValues
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIn(String... possibleValues) {
		List<StringExpression> possVals = new ArrayList<StringExpression>();
		for (String str : possibleValues) {
			possVals.add(StringExpression.value(str));
		}
		return isIn(possVals.toArray(new StringExpression[]{}));
	}

	/**
	 * Creates a query comparison using the IN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that indicates whether
	 * the current StringExpression is included in the supplied values.
	 *
	 * @param possibleValues	possibleValues
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIn(Collection<String> possibleValues) {
		List<StringExpression> possVals = new ArrayList<StringExpression>();
		for (String str : possibleValues) {
			possVals.add(StringExpression.value(str));
		}
		return isIn(possVals.toArray(new StringExpression[]{}));
	}

	/**
	 * Creates a query comparison using the IN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that indicates whether
	 * the current StringExpression is included in the supplied values.
	 *
	 * @param possibleValues	possibleValues
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIn(StringResult... possibleValues) {
		final BooleanExpression isInExpression
				= new BooleanExpression(new DBNnaryBooleanFunction(this, possibleValues) {

					@Override
					public String toSQLString(DBDatabase db) {
						List<String> sqlValues = new ArrayList<String>();
						for (StringResult value : values) {
							sqlValues.add(value.toSQLString(db));
						}
						return db.getDefinition().doInTransform(column.toSQLString(db), sqlValues);
					}

					@Override
					protected String getFunctionName(DBDatabase db) {
						return " IN ";
					}
				});
		if (isInExpression.getIncludesNull()) {
			return BooleanExpression.anyOf(new BooleanExpression(this.isNull()), isInExpression);
		} else {
			return isInExpression;
		}
	}

	/**
	 * Creates a query expression that appends the supplied value to the current
	 * StringExpression.
	 *
	 * @param string2	string2
	 * @return a StringExpression.
	 */
	public StringExpression append(StringResult string2) {
		return new StringExpression(new DBBinaryStringArithmetic(this, string2) {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doConcatTransform(super.first.toSQLString(db), super.second.toSQLString(db));
			}

			@Override
			protected String getEquationOperator(DBDatabase db) {
				return "";
			}
		});
	}

	/**
	 * Creates a query expression that appends the supplied value to the current
	 * StringExpression.
	 *
	 * @param string2	string2
	 * @return a StringExpression.
	 */
	public StringExpression append(String string2) {
		return this.append(StringExpression.value(string2));
	}

	/**
	 * Creates a query expression that appends the supplied value to the current
	 * StringExpression.
	 *
	 * @param number1	number1
	 * @return a StringExpression.
	 */
	public StringExpression append(NumberResult number1) {
		return this.append(new NumberExpression(number1).stringResult());
	}

	/**
	 * Creates a query expression that appends the supplied value to the current
	 * StringExpression.
	 *
	 * @param number1	number1
	 * @return a StringExpression.
	 */
	public StringExpression append(Number number1) {
		return this.append(NumberExpression.value(number1));
	}

	/**
	 * Creates a query expression that replaces the supplied value within the
	 * current StringExpression.
	 *
	 * @param findString findString
	 * @param replaceWith replaceWith
	 * @return a StringExpression.
	 */
	public StringExpression replace(String findString, String replaceWith) {
		return this.replace(new StringExpression(findString), new StringExpression(replaceWith));
	}

	/**
	 * Creates a query expression that replaces the supplied value within the
	 * current StringExpression.
	 *
	 * @param findString findString
	 * @param replaceWith replaceWith
	 * @return a StringExpression.
	 */
	public StringExpression replace(StringResult findString, String replaceWith) {
		return this.replace(findString, StringExpression.value(replaceWith));
	}

	/**
	 * Creates a query expression that replaces the supplied value within the
	 * current StringExpression.
	 *
	 * @param findString findString
	 * @param replaceWith replaceWith
	 * @return a StringExpression.
	 */
	public StringExpression replace(String findString, StringResult replaceWith) {
		return this.replace(StringExpression.value(findString), replaceWith);
	}

	/**
	 * Creates a query expression that replaces the supplied value within the
	 * current StringExpression.
	 *
	 * @param findString findString
	 * @param replaceWith replaceWith
	 * @return a StringExpression.
	 */
	public StringExpression replace(StringResult findString, StringResult replaceWith) {
		StringResult replaceValue = replaceWith;
		if (replaceWith.getIncludesNull()) {
			replaceValue = StringExpression.value("");
		}
		return new StringExpression(
				new DBTrinaryStringFunction(this, findString, replaceValue) {

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doReplaceTransform(
						this.getFirst().toSQLString(db),
						this.getSecond().toSQLString(db),
						this.getThird().toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "REPLACE";
			}

			@Override
			public boolean getIncludesNull() {
				// handled before creation
				return false;
			}
		});
	}

	/**
	 * Retrieve the substring that precedes the supplied value.
	 *
	 * <p>
	 * Complements {@link #substringAfter(java.lang.String) }.
	 *
	 * <p>
	 * Within this expression, find the supplied value and return all characters
	 * before the value, not including the value itself.
	 *
	 * @param splitBeforeThis the value marks the end of the required string.
	 * @return a string expression
	 */
	public StringExpression substringBefore(String splitBeforeThis) {
		return substringBefore(value(splitBeforeThis));
	}

	/**
	 * Retrieve the substring that precedes the supplied value.
	 *
	 * <p>
	 * Complements {@link #substringAfter(java.lang.String) }.
	 *
	 * <p>
	 * Within this expression, find the supplied value and return all characters
	 * before the value, not including the value itself.
	 *
	 * @param splitBeforeThis the value that marks the end of the required string
	 * @return a string expression
	 */
	public StringExpression substringBefore(StringResult splitBeforeThis) {
		return new StringExpression(new DBBinaryStringFunction(this, new StringExpression(splitBeforeThis)) {

			@Override
			public String toSQLString(DBDatabase db) {
				try {
					return db.getDefinition().doSubstringBeforeTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException exp) {
					return getFirst().locationOf(getSecond()).isGreaterThan(0).ifThenElse(getFirst().substring(0, getFirst().locationOf(getSecond()).minus(1)), value("")).toSQLString(db);
				}
			}

//			@Override
//			String getFunctionName(DBDatabase db) {
//				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//			}
		});
	}

	/**
	 * Retrieve the substring that follows the supplied value.
	 *
	 * <p>
	 * Complements {@link #substringBefore(java.lang.String) }.
	 *
	 * <p>
	 * Within this expression, find the supplied value and return all characters
	 * after the value, not including the value itself.
	 *
	 * @param splitAfterThis the value that marks the beginning of the required
	 * string
	 * @return a string expression
	 */
	public StringExpression substringAfter(String splitAfterThis) {
		return substringAfter(value(splitAfterThis));
	}

	/**
	 * Retrieve the substring that follows the supplied value.
	 *
	 * <p>
	 * Complements {@link #substringBefore(java.lang.String) }.
	 *
	 * <p>
	 * Within this expression, find the supplied value and return all characters
	 * after the value, not including the value itself.
	 *
	 * @param splitAfterThis the value that marks the beginning of the required
	 * string
	 * @return a string expression
	 */
	public StringExpression substringAfter(StringResult splitAfterThis) {
		return new StringExpression(new DBBinaryStringFunction(this, new StringExpression(splitAfterThis)) {

			@Override
			public String toSQLString(DBDatabase db) {
				try {
					return db.getDefinition().doSubstringAfterTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException exp) {
					return getFirst().locationOf(getSecond()).isGreaterThan(0).ifThenElse(getFirst().substring(getFirst().locationOf(getSecond()), getFirst().length()), value("")).toSQLString(db);
				}
			}

//			@Override
//			String getFunctionName(DBDatabase db) {
//				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//			}
		});
	}

	/**
	 * Retrieve the substring that follows the first supplied value but precedes
	 * the second value.
	 *
	 * <p>
	 * Complements {@link #substringBefore(java.lang.String) } and {@link #substringAfter(java.lang.String)
	 * }.
	 *
	 * <p>
	 * Within this expression, find the first supplied value and return all
	 * characters after the value, not including the value itself, but prior to
	 * the second value.
	 *
	 * <p>
	 * for an expression like "(1234)", substringBetween("(", ")") will return
	 * "1234".
	 *
	 * @param splitAfterThis the value that marks the beginning of the required
	 * string
	 * @param butBeforeThis the value that marks the end of the required string
	 * @return a string expression
	 */
	public StringExpression substringBetween(String splitAfterThis, String butBeforeThis) {
		return substringBetween(value(splitAfterThis), value(butBeforeThis));
	}

	/**
	 * Retrieve the substring that follows the first supplied value but precedes
	 * the second value.
	 *
	 * <p>
	 * Complements {@link #substringBefore(java.lang.String) } and {@link #substringAfter(java.lang.String)
	 * }.
	 *
	 * <p>
	 * Within this expression, find the first supplied value and return all
	 * characters after the value, not including the value itself, but prior to
	 * the second value.
	 *
	 * <p>
	 * for an expression like "(1234)", substringBetween("(", ")") will return
	 * "1234".
	 *
	 * @param splitAfterThis the value that marks the beginning of the required
	 * string
	 * @param butBeforeThis the value that marks the end of the required string
	 * @return a string expression
	 */
	public StringExpression substringBetween(StringResult splitAfterThis, StringResult butBeforeThis) {
		return substringAfter(splitAfterThis).substringBefore(butBeforeThis);
	}

	/**
	 * Creates a query expression that trims all leading and trailing spaces from
	 * the current StringExpression.
	 *
	 * @return a StringExpression.
	 */
	public StringExpression trim() {
		return new StringExpression(
				new DBUnaryStringFunction(this) {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doTrimFunction(this.only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "NOT USED BECAUSE SQLSERVER DOESN'T IMPLEMENT TRIM";
			}
		});
	}

	/**
	 * Creates a query expression that trims all leading spaces from the current
	 * StringExpression.
	 *
	 * @return a StringExpression.
	 */
	public StringExpression leftTrim() {
		return new StringExpression(
				new DBUnaryStringFunction(this) {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doLeftTrimTransform(this.only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "";
			}
		});
	}

	/**
	 * Creates a query expression that trims all trailing spaces from the current
	 * StringExpression.
	 *
	 * @return a StringExpression.
	 */
	public StringExpression rightTrim() {
		return new StringExpression(
				new DBUnaryStringFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getRightTrimFunctionName();
			}
		});
	}

	/**
	 * Creates a query expression that changes all the letters in the current
	 * StringExpression to lowercase.
	 *
	 * @return a StringExpression.
	 */
	public StringExpression lowercase() {
		return new StringExpression(
				new DBUnaryStringFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getLowercaseFunctionName();
			}
		});
	}

	/**
	 * Creates a query expression that changes all the letters in the current
	 * StringExpression to UPPERCASE.
	 *
	 * @return a StringExpression.
	 */
	public StringExpression uppercase() {
		return new StringExpression(
				new DBUnaryStringFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getUppercaseFunctionName();
			}
		});
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end of the string.
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based	startingIndex0Based
	 * @return a StringExpression
	 */
	public StringExpression substring(Number startingIndex0Based) {
		return new Substring(this, startingIndex0Based);
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end of the string.
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based	startingIndex0Based
	 * @return a StringExpression
	 */
	public StringExpression substring(NumberResult startingIndex0Based) {
		return new Substring(this, startingIndex0Based);
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end position supplied..
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based startingIndex0Based
	 * @param endIndex0Based endIndex0Based
	 * @return a StringExpression
	 */
	public StringExpression substring(Number startingIndex0Based, Number endIndex0Based) {
		return new Substring(this, startingIndex0Based, endIndex0Based);
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end position supplied..
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based startingIndex0Based
	 * @param endIndex0Based endIndex0Based
	 * @return a StringExpression
	 */
	public StringExpression substring(NumberResult startingIndex0Based, Number endIndex0Based) {
		return new Substring(this, startingIndex0Based, new NumberExpression(endIndex0Based));
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end position supplied..
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based startingIndex0Based
	 * @param endIndex0Based endIndex0Based
	 * @return a StringExpression
	 */
	public StringExpression substring(Number startingIndex0Based, NumberExpression endIndex0Based) {
		return new Substring(this, new NumberExpression(startingIndex0Based), endIndex0Based);
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end position supplied..
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based startingIndex0Based
	 * @param endIndex0Based endIndex0Based
	 * @return a StringExpression
	 */
	public StringExpression substring(NumberResult startingIndex0Based, NumberResult endIndex0Based) {
		return new Substring(this, startingIndex0Based, endIndex0Based);
	}

	/**
	 * Create a expression that returns the length of the current expression.
	 *
	 * @return a NumberExpression of the expression's length.
	 */
	public NumberExpression length() {
		return new NumberExpression(
				new DBUnaryNumberFunction(this) {

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doStringLengthTransform(only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getStringLengthFunctionName();
			}
		});
	}

	/**
	 * Create a {@link StringExpression} that returns the name of the current
	 * user.
	 *
	 * <p>
	 * This should be the current username under which the application is
	 * accessing the database.
	 *
	 * @return a StringExpression
	 */
	public static StringExpression currentUser() {
		return new StringExpression(
				new DBNonaryStringFunction() {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getCurrentUserFunctionName();
			}
		});
	}

	/**
	 * Get the StringResult used internally.
	 *
	 * @return the string1
	 */
	protected StringResult getStringInput() {
		return string1;
	}

	/**
	 * Returns the 1-based index of the first occurrence of searchString within
	 * the StringExpression.
	 *
	 * <p>
	 * The index is 1-based, and returns 0 when the searchString is not found.</p>
	 *
	 * @param searchString	searchString
	 * @return an expression that will find the location of the searchString.
	 */
	public NumberExpression locationOf(String searchString) {
		return locationOf(value(searchString));
	}

	/**
	 * Returns the 1-based index of the first occurrence of searchString within
	 * the StringExpression.
	 *
	 * <p>
	 * The index is 1-based, and returns 0 when the searchString is not found.</p>
	 *
	 * @param searchString	searchString
	 * @return an expression that will find the location of the searchString.
	 */
	public NumberExpression locationOf(StringResult searchString) {
		return new NumberExpression(new BinaryComplicatedNumberFunction(this, searchString) {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doPositionInStringTransform(this.first.toSQLString(db), this.second.toSQLString(db));
			}
		});
	}

	/**
	 * Creates an expression that will count all the non-null values of the column
	 * supplied.
	 *
	 * <p>
	 * Count is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * @return a number expression.
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
	 * Creates an expression that will find the largest value in the column
	 * supplied.
	 *
	 * <p>
	 * Max is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * @return a String expression.
	 */
	public StringExpression max() {
		return new StringExpression(new DBUnaryStringFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getMaxFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	/**
	 * Creates an expression that will find the smallest value in the column
	 * supplied.
	 *
	 * <p>
	 * Min is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * @return a String expression.
	 */
	public StringExpression min() {
		return new StringExpression(new DBUnaryStringFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getMinFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	@Override
	public DBString getQueryableDatatypeForExpressionValue() {
		return new DBString();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (string1 != null) {
			hashSet.addAll(string1.getTablesInvolved());
		}
		return hashSet;
	}

	@Override
	public boolean isAggregator() {
		return string1 == null ? false : string1.isAggregator();
	}

	/**
	 * Creates a BooleanExpression that tests to ensure the database value is not
	 * a database NULL value.
	 *
	 * <P>
	 * The expression will be true if the value is not NULL, and false otherwise.
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	/**
	 * Creates a BooleanExpression that tests to ensure the database value is not
	 * a database NULL value AND is not an empty string.
	 *
	 * <P>
	 * The expression will be true if the value is not NULL and is not an empty
	 * string, and false otherwise.
	 *
	 * <p>
	 * This method provides the maximum portability as some database differentiate
	 * between a NULL string and a empty string, and some do not. To protect your
	 * queries against this fundamental variation use this method.
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNotNullAndNotEmpty() {
		return BooleanExpression.allOf(
				BooleanExpression.isNotNull(this),
				this.is("").not()
		);
	}

	/**
	 * Creates a BooleanExpression that tests to ensure the database value is a
	 * database NULL value.
	 *
	 * <P>
	 * The expression will be true if the value is NULL, and false otherwise.
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	/**
	 * Creates a BooleanExpression that tests to ensure the database value is a
	 * database NULL value OR is an empty string.
	 *
	 * <P>
	 * The expression will be true if the value is NULL OR the expression produces
	 * an empty string, and false otherwise.
	 *
	 * <p>
	 * This method provides the maximum portability as some database differentiate
	 * between a NULL string and a empty string, and some do not. To protect your
	 * queries against this fundamental variation use this method.
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNullOrEmpty() {
		return BooleanExpression.anyOf(
				BooleanExpression.isNull(this),
				this.is("")
		);
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	/**
	 * Adds an explicit bracket at this point in the expression chain.
	 *
	 * @return a StringExpression that will have the current expression wrapped in
	 * brackets.
	 */
	public StringExpression bracket() {
		return new StringExpression(new DBUnaryStringFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return "";
			}
		});
	}

	/**
	 * Provides direct access to the IN operator.
	 *
	 * <p>
	 * isInIgnoreCase creates a BooleanExpression that compares the current
	 * expression to the list of values using the IN operator. The resulting
	 * expression will return true if the current expression's value is included
	 * in the list of potential values, otherwise it will return false.
	 *
	 * @param potentialValues	potentialValues
	 * @return a BooleanExpression
	 */
	public BooleanExpression isInIgnoreCase(StringResult[] potentialValues) {
		List<StringResult> lowerStrings = new ArrayList<StringResult>();
		for (StringResult toArray1 : potentialValues) {
			StringExpression lowercase = new StringExpression(toArray1).lowercase();
			lowerStrings.add(lowercase);
		}

		final BooleanExpression isInExpression = this.lowercase().isIn(lowerStrings.toArray(new StringResult[]{}));

		if (isInExpression.getIncludesNull()) {
			return BooleanExpression.anyOf(new BooleanExpression(this.isNull()), isInExpression);
		} else {
			return isInExpression;
		}
	}

	/**
	 * In so far as it is possible, transform the value of this expression into a
	 * number.
	 *
	 * <p>
	 * Uses the database's own facilities to parse the value of this expression
	 * into a number.
	 *
	 * <p>
	 * May return NULL and all sorts of crazy things.
	 *
	 * @return a number expression
	 */
	public NumberExpression numberResult() {
		return new NumberExpression(
				new DBUnaryNumberFunction(this) {

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doStringToNumberTransform(this.only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "TO_NUMBER";
			}
		});
	}

	@Override
	public DBString asExpressionColumn() {
		return new DBString(this);
	}

	private static abstract class DBBinaryStringArithmetic extends StringExpression {

		private StringResult first;
		private StringResult second;
//		private boolean includeNulls;

		DBBinaryStringArithmetic(StringResult first, StringResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
		}

		@Override
		public DBBinaryStringArithmetic copy() {
			DBBinaryStringArithmetic newInstance;
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
			return this.first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBNonaryStringFunction extends StringExpression {

		DBNonaryStringFunction() {
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
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
		public DBNonaryStringFunction copy() {
			DBNonaryStringFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return false;
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

	private static abstract class DBUnaryStringFunction extends StringExpression {

		protected StringExpression only;

		DBUnaryStringFunction() {
			this.only = null;
		}

		DBUnaryStringFunction(StringExpression only) {
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
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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

//	private static abstract class DBUnaryBooleanArithmetic implements BooleanResult {
//
//		protected StringExpression only;
//
//		DBUnaryBooleanArithmetic() {
//			this.only = null;
//		}
//
//		DBUnaryBooleanArithmetic(StringExpression only) {
//			this.only = only;
//		}
//
//		@Override
//		public DBString getQueryableDatatypeForExpressionValue() {
//			return new DBString();
//		}
//
//		abstract String getFunctionName(DBDatabase db);
//
//		protected String beforeValue(DBDatabase db) {
//			return " (";
//		}
//
//		protected String afterValue(DBDatabase db) {
//			return ") ";
//		}
//
//		@Override
//		public String toSQLString(DBDatabase db) {
//			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + getFunctionName(db) + this.afterValue(db);
//		}
//
//		@Override
//		public DBUnaryBooleanArithmetic copy() {
//			DBUnaryBooleanArithmetic newInstance;
//			try {
//				newInstance = getClass().newInstance();
//			} catch (InstantiationException ex) {
//				throw new RuntimeException(ex);
//			} catch (IllegalAccessException ex) {
//				throw new RuntimeException(ex);
//			}
//			newInstance.only = only.copy();
//			return newInstance;
//		}
//
//		@Override
//		public Set<DBRow> getTablesInvolved() {
//			HashSet<DBRow> hashSet = new HashSet<DBRow>();
//			if (only != null) {
//				hashSet.addAll(only.getTablesInvolved());
//			}
//			return hashSet;
//		}
//
//		@Override
//		public boolean isAggregator() {
//			return only.isAggregator();
//		}
//
//		@Override
//		public boolean getIncludesNull() {
//			return false;
//		}
//
//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			throw new UnsupportedOperationException("NULL support would be meaningless for this function"); //To change body of generated methods, choose Tools | Templates.
//		}
//	}
	private static abstract class DBUnaryNumberFunction extends NumberExpression {

		protected StringExpression only;

		DBUnaryNumberFunction() {
			this.only = null;
		}

		DBUnaryNumberFunction(StringExpression only) {
			this.only = only;
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
			return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
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
			newInstance.only = (only == null ? null : only.copy());
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
	}

	private static abstract class DBTrinaryStringFunction extends StringExpression {

		private DBExpression first;
		private DBExpression second;
		private DBExpression third;

		DBTrinaryStringFunction(DBExpression first) {
			this.first = first;
			this.second = null;
			this.third = null;
		}

		DBTrinaryStringFunction(DBExpression first, DBExpression second) {
			this.first = first;
			this.second = second;
		}

		DBTrinaryStringFunction(DBExpression first, DBExpression second, DBExpression third) {
			this.first = first;
			this.second = second;
			this.third = third;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return this.beforeValue(db) + getFirst().toSQLString(db)
					+ this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db))
					+ this.getSeparator(db) + (getThird() == null ? "" : getThird().toSQLString(db))
					+ this.afterValue(db);
		}

		@Override
		public DBTrinaryStringFunction copy() {
			DBTrinaryStringFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst() == null ? null : getFirst().copy();
			newInstance.second = getSecond() == null ? null : getSecond().copy();
			newInstance.third = getThird() == null ? null : getThird().copy();
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
			if (getThird() != null) {
				hashSet.addAll(getThird().getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return getFirst().isAggregator() || getSecond().isAggregator() || getThird().isAggregator();
		}

		/**
		 * @return the first
		 */
		protected DBExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		protected DBExpression getSecond() {
			return second;
		}

		/**
		 * @return the third
		 */
		protected DBExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null && third == null) {
				return true;
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
			}
		}
	}

	private static abstract class DBBinaryStringFunction extends StringExpression {

		private StringExpression first;
		private StringExpression second;

		DBBinaryStringFunction(StringExpression first) {
			this.first = first;
			this.second = null;
		}

		DBBinaryStringFunction(StringExpression first, StringExpression second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

//		@Override
//		public String toSQLString(DBDatabase db) {
//			return this.beforeValue(db) + getFirst().toSQLString(db)
//					+ this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db))
//					+ this.afterValue(db);
//		}

		@Override
		public DBBinaryStringFunction copy() {
			DBBinaryStringFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = getFirst() == null ? null : getFirst().copy();
			newInstance.second = getSecond() == null ? null : getSecond().copy();
			return newInstance;
		}

		//abstract String getFunctionName(DBDatabase db);

//		protected String beforeValue(DBDatabase db) {
//			return " " + getFunctionName(db) + "( ";
//		}

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

		/**
		 * @return the first
		 */
		protected StringExpression getFirst() {
			return first;
		}

		/**
		 * @return the second
		 */
		protected StringExpression getSecond() {
			return second;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private static abstract class BinaryComplicatedNumberFunction extends NumberExpression {

		protected StringExpression first = null;
		protected StringResult second = null;

		BinaryComplicatedNumberFunction() {
			this.first = null;
		}

		BinaryComplicatedNumberFunction(StringExpression first, StringResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public abstract String toSQLString(DBDatabase db);

		@Override
		public StringExpression.BinaryComplicatedNumberFunction copy() {
			StringExpression.BinaryComplicatedNumberFunction newInstance;
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
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

	private class Substring extends StringExpression implements StringResult {

		private NumberResult startingPosition;
		private NumberResult length;

		Substring(StringResult stringInput, Number startingIndex0Based) {
			super(stringInput);
			this.startingPosition = new DBNumber(startingIndex0Based);
			this.length = new StringExpression(stringInput).length();
		}

		Substring(StringResult stringInput, NumberResult startingIndex0Based) {
			super(stringInput);
			this.startingPosition = startingIndex0Based.copy();
			this.length = new StringExpression(stringInput).length();
		}

		Substring(StringResult stringInput, Number startingIndex0Based, Number endIndex0Based) {
			super(stringInput);
			this.startingPosition = new DBNumber(startingIndex0Based);
			this.length = new DBNumber(endIndex0Based);
		}

		Substring(StringResult stringInput, NumberResult startingIndex0Based, NumberResult endIndex0Based) {
			super(stringInput);
			this.startingPosition = startingIndex0Based.copy();
			this.length = endIndex0Based.copy();
		}

		@Override
		public Substring copy() {
			return new Substring(getStringInput(), startingPosition, length);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (getStringInput() == null) {
				return "";
			} else {
				return doSubstringTransform(db, getStringInput(), startingPosition, length);
			}
		}

		public String doSubstringTransform(DBDatabase db, StringResult enclosedValue, NumberResult startingPosition, NumberResult substringLength) {
			return db.getDefinition().doSubstringTransform(
					enclosedValue.toSQLString(db),
					(startingPosition.toSQLString(db) + " + 1"),
					(substringLength != null ? (substringLength.toSQLString(db) + " - " + startingPosition.toSQLString(db)) : "")
			);
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (startingPosition == null && length == null && string1 == null) {
				return true;
			} else {
				return (startingPosition == null || startingPosition.isPurelyFunctional())
						&& (length == null || length.isPurelyFunctional())
						&& (string1 == null || string1.isPurelyFunctional());
			}
		}

	}

	private static abstract class DBBinaryBooleanArithmetic extends BooleanExpression {

		private StringResult first;
		private StringResult second;

		DBBinaryBooleanArithmetic(StringResult first, StringResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
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
	}

	private static abstract class DBNnaryBooleanFunction extends BooleanExpression {

		protected StringExpression column;
		protected List<StringResult> values = new ArrayList<StringResult>();
		private boolean includesNulls = false;

		DBNnaryBooleanFunction() {
			this.values = null;
		}

		DBNnaryBooleanFunction(StringExpression leftHandSide, StringResult[] rightHandSide) {
			this.column = leftHandSide;
			for (StringResult stringResult : rightHandSide) {
				if (stringResult == null) {
					this.includesNulls = true;
				} else if (stringResult.getIncludesNull()) {
					this.includesNulls = true;
				} else {
					values.add(stringResult);
				}
			}
		}

//		@Override
//		public DBString getQueryableDatatypeForExpressionValue() {
//			return new DBString();
//		}
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
					.append(column.toSQLString(db))
					.append(this.getFunctionName(db))
					.append(this.beforeValue(db));
			String separator = "";
			for (StringResult val : values) {
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
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
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
			for (StringResult second : values) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = column.isAggregator();
			for (StringResult numer : values) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return includesNulls;
		}

//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			includesNulls = nullsAreIncluded;
//		}
	}

	private static abstract class DBBinaryStringNumberArithmetic implements StringResult {

		private StringResult first;
		private NumberResult second;

		DBBinaryStringNumberArithmetic(StringResult first, NumberResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
		}

		@Override
		public DBBinaryStringNumberArithmetic copy() {
			DBBinaryStringNumberArithmetic newInstance;
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

	}

}
