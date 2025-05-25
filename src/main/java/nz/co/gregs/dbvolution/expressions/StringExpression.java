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

import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.expressions.search.SearchAcross;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.ExpressionHasStandardStringResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.expressions.search.SearchString;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionRequiresOrderBy;
import nz.co.gregs.separatedstring.Builder;
import nz.co.gregs.separatedstring.Encoder;

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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class StringExpression extends RangeExpression<String, StringResult, DBString> implements StringResult {

	private final static long serialVersionUID = 1l;

	// needed because of Oracle's difficulty with empty/null strings
	private final boolean stringNullProtectionRequired;

	/**
	 * Creates a StringExpression that will return a database NULL.
	 *
	 * @return a StringExpression that resolves to NULL within the database
	 */
	@Override
	public StringExpression nullExpression() {
		return new StringExpression(new NullStringExpression());
	}

	public final static StringExpression emptyString() {
		return new EmptyStringExpression();
	}

	/**
	 * Default Constructor
	 *
	 */
	protected StringExpression() {
		super();
		stringNullProtectionRequired = false;
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
		super(stringVariable);
		stringNullProtectionRequired = stringVariable == null || stringVariable.getIncludesNull();
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
	protected StringExpression(AnyResult<?> stringVariable) {
		super(stringVariable);
		stringNullProtectionRequired = stringVariable == null || stringVariable.getIncludesNull();
	}

	/**
	 * Creates a StringExpression from an arbitrary DBString object.
	 *
	 * @param stringVariable	stringVariable
	 */
	public StringExpression(DBString stringVariable) {
		super(stringVariable);
		if (stringVariable == null) {
			stringNullProtectionRequired = true;
		} else {
			final String value = stringVariable.getValue();
			stringNullProtectionRequired = value == null || value.isEmpty();
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
		super(numberVariable);
		if (numberVariable == null) {
			stringNullProtectionRequired = true;
		} else {
			stringNullProtectionRequired = numberVariable.getIncludesNull();
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
	public StringExpression(IntegerResult numberVariable) {
		super(numberVariable);
		if (numberVariable == null) {
			stringNullProtectionRequired = true;
		} else {
			stringNullProtectionRequired = numberVariable.getIncludesNull();
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
		super(value(numberVariable).stringResult());
		if (numberVariable == null) {
			stringNullProtectionRequired = true;
		} else {
			stringNullProtectionRequired = new DBNumber(numberVariable).getIncludesNull();
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
	public StringExpression(long numberVariable) {
		super(value(numberVariable).stringResult());
		stringNullProtectionRequired = false;
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
	public StringExpression(int numberVariable) {
		super(value(numberVariable).stringResult());
		stringNullProtectionRequired = false;
	}

	@Override
	public String toSQLString(DBDefinition db) {
		AnyResult<?> stringInput = getInnerResult();
		if (stringInput == null) {
			stringInput = StringExpression.value("<NULL>");
		} else if (!(stringInput instanceof StringResult)
				&& (stringInput instanceof ExpressionHasStandardStringResult)) {
			stringInput = ((ExpressionHasStandardStringResult) stringInput).stringResult();
		}
		return stringInput.toSQLString(db);
	}

	@Override
	public StringExpression copy() {
		//return isNullSafetyTerminator() ? nullString() : new StringExpression((AnyResult<?>) getInnerResult().copy());
		return new StringExpression((AnyResult<?>) (getInnerResult() == null ? null : getInnerResult().copy()));
	}

	@Override
	protected boolean isNullSafetyTerminator() {
		return stringNullProtectionRequired == false
				&& super.isNullSafetyTerminator();
	}

	@Override
	public boolean getIncludesNull() {
		return stringNullProtectionRequired == true || super.getIncludesNull();
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
	 * @return the most common string
	 */
	public StringExpression modeSimple() {
		StringExpression modeExpr = new StringExpression(
				new ModeSimpleExpression<>(this));
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
	public StringExpression modeStrict() {
		StringExpression modeExpr = new StringExpression(
				new ModeStrictExpression<>(this));

		return modeExpr;
	}

	/**
	 * Create An Appropriate Expression Object For This Object
	 *
	 * <p>
	 * The expression framework requires a *Expression to work with. The easiest
	 * way to get that is the {@code DBRow.column()} method.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	@Override
	public StringExpression expression(String string) {
		return new StringLiteral(string);
	}

	/**
	 * Create An Appropriate Expression Object For This Object
	 *
	 * <p>
	 * The expression framework requires a *Expression to work with. The easiest
	 * way to get that is the {@code DBRow.column()} method.
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
	 * @param number	string
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public StringExpression expression(Number number) {
		return new NumberExpression(number).stringResult();
	}

	/**
	 * Create An Appropriate Expression Object For This Object
	 *
	 * <p>
	 * The expression framework requires a *Expression to work with. The easiest
	 * way to get that is the {@code DBRow.column()} method.
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
	 * @param number	string
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public StringExpression expression(NumberResult number) {
		return new NumberExpression(number).stringResult();
	}

	/**
	 * Create An Appropriate Expression Object For This Object
	 *
	 * <p>
	 * The expression framework requires a *Expression to work with. The easiest
	 * way to get that is the {@code DBRow.column()} method.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	@Override
	public StringExpression expression(StringResult string) {
		return new StringExpression(string);
	}

	/**
	 * Provides a default option when the StringExpression resolves to NULL within
	 * the query.
	 *
	 * @param alternative	alternative
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression that will substitute to the given value when the
	 * StringExpression resolves to NULL.
	 */
	public StringExpression ifDBNull(String alternative) {
		return this.ifDBNull(StringExpression.value(alternative));
	}

	/**
	 * Provides a default option when the StringExpression resolves to NULL within
	 * the query.
	 *
	 * @param alternative	alternative
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression that will substitute to the given value when the
	 * StringExpression resolves to NULL.
	 */
	public StringExpression ifDBNull(StringResult alternative) {
		return new StringExpression(
				new StringIfDBNullExpression(this, new StringExpression(alternative)));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLike(String sqlPattern) {
		return isLike(value(sqlPattern));
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
	 * @param string a literal value to find
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression contains(String string) {
		return contains(value(string));
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
	 * @param strings literal values to find
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression containsAnyOf(String... strings) {
		List<BooleanExpression> bools = new ArrayList<>();
		for (String string : strings) {
			bools.add(this.contains(string));
		}
		BooleanExpression[] boolArray = bools.toArray(new BooleanExpression[]{});
		return BooleanExpression.anyOf(boolArray);
	}

	/**
	 * Advanced search that lowercases everything, requires search terms with a
	 * plus (+requiredterm), and removes search terms with a minus (-neverthis).
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.</p>
	 *
	 * <p>
	 * DBvolution does not process the SQL pattern so please ensure that it
	 * conforms to the database's implementation of LIKE. Most implementations
	 * only provide access to the "_" and "%" wildcards but there may be
	 * exceptions.</p>
	 *
	 * <p>
	 * See
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.search.HasComparisonExpression) }
	 * and {@link SearchAcross} for using this method with multiple columns
	 * easily.</p>
	 *
	 * @param searchString a collection of search terms to find
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression searchFor(SearchString searchString) {
		return searchString.setExpression(this).getComparisonExpression();
	}

	/**
	 * Advanced search that lowercases everything, requires search terms with a
	 * plus (+requiredterm), and removes search terms with a minus (-neverthis).
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.</p>
	 *
	 * <p>
	 * DBvolution does not process the SQL pattern so please ensure that it
	 * conforms to the database's implementation of LIKE. Most implementations
	 * only provide access to the "_" and "%" wildcards but there may be
	 * exceptions.</p>
	 *
	 * <p>
	 * See
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.search.HasComparisonExpression) }
	 * and {@link SearchAcross} for using this method with multiple columns
	 * easily.</p>
	 *
	 * @param strings search terms to find
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression searchFor(String... strings) {
		final SearchString searchStr = new SearchString(this, Builder.bySpaces().encoder().addAll(strings).toString());
		return searchStr.getComparisonExpression();
	}

	/**
	 * Standardised searching using string terms and expression aliases.
	 *
	 * <p>
	 * Designed to provide easy access to complex user-driven searching such as
	 * 'terminator -schwarzenagger "come with me if" desc:quote author:+"james
	 * cameron"'.</p>
	 *
	 * <p>
	 * Search terms can be single words or sequence, or quoted phrases. Terms can
	 * also be prioritized with + and - and restricted to a single column using an
	 * alias followed by a colon (alias:term). Searching for any empty value can
	 * be done with an alias followed by empty quotes, for example
	 * description:""</p>
	 *
	 * <p>
	 * Use with a single column using {@link StringExpression#searchFor(nz.co.gregs.dbvolution.expressions.search.SearchString)
	 * } and {@link StringExpression#searchForRanking(nz.co.gregs.dbvolution.expressions.search.SearchString)
	 * }: e.g. marq.column(marq.name).searchFor(searchString). If you have
	 * individual strings use
	 * {@link StringExpression#searchFor(java.lang.String...)} and {@link StringExpression#searchForRanking(java.lang.String...)
	 * }.</p>
	 *
	 * <p>
	 * searchForRanking produces a number value that can be used for sorting. </p>
	 *
	 *
	 * @param strings search terms to find
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public NumberExpression searchForRanking(SearchString strings) {
		return strings.setExpression(this).getRankingExpression();
	}

	/**
	 * Standardised searching using string terms and expression aliases.
	 *
	 * <p>
	 * Designed to provide easy access to complex user-driven searching such as
	 * 'terminator -schwarzenagger "come with me if" desc:quote author:+"james
	 * cameron"'.</p>
	 *
	 * <p>
	 * Search terms can be single words or sequence, or quoted phrases. Terms can
	 * also be prioritized with + and - and restricted to a single column using an
	 * alias followed by a colon (alias:term). Searching for any empty value can
	 * be done with an alias followed by empty quotes, for example
	 * description:""</p>
	 *
	 * <p>
	 * Use with a single column using {@link StringExpression#searchFor(nz.co.gregs.dbvolution.expressions.search.SearchString)
	 * } and {@link StringExpression#searchForRanking(nz.co.gregs.dbvolution.expressions.search.SearchString)
	 * }: e.g. marq.column(marq.name).searchFor(searchString). If you have
	 * individual strings use
	 * {@link StringExpression#searchFor(java.lang.String...)} and {@link StringExpression#searchForRanking(java.lang.String...)
	 * }.</p>
	 *
	 * <p>
	 * searchForRanking produces a number value that can be used for sorting. </p>
	 *
	 * @param strings terms to find and rank
	 * @return an expression
	 *
	 */
	public NumberExpression searchForRanking(String... strings) {
		Encoder separatedBySpaces = Builder.bySpaces().encoder();
		separatedBySpaces.addAll(strings);
		final SearchString searchStr = new SearchString(this, separatedBySpaces.encode());
		return searchForRanking(searchStr);
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
	 * @param string the literal value to search for
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression contains(StringResult string) {
		return contains(value(string));
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
	 * @param string an expression to search for
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression contains(StringExpression string) {
		return this.isLike(string.prepend("%").append("%"));
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
	 * @param string a literal string to find
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression containsIgnoreCase(String string) {
		return containsIgnoreCase(value(string));
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
	 * @param string an expression to search for
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression containsIgnoreCase(StringResult string) {
		return containsIgnoreCase(value(string));
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
	 * @param string an expression to search for
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression containsIgnoreCase(StringExpression string) {
		return this.lowercase().isLike(string.lowercase().prepend("%").append("%"));
	}

	/**
	 * Creates a query comparison using the LIKE operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.
	 *
	 * @param string a literal to search for
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression startsWith(String string) {
		return startsWith(value(string));
	}

	/**
	 * Creates a query comparison using the LIKE operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.
	 *
	 * @param string a literal to search for
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression endsWith(String string) {
		return endsWith(value(string));
	}

	/**
	 * Creates a query comparison using the LIKE operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.
	 *
	 * @param string an expression to search for
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression startsWith(StringResult string) {
		return isLike(value(string).append("%"));
	}

	/**
	 * Creates a query comparison using the LIKE operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied SQL pattern.
	 *
	 * @param string an expression to find
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression endsWith(StringResult string) {
		return isLike(value(string).prepend("%"));
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
	public BooleanExpression isLessThan(String value, BooleanExpression fallBackWhenEquals) {
		return this.isLessThan(StringExpression.value(value), fallBackWhenEquals);
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
	public BooleanExpression isGreaterThan(String value, BooleanExpression fallBackWhenEquals) {
		return this.isGreaterThan(StringExpression.value(value), fallBackWhenEquals);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLike(StringResult sqlPattern) {
		if (sqlPattern.getIncludesNull()) {
			return new BooleanExpression(this.isNull());
		} else {
			return new BooleanExpression(new StringIsLikeExpression(this, sqlPattern));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isLikeIgnoreCase(StringResult sqlPattern) {
		return this.lowercase().isLike(value(sqlPattern).lowercase());
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIgnoreCase(Number number) {
		return isIgnoreCase(NumberExpression.value(number));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression is(Number number) {
		return is(NumberExpression.value(number));
	}

	/**
	 * Creates a query comparison using the EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression is(StringResult equivalentString) {
		if (equivalentString == null) {
			return new BooleanExpression(this.isNull());
		} else {
			final BooleanExpression is = new BooleanExpression(new StringIsExpression(this, equivalentString));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isNot(String equivalentString) {
		return this.isNot(value(equivalentString));
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param numberResult	numberResult
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isNot(NumberResult numberResult) {
		return this.isNot(numberResult.stringResult());
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param numberResult	numberResult
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isNot(IntegerResult numberResult) {
		return this.isNot(numberResult.stringResult());
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param number	number
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isNot(Number number) {
		return isNot(NumberExpression.value(number));
	}

	/**
	 * Creates a query comparison using the NOT EQUALS operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isNot(StringResult equivalentString) {

		if (equivalentString.getIncludesNull()) {
			return this.isNotNull();
		} else {
			return new BooleanExpression(new StringIsNotExpression(this, equivalentString));
		}
	}

	public BooleanExpression isEmpty() {
		return this.length().is(0);
	}

	public BooleanExpression isNotEmpty() {
		return this.length().isGreaterThan(0);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(String lowerBound, StringResult upperBound) {
		return this.isBetween(value(lowerBound), upperBound);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(StringResult lowerBound, String upperBound) {
		return this.isBetween(lowerBound, value(upperBound));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetween(String lowerBound, String upperBound) {
		return this.isBetween(value(lowerBound), value(upperBound));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(String lowerBound, StringResult upperBound) {
		return this.isBetweenInclusive(value(lowerBound), upperBound);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(StringResult lowerBound, String upperBound) {
		return this.isBetweenInclusive(lowerBound, value(upperBound));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenInclusive(String lowerBound, String upperBound) {
		return this.isBetweenInclusive(value(lowerBound), value(upperBound));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(String lowerBound, StringResult upperBound) {
		return this.isBetweenExclusive(value(lowerBound), upperBound);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(StringResult lowerBound, String upperBound) {
		return this.isBetweenExclusive(lowerBound, value(upperBound));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression representing the required comparison
	 */
	@Override
	public BooleanExpression isBetweenExclusive(String lowerBound, String upperBound) {
		return this.isBetweenExclusive(value(lowerBound), value(upperBound));
	}

	/**
	 * Creates a query comparison using the LESSTHAN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that compares the
	 * current StringExpression to the supplied value.
	 *
	 * @param equivalentString	equivalentString
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isLessThan(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return new BooleanExpression(this.isNull());
		} else {
			return new BooleanExpression(new StringIsLessThanExpression(this, equivalentString));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isLessThanOrEqual(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return new BooleanExpression(this.isNull());
		} else {
			return new BooleanExpression(new StringIsLessThanOrEqualExpression(this, equivalentString));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isGreaterThan(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return new BooleanExpression(this.isNotNull());
		} else {
			return new BooleanExpression(new StringIsGreaterThanExpression(this, equivalentString));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isGreaterThanOrEqual(StringResult equivalentString) {
		if (equivalentString.getIncludesNull()) {
			return this.is(equivalentString).not();
		} else {
			return new BooleanExpression(new StringIsGreaterThanOrEqualExpression(this, equivalentString));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
//	@Override
//	public BooleanExpression isIn(String... possibleValues) {
//		return isIn(expressions(possibleValues));
//	}

	/**
	 * Creates a query comparison using the IN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that indicates whether
	 * the current StringExpression is included in the supplied values.
	 *
	 * @param possibleValues	possibleValues
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	public BooleanExpression isIn(Collection<String> possibleValues) {
		return isIn(expressions(possibleValues));
	}

	/**
	 * Creates a query comparison using the IN operator.
	 *
	 * <p>
	 * Use this comparison to generate a BooleanExpression that indicates whether
	 * the current StringExpression is included in the supplied values.
	 *
	 * @param possibleValues	possibleValues
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isInCollection(Collection<StringResult> possibleValues) {
		final BooleanExpression isInExpression
				= new BooleanExpression(new StringIsInExpression(this, possibleValues));
		if (isInExpression.getIncludesNull()) {
			return BooleanExpression.anyOf(new BooleanExpression(this.isNull()), isInExpression);
		} else {
			return isInExpression;
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression of the SQL comparison.
	 */
	@Override
	public BooleanExpression isNotInCollection(Collection<StringResult> possibleValues) {
		final BooleanExpression isNotInExpression
				= new BooleanExpression(new StringIsNotInExpression(this, possibleValues));
		if (isNotInExpression.getIncludesNull()) {
			return BooleanExpression.noneOf(new BooleanExpression(this.isNull()), isNotInExpression);
		} else {
			return isNotInExpression;
		}
	}

	/**
	 * Creates a query expression that appends the supplied value to the current
	 * StringExpression.
	 *
	 * @param string2	string2
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public StringExpression append(StringResult string2) {
		return new StringExpression(new StringAppendExpression(this, string2));
	}

	/**
	 * Creates a query expression that appends the supplied value to the current
	 * StringExpression.
	 *
	 * @param string2	string2
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public StringExpression append(Number number1) {
		return this.append(NumberExpression.value(number1));
	}

	/**
	 * Creates a query expression that aggregates the current StringExpression
	 * into a single string value.
	 *
	 * <p>
	 * For a table with rows like {"First"} and {"Second"}, using aggregate(", ")
	 * will return "First, Second".</p>
	 *
	 * <p>
	 * Like all aggregates, return values will vary based on the other fields
	 * returned by the queries. For instance if the table has rows like {1,
	 * "First", 3} and {2, "Second", 3} and the query returns all the columns and
	 * the string aggregate, the result will be {1, "First", 3, "First"} and {2,
	 * "Second", 3, "Second"}. However if only the third column and the string
	 * aggregate are returned then the result will be {3, "First, Second"}. Use {@link DBRow#setReturnFields(nz.co.gregs.dbvolution.columns.ColumnProvider...)}, {@link DBRow#setReturnFieldsToNone()
	 * }, and similar methods to change the returned fields.
	 * </p>
	 *
	 * @param separator	separator to place between the values
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public StringExpression aggregate(String separator) {
		return new StringAggregateExpression(this, separator);
	}

	/**
	 * Creates a query expression that aggregates the current StringExpression
	 * into a single string value.
	 *
	 * <p>
	 * For a table with rows like {"First"} and {"Second"}, using aggregate(", ")
	 * will return "First, Second".</p>
	 *
	 * <p>
	 * Like all aggregates, return values will vary based on the other fields
	 * returned by the queries. For instance if the table has rows like {1,
	 * "First", 3} and {2, "Second", 3} and the query returns all the columns and
	 * the string aggregate, the result will be {1, "First", 3, "First"} and {2,
	 * "Second", 3, "Second"}. However if only the third column and the string
	 * aggregate are returned then the result will be {3, "First, Second"}. Use {@link DBRow#setReturnFields(nz.co.gregs.dbvolution.columns.ColumnProvider...)}, {@link DBRow#setReturnFieldsToNone()
	 * }, and similar methods to change the returned fields.
	 * </p>
	 *
	 * @param separator	separator placed between all values
	 * @param orderBy the sort ordering to use in the aggregator
	 * @return a StringExpression.
	 */
	public StringExpression aggregate(String separator, SortProvider orderBy) {
		return new StringAggregateWithOrderByExpression(
				this,
				separator,
				orderBy
		);
	}

	/**
	 * Convenience method to replace any null results from the expression into
	 * empty strings.
	 *
	 * @return a StringExpression.
	 */
	public StringExpression replaceNullWithEmptyString() {
		return this.replace(AnyExpression.nullString(), StringExpression.emptyString());
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
		return this.replace(StringExpression.value(findString), StringExpression.value(replaceWith));
	}

	/**
	 * Creates a query expression that replaces the supplied value within the
	 * current StringExpression.
	 *
	 * @param findString findString
	 * @param replaceWith replaceWith
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public StringExpression replace(StringResult findString, StringResult replaceWith) {
		StringResult replaceValue = replaceWith;
		if (replaceWith.getIncludesNull()) {
			replaceValue = StringExpression.value("");
		}
		return new StringExpression(
				new StringReplaceExpression(
						this,
						StringExpression.value(findString),
						StringExpression.value(replaceValue)));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a string expression
	 */
	public StringExpression substringBefore(StringResult splitBeforeThis) {
		return new StringExpression(new StringSubstringBeforeExpression(this, new StringExpression(splitBeforeThis)));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a string expression
	 */
	public StringExpression substringAfter(StringResult splitAfterThis) {
		return new StringExpression(new StringSubstringAfterExpression(this, new StringExpression(splitAfterThis)));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a string expression
	 */
	public StringExpression substringBetween(StringResult splitAfterThis, StringResult butBeforeThis) {
		return substringAfter(splitAfterThis).substringBefore(butBeforeThis);
	}

	/**
	 * Creates a query expression that trims all leading and trailing spaces from
	 * the current StringExpression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression.
	 */
	public StringExpression trim() {
		return new StringExpression(
				new StringTrimExpression(this));
	}

	/**
	 * Tests that a string expression is shorter than or equal to the specified
	 * length.
	 *
	 * <p>
	 * This method is useful to test strings will fit within a specific field
	 * size</p>
	 *
	 * @param maxLength the longest possible number of characters
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression.
	 */
	public BooleanExpression isShorterThanOrAsLongAs(int maxLength) {
		return this.length().isLessThanOrEqual(maxLength);
	}

	/**
	 * Finds and returns the first number in the string or NULL if no number is
	 * found.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression.
	 */
	public StringExpression getFirstNumberAsSubstring() {
		StringExpression exp = new StringExpression(
				new StringFirstNumberAsSubstringExpression(this));
		return exp;
	}

	/**
	 * Finds and returns the first integer in the string or NULL if no integer is
	 * found.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression.
	 */
	public StringExpression getFirstIntegerAsSubstring() {
		final StringExpression exp = new StringExpression(
				new StringFirstIntegerAsSubstringExpression(this));
		return exp;

	}

	/**
	 * Finds and returns the first number in the string or NULL if no number is
	 * found.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression.
	 */
	public NumberExpression getFirstNumber() {
		return getFirstNumberAsSubstring().numberResult();
	}

	/**
	 * Finds and returns the first integer in the string or NULL if no integer is
	 * found.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an IntegerExpression.
	 */
	public IntegerExpression getFirstInteger() {
		return getFirstIntegerAsSubstring().integerResult();

	}

	/**
	 * Creates a query expression that trims all leading spaces from the current
	 * StringExpression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression.
	 */
	public StringExpression leftTrim() {
		return new StringExpression(
				new StringLeftTrimExpression(this));
	}

	/**
	 * Creates a query expression that trims all trailing spaces from the current
	 * StringExpression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression.
	 */
	public StringExpression rightTrim() {
		return new StringExpression(
				new StringRightTrimExpression(this));
	}

	/**
	 * Creates a query expression that changes all the letters in the current
	 * StringExpression to lowercase.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression.
	 */
	public StringExpression lowercase() {
		return new StringExpression(
				new StringLowercaseExpression(this));
	}

	/**
	 * Creates a query expression that changes all the letters in the current
	 * StringExpression to UPPERCASE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression.
	 */
	public StringExpression uppercase() {
		return new StringExpression(
				new StringUppercaseExpression(this));
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end of the string.
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based	startingIndex0Based
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(Integer startingIndex0Based) {
		return substring(NumberExpression.value(startingIndex0Based));
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end of the string.
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based	startingIndex0Based
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(Long startingIndex0Based) {
		return substring(NumberExpression.value(startingIndex0Based));
	}

	/**
	 * Create a substring of the current StringExpression starting from the
	 * position supplied and continuing until the end of the string.
	 *
	 * <p>
	 * The first character is at position zero (0).
	 *
	 * @param startingIndex0Based	startingIndex0Based
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(IntegerResult startingIndex0Based) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(Integer startingIndex0Based, Integer endIndex0Based) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(Long startingIndex0Based, Long endIndex0Based) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(IntegerResult startingIndex0Based, Integer endIndex0Based) {
		return new Substring(this, startingIndex0Based, value(endIndex0Based));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(IntegerResult startingIndex0Based, Long endIndex0Based) {
		return new Substring(this, startingIndex0Based, value(endIndex0Based));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(Integer startingIndex0Based, IntegerResult endIndex0Based) {
		return new Substring(this, value(startingIndex0Based), endIndex0Based);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(Long startingIndex0Based, IntegerResult endIndex0Based) {
		return new Substring(this, value(startingIndex0Based), endIndex0Based);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression
	 */
	public StringExpression substring(IntegerResult startingIndex0Based, IntegerResult endIndex0Based) {
		return new Substring(this, startingIndex0Based, endIndex0Based);
	}

	/**
	 * Create a expression that returns the length of the current expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression of the expression's length.
	 */
	public IntegerExpression length() {
		return new IntegerExpression(
				new IntegerLengthExpression(this));
	}

	/**
	 * Create a {@link StringExpression} that returns the name of the current
	 * user.
	 *
	 * <p>
	 * This should be the current username under which the application is
	 * accessing the database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression
	 */
	public static StringExpression currentUser() {
		return new StringExpression(
				new StringCurrentUserExpression());
	}

	/**
	 * Returns the 1-based index of the first occurrence of searchString within
	 * the StringExpression.
	 *
	 * <p>
	 * The index is 1-based, and returns 0 when the searchString is not found.</p>
	 *
	 * @param searchString	searchString
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will find the location of the searchString.
	 */
	public IntegerExpression locationOf(String searchString) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an expression that will find the location of the searchString.
	 */
	public IntegerExpression locationOf(StringResult searchString) {
		return new NumberExpression(new StringLocationOfExpression(this, searchString)).integerResult();
	}

	/**
	 * Creates an expression that will find the largest value in the column
	 * supplied.
	 *
	 * <p>
	 * Max is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String expression.
	 */
	public StringExpression max() {
		return new StringExpression(
				new StringMaxExpression(this));
	}

	/**
	 * Creates an expression that will find the smallest value in the column
	 * supplied.
	 *
	 * <p>
	 * Min is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String expression.
	 */
	public StringExpression min() {
		return new StringExpression(new StringMinExpression(this));
	}

	@Override
	public DBString getQueryableDatatypeForExpressionValue() {
		return new DBString();
	}

	/**
	 * Creates a BooleanExpression that tests to ensure the database value is not
	 * a database NULL value.
	 *
	 * <P>
	 * The expression will be true if the value is not NULL, and false otherwise.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNotNullAndNotEmpty() {
		return BooleanExpression.allOf(
				BooleanExpression.isNotNull(this),
				this.isNotEmpty()
		);
	}

	/**
	 * Creates a BooleanExpression that tests to ensure the database value is a
	 * database NULL value.
	 *
	 * <P>
	 * The expression will be true if the value is NULL, and false otherwise.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNullOrEmpty() {
		return BooleanExpression.anyOf(
				BooleanExpression.isNull(this),
				this.isEmpty()
		);
	}

	/**
	 * Adds an explicit bracket at this point in the expression chain.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression that will have the current expression wrapped in
	 * brackets.
	 */
	public StringExpression bracket() {
		return new StringExpression(new StringBracketExpression(this));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isInIgnoreCase(String... potentialValues) {
		return this.isInIgnoreCase(expressions(potentialValues));
	}

	public StringResult[] expressions(String... potentialValues) {
		List<StringResult> possVals = new ArrayList<>(0);
		for (String str : potentialValues) {
			if (str == null) {
				possVals.add(nullString());
			} else {
				possVals.add(StringExpression.value(str));
			}
		}
		return possVals.toArray(new StringResult[]{});
	}

	public StringResult[] expressions(Collection<String> potentialValues) {
		List<StringResult> possVals = new ArrayList<>(0);
		for (String str : potentialValues) {
			if (str == null) {
				possVals.add(nullString());
			} else {
				possVals.add(StringExpression.value(str));
			}
		}
		return possVals.toArray(new StringResult[]{});
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isInIgnoreCase(Collection<String> potentialValues) {
		return this.isInIgnoreCase(expressions(potentialValues));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isInIgnoreCase(StringResult... potentialValues) {
		List<StringResult> lowerStrings = new ArrayList<>();
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
	 * You should probably use {@link #getFirstNumber() } or {@link #getFirstNumber()
	 * } instead as they are more reliable.</p>
	 *
	 * <p>
	 * Uses the database's own facilities to parse the value of this expression
	 * into a number.
	 *
	 * <p>
	 * May return NULL and all sorts of crazy things.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public NumberExpression numberResult() {
		return new NumberExpression(
				new StringNumberResultExpression(this));
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression
	 */
	public IntegerExpression integerResult() {
		return this.numberResult().isNotNull()
				.ifThenElse(this.numberResult().integerResult(), nullInteger());
	}

	@Override
	public DBString asExpressionColumn() {
		return new DBString(this);
	}

	@Override
	public StringExpression stringResult() {
		return this;
	}

	@Override
	public StringResult expression(DBString value) {
		return new StringExpression(value);
	}

	public StringExpression prepend(String string) {
		return StringExpression.value(string).append(this);
	}

	public StringExpression prepend(StringResult string) {
		return value(string).append(this);
	}

	public StringExpression append(IntegerExpression value) {
		return append(value.stringResult());
	}

	public StringExpression leftPad(String padWith, int ensureLength) {
		return new LeftPadExpression(this, AnyExpression.value(padWith), AnyExpression.value(ensureLength));
	}

	public StringExpression leftPad(StringExpression padWith, IntegerExpression ensureLength) {
		return new LeftPadExpression(this, padWith, ensureLength);
	}

	private static class LeftPadExpression extends StringExpression {

		private final StringExpression string;
		private final IntegerExpression length;
		private final StringExpression padding;

		public LeftPadExpression(StringExpression string, StringExpression padding, IntegerExpression i) {
			super();
			this.string = string;
			this.padding = padding;
			this.length = i;
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.supportsLeftPadTransform()) {
				return db.doLeftPadTransform(this.string.toSQLString(db), this.padding.toSQLString(db), this.length.toSQLString(db));
			} else {
        final Encoder encoder = Builder
                .forSeparator(", ")
                .startsWith("LPAD_SUBSTITUTE_REQUIRED(")
                .withSuffix(")")
                .encoder();
        encoder.addAll(this.string.toSQLString(db), this.padding.toSQLString(db), this.length.toSQLString(db));
				return "LPAD_SUBSTITUTE_REQUIRED(" + encoder.encode();
			}
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public LeftPadExpression copy() {
			return new LeftPadExpression(
					string == null ? null : string.copy(),
					padding == null ? null : padding.copy(),
					length == null ? null : length.copy()
			);
		}
	}

	private static abstract class DBBinaryStringArithmetic extends StringExpression {

		private static final long serialVersionUID = 1L;

		protected StringResult first;
		protected StringResult second;

		DBBinaryStringArithmetic(StringResult first, StringResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
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
			return this.first.isAggregator() || second.isAggregator();
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

	private static abstract class DBTrinaryStringFunction extends StringExpression {

		private static final long serialVersionUID = 1L;

		private StringExpression first;
		private StringExpression second;
		private StringExpression third;

		DBTrinaryStringFunction(StringExpression first) {
			this.first = first;
			this.second = null;
			this.third = null;
		}

		DBTrinaryStringFunction(StringExpression first, StringExpression second) {
			this.first = first;
			this.second = second;
		}

		DBTrinaryStringFunction(StringExpression first, StringExpression second, StringExpression third) {
			this.first = first;
			this.second = second;
			this.third = third;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + getFirst().toSQLString(db)
					+ this.getSeparator(db) + (getSecond() == null ? "" : getSecond().toSQLString(db))
					+ this.getSeparator(db) + (getThird() == null ? "" : getThird().toSQLString(db))
					+ this.afterValue(db);
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
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		protected StringExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected StringExpression getSecond() {
			return second;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the third
		 */
		protected StringExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null && third == null) {
				return true;
			} else if (first == null) {
				return second.isPurelyFunctional();
			} else if (second == null) {
				return first.isPurelyFunctional();
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
			}
		}
	}

	protected static abstract class DBBinaryStringFunction extends StringExpression {

		private static final long serialVersionUID = 1L;

		protected StringExpression first;
		protected StringExpression second;

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

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		protected StringExpression getFirst() {
			return first;
		}

		/**
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the second
		 */
		protected StringExpression getSecond() {
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

	private static abstract class BinaryComplicatedNumberFunction extends NumberExpression {

		private static final long serialVersionUID = 1L;

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
		public abstract String toSQLString(DBDefinition db);

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

	private static class Substring extends StringExpression {

		private final static long serialVersionUID = 1l;

		private IntegerResult startingPosition;
		private IntegerResult length;

		Substring(StringResult stringInput, Long startingIndex0Based) {
			super(stringInput);
			this.startingPosition = value(startingIndex0Based);
			this.length = new StringExpression(stringInput).length();
		}

		Substring(StringResult stringInput, Integer startingIndex0Based) {
			super(stringInput);
			this.startingPosition = value(startingIndex0Based);
			this.length = new StringExpression(stringInput).length();
		}

		Substring(StringResult stringInput, IntegerResult startingIndex0Based) {
			super(stringInput);
			this.startingPosition = startingIndex0Based;
			this.length = value(stringInput).length();
		}

		Substring(StringResult stringInput, Long startingIndex0Based, Long endIndex0Based) {
			super(stringInput);
			this.startingPosition = value(startingIndex0Based);
			this.length = value(endIndex0Based);
		}

		Substring(StringResult stringInput, Integer startingIndex0Based, Integer endIndex0Based) {
			super(stringInput);
			this.startingPosition = value(startingIndex0Based);
			this.length = value(endIndex0Based);
		}

		Substring(StringResult stringInput, IntegerResult startingIndex0Based, IntegerResult endIndex0Based) {
			super(stringInput);
			this.startingPosition = startingIndex0Based;
			this.length = endIndex0Based;
		}

		private Substring(ExpressionHasStandardStringResult stringInput, IntegerResult startingIndex0Based, IntegerResult endIndex0Based) {
			super(stringInput.stringResult());
			this.startingPosition = startingIndex0Based;
			this.length = endIndex0Based;
		}

		@Override
		public Substring copy() {
			return new Substring(
					getInnerResult() == null ? null : (ExpressionHasStandardStringResult) getInnerResult().copy(),
					startingPosition == null ? null : startingPosition.copy(),
					length == null ? null : length.copy()
			);
		}

		@Override
		public String toSQLString(DBDefinition db) {
			if (getInnerResult() == null) {
				return "";
			} else {
				return doSubstringTransform(db, (ExpressionHasStandardStringResult) getInnerResult(), startingPosition, length);
			}
		}

		public String doSubstringTransform(DBDefinition db, ExpressionHasStandardStringResult enclosedValue, IntegerResult startingPosition, IntegerResult substringLength) {
			return db.doSubstringTransform(
					enclosedValue.stringResult().toSQLString(db),
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
			final AnyResult<?> string1 = getInnerResult();
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

		private static final long serialVersionUID = 1L;

		protected final StringResult first;
		protected final StringResult second;

		DBBinaryBooleanArithmetic(StringResult first, StringResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
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
	}

	private static abstract class DBNnaryBooleanFunction extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		protected final StringExpression column;
		protected final List<StringResult> values = new ArrayList<>();
		private final boolean includesNulls;

		DBNnaryBooleanFunction() {
			column = null;
			includesNulls = false;
		}

		DBNnaryBooleanFunction(StringExpression leftHandSide, Collection<StringResult> rightHandSide) {
			this.column = leftHandSide;
			boolean nulls = false;
			for (StringResult stringResult : rightHandSide) {
				if (stringResult == null) {
					nulls = true;
				} else if (stringResult.getIncludesNull()) {
					nulls = true;
				} else {
					values.add(stringResult);
				}
			}
			includesNulls = nulls;
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
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			values.stream()
					.filter(second -> (second != null))
					.forEachOrdered(second -> hashSet.addAll(second.getTablesInvolved()));
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
	}

	private static class NullStringExpression extends StringExpression {

		public NullStringExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			if (db.requiredToProduceEmptyStringsForNull()) {
				return db.getEmptyString();
			} else {
				return db.getNull();
			}
		}

		@Override
		public boolean getIncludesNull() {
			return true;
		}

		@Override
		public NullStringExpression copy() {
			return new NullStringExpression();
		}
	}

	private static class EmptyStringExpression extends StringExpression {

		public EmptyStringExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getEmptyString();
		}

		@Override
		public boolean getIncludesNull() {
			return true;
		}

		@Override
		public EmptyStringExpression copy() {
			return new EmptyStringExpression();
		}
	}

	protected class StringIfDBNullExpression extends DBBinaryStringFunction {

		public StringIfDBNullExpression(StringExpression first, StringExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doStringIfNullTransform(this.getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIfDBNullExpression copy() {
			return new StringIfDBNullExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy());
		}
	}

	protected static class StringIsLikeExpression extends DBBinaryBooleanArithmetic {

		public StringIsLikeExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " LIKE ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIsLikeExpression copy() {
			return new StringIsLikeExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class StringIsExpression extends DBBinaryBooleanArithmetic {

		public StringIsExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doStringEqualsTransform(super.first.toSQLString(db), super.second.toSQLString(db));
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " = ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIsExpression copy() {
			return new StringIsExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class StringIsNotExpression extends DBBinaryBooleanArithmetic {

		public StringIsNotExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return (new StringExpression(first)).ifDBNull("<DBV NULL PROTECTION>").toSQLString(db)
					+ this.getEquationOperator(db)
					+ (new StringExpression(second)).ifDBNull("<DBV NULL PROTECTION>").toSQLString(db);
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <> ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIsNotExpression copy() {
			return new StringIsNotExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected static class StringIsLessThanExpression extends DBBinaryBooleanArithmetic {

		public StringIsLessThanExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " < ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIsLessThanExpression copy() {
			return new StringIsLessThanExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected static class StringIsLessThanOrEqualExpression extends DBBinaryBooleanArithmetic {

		public StringIsLessThanOrEqualExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " <= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIsLessThanOrEqualExpression copy() {
			return new StringIsLessThanOrEqualExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected static class StringIsGreaterThanExpression extends DBBinaryBooleanArithmetic {

		public StringIsGreaterThanExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " > ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIsGreaterThanExpression copy() {
			return new StringIsGreaterThanExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected static class StringIsGreaterThanOrEqualExpression extends DBBinaryBooleanArithmetic {

		public StringIsGreaterThanOrEqualExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return " >= ";
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public StringIsGreaterThanOrEqualExpression copy() {
			return new StringIsGreaterThanOrEqualExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	protected class StringIsInExpression extends DBNnaryBooleanFunction {

		public StringIsInExpression(StringExpression leftHandSide, Collection<StringResult> rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<>();
			values.stream()
					.filter(value -> (!value.getIncludesNull()))
					.forEachOrdered(value -> sqlValues.add(value.toSQLString(db)));
			return db.doInTransform(column.toSQLString(db), sqlValues);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return " IN ";
		}

		@Override
		public StringIsInExpression copy() {
			return new StringIsInExpression(
					column == null ? null : column.copy(),
					values
			);
		}
	}

	protected class StringIsNotInExpression extends DBNnaryBooleanFunction {

		public StringIsNotInExpression(StringExpression leftHandSide, Collection<StringResult> rightHandSide) {
			super(leftHandSide, rightHandSide);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			List<String> sqlValues = new ArrayList<>();
			values.stream()
					.filter(value -> (!value.getIncludesNull()))
					.forEachOrdered(value -> sqlValues.add(value.toSQLString(db)));
			return db.doNotInTransform(column.toSQLString(db), sqlValues);
		}

		@Override
		protected String getFunctionName(DBDefinition db) {
			return " NOT IN ";
		}

		@Override
		public StringIsNotInExpression copy() {
			return new StringIsNotInExpression(
					column == null ? null : column.copy(),
					values
			);
		}
	}

	protected class StringAppendExpression extends DBBinaryStringArithmetic {

		public StringAppendExpression(StringResult first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doConcatTransform(super.first.toSQLString(db), super.second.toSQLString(db));
		}

		@Override
		protected String getEquationOperator(DBDefinition db) {
			return "";
		}

		@Override
		public StringAppendExpression copy() {
			return new StringAppendExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	public static class StringAggregateExpression extends StringExpression implements CanBeWindowingFunctionWithFrame<StringExpression> {

		public StringAggregateExpression(StringExpression columnToAccumulate, String separator) {
			this.column = columnToAccumulate;
			this.separator = separator;
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doStringAccumulateTransform(column.toSQLString(db),
					separator,
					column.getTablesInvolved().toArray(new DBRow[]{})[0].getTableName());
		}

		@Override
		public StringAggregateExpression copy() {
			return new StringAggregateExpression(
					column == null ? null : column.copy(),
					separator == null ? null : separator
			);
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public WindowFunctionFramable<StringExpression> over() {
			return new WindowFunctionFramable<>(new StringExpression(this));
		}
		protected StringExpression column;
		protected String separator;

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (column == null) {
				return true;
			} else {
				return column.isPurelyFunctional();
			}
		}
	}

	public static class StringAggregateWithOrderByExpression extends StringExpression implements CanBeWindowingFunctionWithFrame<StringExpression> {

		private final static long serialVersionUID = 1l;
		
		private final StringExpression columnToAccumulate;
		private final String separator;
		private final SortProvider orderBy;

		public StringAggregateWithOrderByExpression(StringExpression columnToAccumulate, String separator, SortProvider orderBy) {
			super();
			this.columnToAccumulate = columnToAccumulate;
			this.separator = separator;
			this.orderBy = orderBy.copy();
			orderBy
					.getTablesInvolved()
					.forEach(table -> table.setSortedSubselectRequired(orderBy));
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doStringAccumulateTransform(
					columnToAccumulate,
					separator,
					orderBy
			);
		}

		@Override
		public StringAggregateWithOrderByExpression copy() {
			return new StringAggregateWithOrderByExpression(
					columnToAccumulate == null ? StringExpression.nullString() : columnToAccumulate.copy(),
					separator == null ? null : separator,
					orderBy == null ? null : orderBy.copy()
			);
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public WindowFunctionFramable<StringExpression> over() {
			return new WindowFunctionFramable<>(new StringExpression(this));
		}
	}

	protected static class StringReplaceExpression extends DBTrinaryStringFunction {

		public StringReplaceExpression(StringExpression first, StringExpression second, StringExpression third) {
			super(first, second, third);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doReplaceTransform(
					this.getFirst().toSQLString(db),
					this.getSecond().toSQLString(db),
					this.getThird().toSQLString(db));
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "REPLACE";
		}

		@Override
		public boolean getIncludesNull() {
			// handled before creation
			return false;
		}

		@Override
		public StringReplaceExpression copy() {
			return new StringReplaceExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy(),
					getThird() == null ? null : getThird().copy()
			);
		}
	}

	protected static class StringSubstringBeforeExpression extends DBBinaryStringFunction {

		public StringSubstringBeforeExpression(StringExpression first, StringExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			try {
				return db.doSubstringBeforeTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				return getFirst().locationOf(getSecond()).isGreaterThan(0).ifThenElse(getFirst().substring(0, getFirst().locationOf(getSecond()).minus(1).integerResult()), value("")).toSQLString(db);
			}
		}

		@Override
		public StringSubstringBeforeExpression copy() {
			StringSubstringBeforeExpression newInstance;
			newInstance
					= new StringSubstringBeforeExpression(
							getFirst() == null ? null : getFirst().copy(),
							getSecond() == null ? null : getSecond().copy()
					);
			return newInstance;
		}
	}

	protected static class StringSubstringAfterExpression extends DBBinaryStringFunction {

		public StringSubstringAfterExpression(StringExpression first, StringExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			try {
				return db.doSubstringAfterTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				return getFirst().locationOf(getSecond()).isGreaterThan(0).ifThenElse(getFirst().substring(getFirst().locationOf(getSecond()).integerResult(), getFirst().length()), value("")).toSQLString(db);
			}
		}

		@Override
		public StringSubstringAfterExpression copy() {
			StringSubstringAfterExpression newInstance;
			newInstance
					= new StringSubstringAfterExpression(
							getFirst() == null ? null : getFirst().copy(),
							getSecond() == null ? null : getSecond().copy()
					);
			return newInstance;
		}
	}

	protected static class StringTrimExpression extends StringExpression {

		public StringTrimExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doTrimFunction(this.getInnerResult().toSQLString(db));
		}

		@Override
		public StringTrimExpression copy() {
			return new StringTrimExpression(
					(StringResult) (getInnerResult() == null ? null : getInnerResult().copy())
			);
		}
	}

	protected static class StringFirstNumberAsSubstringExpression extends StringExpression {

		public StringFirstNumberAsSubstringExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doFindNumberInStringTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public StringFirstNumberAsSubstringExpression copy() {
			return new StringFirstNumberAsSubstringExpression(
					(StringResult) (getInnerResult() == null ? null : getInnerResult().copy())
			);
		}
	}

	protected static class StringFirstIntegerAsSubstringExpression extends StringExpression {

		public StringFirstIntegerAsSubstringExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doFindIntegerInStringTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public StringFirstIntegerAsSubstringExpression copy() {
			return new StringFirstIntegerAsSubstringExpression(
					(StringResult) (getInnerResult() == null ? null : getInnerResult().copy())
			);
		}
	}

	protected static class StringLeftTrimExpression extends StringExpression {

		public StringLeftTrimExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doLeftTrimTransform(this.getInnerResult().toSQLString(db));
		}

		@Override
		public StringLeftTrimExpression copy() {
			return new StringLeftTrimExpression((StringResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	protected static class StringRightTrimExpression extends StringExpression {

		public StringRightTrimExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition defn) {
			return defn.getRightTrimFunctionName() + "(" + getInnerResult().toSQLString(defn) + ")";
		}

		@Override
		public StringRightTrimExpression copy() {
			return new StringRightTrimExpression((StringResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	protected static class StringLowercaseExpression extends StringExpression {

		public StringLowercaseExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition defn) {
			return defn.getLowercaseFunctionName() + "(" + getInnerResult().toSQLString(defn) + ")";
		}

		@Override
		public StringLowercaseExpression copy() {
			return new StringLowercaseExpression((StringResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	protected static class StringUppercaseExpression extends StringExpression {

		public StringUppercaseExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition defn) {
			return defn.getUppercaseFunctionName() + "(" + getInnerResult().toSQLString(defn) + ")";
		}

		@Override
		public StringUppercaseExpression copy() {
			return new StringUppercaseExpression((StringResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	protected static class IntegerLengthExpression extends IntegerExpression {

		public IntegerLengthExpression(AnyResult<?> only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doStringLengthTransform(getInnerResult().toSQLString(db));
		}

		@Override
		public IntegerLengthExpression copy() {
			return new IntegerLengthExpression((AnyResult<?>) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	protected static class StringCurrentUserExpression extends StringExpression {

		public StringCurrentUserExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getCurrentUserFunctionName();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			return true;
		}

		@Override
		public StringCurrentUserExpression copy() {
			return new StringCurrentUserExpression();
		}
	}

	protected static class StringLocationOfExpression extends BinaryComplicatedNumberFunction {

		public StringLocationOfExpression(StringExpression first, StringResult second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doPositionInStringTransform(this.first.toSQLString(db), this.second.toSQLString(db));
		}

		@Override
		public StringLocationOfExpression copy() {
			return new StringLocationOfExpression(
					first == null ? null : first.copy(),
					second == null ? null : second.copy()
			);
		}
	}

	public static class StringMaxExpression extends StringExpression implements CanBeWindowingFunctionWithFrame<StringExpression> {

		public StringMaxExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition defn) {
			return defn.getMaxFunctionName() + "(" + getInnerResult().toSQLString(defn) + ")";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public StringMaxExpression copy() {
			return new StringMaxExpression((StringResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}

		@Override
		public WindowFunctionFramable<StringExpression> over() {
			return new WindowFunctionFramable<>(new StringExpression(this));
		}
	}

	public static class StringMinExpression extends StringExpression implements CanBeWindowingFunctionWithFrame<StringExpression> {

		public StringMinExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition defn) {
			return defn.getMinFunctionName() + "(" + getInnerResult().toSQLString(defn) + ")";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public StringMinExpression copy() {
			return new StringMinExpression((StringResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}

		@Override
		public WindowFunctionFramable<StringExpression> over() {
			return new WindowFunctionFramable<>(new StringExpression(this));
		}
	}

	protected static class StringBracketExpression extends StringExpression {

		public StringBracketExpression(StringResult stringVariable) {
			super(stringVariable);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition defn) {
			return "(" + getInnerResult().toSQLString(defn) + ")";
		}

		@Override
		public StringBracketExpression copy() {
			return new StringBracketExpression((StringResult) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	protected static class StringNumberResultExpression extends NumberExpression {

		public StringNumberResultExpression(AnyResult<?> value) {
			super(value);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.doStringToNumberTransform(getInnerResult().toSQLString(db));
		}

		@Override
		public StringNumberResultExpression copy() {
			return new StringNumberResultExpression((AnyResult<?>) (getInnerResult() == null ? null : getInnerResult().copy()));
		}
	}

	public static WindowFunctionFramable<StringExpression> firstValue() {
		return new FirstValueExpression().over();
	}

	public static class FirstValueExpression extends StringExpression implements CanBeWindowingFunctionWithFrame<StringExpression> {

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
		public WindowFunctionFramable<StringExpression> over() {
			return new WindowFunctionFramable<StringExpression>(new StringExpression(this));
		}

	}

	public static WindowFunctionFramable<StringExpression> lastValue() {
		return new LastValueExpression().over();
	}

	public static class LastValueExpression extends StringExpression implements CanBeWindowingFunctionWithFrame<StringExpression> {

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
		public StringExpression copy() {
			return new LastValueExpression();
		}

		@Override
		public WindowFunctionFramable<StringExpression> over() {
			return new WindowFunctionFramable<StringExpression>(new StringExpression(this));
		}

	}

	public static WindowFunctionFramable<StringExpression> nthValue(IntegerExpression indexExpression) {
		return new NthValueExpression(indexExpression).over();
	}

	public static class NthValueExpression extends StringExpression implements CanBeWindowingFunctionWithFrame<StringExpression> {

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
		public WindowFunctionFramable<StringExpression> over() {
			return new WindowFunctionFramable<StringExpression>(new StringExpression(this));
		}
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
	public LagLeadWindow lag() {
		return lag(IntegerExpression.value(1));
	}

	/**
	 *
	 * Synonym for lag.
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
	public LagLeadWindow previousRowValue() {
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
	public LagLeadWindow lag(IntegerExpression offset) {
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
	public LagLeadWindow lag(IntegerExpression offset, StringExpression defaultExpression) {
		return new LagExpression(this, offset, defaultExpression).over();
	}

	/**
	 * Synonym for lead.
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
	public LagLeadWindow nextRowValue() {
		return lead();
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
	public LagLeadWindow lead() {
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
	public LagLeadWindow lead(IntegerExpression offset) {
		return lead(offset, nullExpression());
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
	public LagLeadWindow lead(IntegerExpression offset, StringExpression defaultExpression) {
		return new LeadExpression(this, offset, defaultExpression).over();
	}

	private static abstract class LagLeadExpression extends StringExpression implements CanBeWindowingFunctionRequiresOrderBy<StringExpression> {

		private static final long serialVersionUID = 1L;

		protected StringExpression first;
		protected IntegerExpression second;
		protected StringExpression third;

		LagLeadExpression(StringExpression first, IntegerExpression second, StringExpression third) {
			this.first = first;
			this.second = second == null ? value(1) : second;
			this.third = third == null ? nullString() : third;
		}

		@Override
		public DBString getQueryableDatatypeForExpressionValue() {
			return new DBString();
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
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the first
		 */
		protected StringExpression getFirst() {
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

		/**
		 *
		 * @return the third
		 */
		protected StringExpression getThird() {
			return third;
		}

		@Override
		public boolean isPurelyFunctional() {
			return first.isPurelyFunctional() && second.isPurelyFunctional() && third.isPurelyFunctional();
		}

		@Override
		public LagLeadWindow over() {
			return new LagLeadWindow(new StringExpression(this));
		}
	}

	public static class LagLeadWindow extends WindowFunctionRequiresOrderBy<StringExpression> {

		public LagLeadWindow(StringExpression expression) {
			super(expression);
		}
	}

	public static class LagExpression extends LagLeadExpression {

		private static final long serialVersionUID = 1L;

		public LagExpression(StringExpression first, IntegerExpression second, StringExpression third) {
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

	public static class LeadExpression extends LagLeadExpression {

		private static final long serialVersionUID = 1L;

		public LeadExpression(StringExpression first, IntegerExpression second, StringExpression third) {
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
