/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.operators.*;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.utility.comparators.ComparableComparator;

/**
 * Encapsulates database values that are strings of characters.
 *
 * <p>
 * Use DBString when the column is a {@code char}, {@code varchar}, or
 * {@code nvarchar} datatype.
 *
 * <p>
 * Generally DBString is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBString myCharColumn = new DBString();}
 *
 * <p>
 * If the string column encodes repeating information like a status code, use
 * {@link DBStringEnum} to encapsulate the values.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBString extends QueryableDatatype<String> implements StringResult {

	private static final long serialVersionUID = 1L;
	private boolean isDBEmptyString = false;

	/**
	 * Utility function to return the values of a list of DBStrings in a list of
	 * Strings.
	 *
	 * @param dbStrings	dbStrings
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the defined values of all the DBStrings.
	 */
	public static List<String> toStringList(List<DBString> dbStrings) {
		ArrayList<String> strings = new ArrayList<>();
		for (DBString dBString : dbStrings) {
			strings.add(dBString.stringValue());
		}
		return strings;
	}

	/**
	 * Utility function to return the values of a list of Strings in a list of
	 * DBStrings.
	 *
	 * @param strings the strings to transformed into DBStrings
	 * @return the defined values of all the DBStrings.
	 */
	public static ArrayList<DBString> toDBStringList(List<String> strings) {
		ArrayList<DBString> dbStrings = new ArrayList<>();
		for (String string : strings) {
			dbStrings.add(new DBString(string));
		}
		return dbStrings;
	}

	/**
	 * The default constructor for DBString.
	 *
	 * <p>
	 * Creates an unset undefined DBString object.
	 *
	 */
	public DBString() {
		super();
	}

	/**
	 * Creates a DBString with the value provided.
	 *
	 * <p>
	 * The resulting DBString will be set as having the value provided but will
	 * not be defined in the database.
	 *
	 * @param string	string
	 */
	public DBString(String string) {
		super(string);
	}

	/**
	 * Creates a column expression with a string result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param stringExpression	stringExpression
	 */
	public DBString(StringExpression stringExpression) {
		super(stringExpression);
	}

	/**
	 * Sets the value of this DBString to the value provided.
	 *
	 * @param str	str
	 */
	@Override
	public void setValue(String str) {
		super.setLiteralValue(str);
	}

	@Override
	public String getValue() {
		final Object value = super.getValue();
		if (value == null) {
			return null;
		} else if (value instanceof String) {
			return (String) value;
		} else {
			return value.toString();
		}
	}

	@Override
	public String getSQLDatatype() {
		return "VARCHAR(1000)";
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition defn) {
		if (getLiteralValue().isEmpty()) {
			return defn.getEmptyString();
		} else {
			String unsafeValue = getLiteralValue();
			return defn.beginStringValue() + defn.safeString(unsafeValue) + defn.endStringValue();
		}
	}

	@Override
	public DBString copy() {
		return (DBString) super.copy();
	}

	@Override
	public DBString getQueryableDatatypeForExpressionValue() {
		return new DBString();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<>();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(String... permitted) {
		this.setOperator(new DBPermittedValuesOperator<String>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Object... permitted) {
		this.setOperator(new DBPermittedValuesOperator<Object>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Collection<String> permitted) {
		this.setOperator(new DBPermittedValuesOperator<String>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of Strings
	 * ignoring letter case.
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesIgnoreCase(String... permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of Strings
	 * ignoring letter case.
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesIgnoreCase(StringExpression... permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of Strings
	 * ignoring letter case.
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesIgnoreCase(List<String> permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of Strings
	 * ignoring letter case.
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesIgnoreCase(Set<String> permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
	}

	/**
	 * Reduces the rows to excluding the object, Set, List, Array, or vararg of
	 * Strings ignoring letter case.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(String... excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
		negateOperator();
	}

	/**
	 * Reduces the rows to excluding the object, Set, List, Array, or vararg of
	 * Strings ignoring letter case.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(StringExpression... excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
		negateOperator();
	}

	/**
	 * Reduces the rows to excluding the object, Set, List, Array, or vararg of
	 * Strings ignoring letter case.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(Collection<String> excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
		negateOperator();
	}

	/**
	 *
	 *
	 */
//	public void excludedValuesIgnoreCase(Set<String> excluded) {
//		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
//		negateOperator();
//	}
	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(String... excluded) {
		this.setOperator(new DBPermittedValuesOperator<String>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(Collection<String> excluded) {
		this.setOperator(new DBPermittedValuesOperator<String>(excluded));
		negateOperator();
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
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRange(String lowerBound, String upperBound) {
		setOperator(new DBPermittedRangeOperator<String>(lowerBound, upperBound));
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
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeInclusive(String lowerBound, String upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeExclusive(String lowerBound, String upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included
	 * within the range and the upper-bound excluded. I.e excludedRange(1,3) will
	 * exclude 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will exclude 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(String lowerBound, String upperBound) {
		setOperator(new DBPermittedRangeOperator<String>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included within the range. I.e excludedRangeInclusive(1,3) will
	 * exclude 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will exclude 5,4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeInclusive(String lowerBound, String upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded from the range. I.e excludedRangeExclusive(1,3) will
	 * exclude 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will exclude 2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will exclude 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeExclusive(String lowerBound, String upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Perform searches based on using database compatible pattern matching
	 *
	 * <p>
	 * This facilitates the LIKE operator.
	 *
	 * <p>
	 * Please use the pattern system appropriate to your database.
	 *
	 * <p>
	 * Java-style regular expressions are not yet supported.
	 *
	 * @param pattern	pattern
	 */
	public void permittedPattern(String pattern) {
		this.setOperator(new DBPermittedPatternOperator(pattern));
	}

	/**
	 * Perform searches based on using database compatible pattern matching
	 *
	 * <p>
	 * This facilitates the LIKE operator.
	 *
	 * <p>
	 * Please use the pattern system appropriate to your database.
	 *
	 * <p>
	 * Java-style regular expressions are not yet supported.
	 *
	 * @param pattern	pattern
	 */
	public void excludedPattern(String pattern) {
		this.setOperator(new DBPermittedPatternOperator(pattern));
		this.negateOperator();
	}

	/**
	 * Perform searches based on using database compatible pattern matching
	 *
	 * <p>
	 * This facilitates the LIKE operator.
	 *
	 * <p>
	 * Please use the pattern system appropriate to your database.
	 *
	 * <p>
	 * Java-style regular expressions are not yet supported.
	 *
	 * @param pattern	pattern
	 */
	public void permittedPattern(StringExpression pattern) {
		this.setOperator(new DBPermittedPatternOperator(pattern));
	}

	/**
	 * Perform searches based on using database compatible pattern matching
	 *
	 * <p>
	 * This facilitates the LIKE operator.
	 *
	 * <p>
	 * Please use the pattern system appropriate to your database.
	 *
	 * <p>
	 * Java-style regular expressions are not yet supported.
	 *
	 * @param pattern	pattern
	 */
	public void excludedPattern(StringExpression pattern) {
		this.setOperator(new DBPermittedPatternOperator(pattern));
		this.negateOperator();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(StringExpression... permitted) {
		this.setOperator(new DBPermittedValuesOperator<StringExpression>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(StringExpression... excluded) {
		this.setOperator(new DBPermittedValuesOperator<StringExpression>(excluded));
		negateOperator();
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
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRange(StringExpression lowerBound, StringExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<StringExpression>(lowerBound, upperBound));
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
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeInclusive(StringExpression lowerBound, StringExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void permittedRangeExclusive(StringExpression lowerBound, StringExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded from the range. I.e
	 * excludedRange(1,3) will exclude 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will exclude 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(StringExpression lowerBound, StringExpression upperBound) {
		setOperator(new DBPermittedRangeOperator<StringExpression>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the range. I.e excludedRangeInclusive(1,3) will exclude
	 * 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will exclude 5,4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeInclusive(StringExpression lowerBound, StringExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the range. I.e excludedRangeExclusive(1,3) will exclude
	 * 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will exclude 2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will exclude 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeExclusive(StringExpression lowerBound, StringExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	/**
	 * Indicates whether this DBString value is the SQL equivalent of a java empty
	 * String.
	 *
	 * <p>
	 * Some databases, notably Oracle, cannot differentiate between empty strings
	 * and NULL strings. This method helps programmers access the empty or NULL
	 * states will allowing DBV to provide a consistent interface.
	 *
	 * <p>
	 * In Oracle and similar databases isDBNull and isEmptyString will both be
	 * TRUE when the value is NULL. However most databases will have isEmptyString
	 * false when the string is null, and isDBNull false when the string is empty.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true if the database value represents an empty string, otherwise
	 * FALSE.
	 */
	public boolean isEmptyString() {
		return isDBEmptyString || (getLiteralValue() != null && getLiteralValue().isEmpty());
	}

	@Override
	protected String getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) throws SQLException {
		String gotString = resultSet.getString(fullColumnName);
		if (resultSet.wasNull() || gotString == null) {
			if (defn.requiredToProduceEmptyStringsForNull()) {
				return "";
			} else {
				return null;
			}
		} else {
			return gotString;
		}
	}

	/**
	 * Perform case-insensitive searches based on using database compatible
	 * pattern matching.
	 *
	 * <p>
	 * This facilitates the LIKE operator.
	 *
	 * <p>
	 * Please use the pattern system appropriate to your database.
	 *
	 * <p>
	 * Java-style regular expressions are not yet supported.
	 *
	 * @param pattern	pattern
	 */
	public void permittedPatternIgnoreCase(String pattern) {
		this.setOperator(new DBPermittedPatternIgnoreCaseOperator(pattern));
	}

	@Override
	protected DBOperator setToNull(DBDefinition database) {
		if (!database.canProduceNullStrings()) {
			this.isDBEmptyString = true;
		}
		return setToNull();
	}

	/**
	 * Returns TRUE if the database value is the empty string or NULL
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the database value is "" or NULL, otherwise FALSE.
	 */
	public boolean isEmptyOrNullString() {
		return isNull() || isEmptyString();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		setValue(encodedValue);
	}

	@Override
	public StringColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new StringColumn(row, this);
	}

	@Override
	public StringExpression stringResult() {
		return new StringExpression(this).stringResult();
	}

	public void excludeNotNull() {
		this.permittedValues((String) null);
	}

	public void excludeNull() {
		this.excludedValues((String) null);
	}

	public void permitOnlyNull() {
		excludeNotNull();
	}

	public void permitOnlyNotNull() {
		excludeNull();
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultInsertValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultInsertValue(new Date()) is probably NOT what you
	 * want, setDefaultInsertValue(DateExpression.currentDate()) will produce a
	 * correct creation date value.</p>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBString setDefaultInsertValue(String value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBString setDefaultInsertValue(StringResult value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultUpdateValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultUpdateValue(new Date()) is probably NOT what you
	 * want, setDefaultUpdateValue(DateExpression.currentDate()) will produce a
	 * correct update time value.</p>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBString setDefaultUpdateValue(String value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBString setDefaultUpdateValue(StringResult value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	@Override
	protected boolean checkForNullDuringSetFromResultSet() {
		return false;
	}

	@Override
	public Boolean isConsistentWithEmptyRow(DBDefinition defn) {
		if (defn.canProduceNullStrings()) {
			return isNull();
		} else {
			return isEmptyOrNullString();
		}
	}

	@Override
	public Comparator<String> getComparator() {
		return ComparableComparator.forClass(String.class);
	}

	@Override
	public boolean getCouldProduceEmptyStringForNull() {
		return true;
	}
}
