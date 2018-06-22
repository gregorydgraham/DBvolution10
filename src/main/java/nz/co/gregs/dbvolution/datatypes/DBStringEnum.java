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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.operators.*;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Like {@link DBString} except that the database value can be easily
 * interpreted as an enumeration with integer codes.
 *
 * DBStringEnum maps string values automatically from the database value to the
 * enumeration value via the {@link DBEnumValue} interface.
 *
 * <p>
 * Internally stores only the database-centric literal value in its type.
 * Conversion to the enumeration type is done lazily so that it's possible to
 * handle the case where a database has an invalid value or a new value that
 * isn't in the enumeration.
 * 
 * <p>
 * Normally declared as something like:</p>
 * <pre>
 * {@literal @}DBColumn
 * DBStringEnum state = new DBStringEnum&lt;MyEnumValue&gt;();
 *
 *
 * public static enum MyEnumValue implements DBEnumValue&lt;String&gt; {
 *
 * STATE_ONE(1, "One"),
 * STATE_TWO(2, "Two"),
 * STATE_THREE(3, "Three");
 * private final String literalValue;
 * private final String displayName;
 *
 * private GenericEnumType(String code, String displayName) {
 * this.literalValue = code;
 * this.displayName = displayName;
 * }
 *
 * public String getCode() {
 * return literalValue;
 * }
 *
 * public String getDisplayName() {
 * return displayName;
 * }
 * }
 * </pre>
 *
 *
 *
 * @param <E> an enumeration class that implements the {@link DBEnumValue}
 * interface for String values.
 */
public class DBStringEnum<E extends Enum<E> & DBEnumValue<String>> extends DBEnum<E, String> {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBStringEnum.
	 *
	 * <p>
	 * Creates an unset undefined DBStringEnum object.
	 *
	 */
	public DBStringEnum() {
	}

	/**
	 * Creates a DBStringEnum with the value provided.
	 *
	 * @param value	value
	 */
	public DBStringEnum(String value) {
		super(value);
	}

	/**
	 * Creates a DBStringEnum column expression based on the
	 * {@link StringExpression} provided.
	 *
	 * <p>
	 * This is used to generate a DBStringEnum result from data in the database.at
	 * runtime. Use a StringExpression to create the data.
	 *
	 * <p>
	 * DBvolution can not ensure that the results of your expression will conform
	 * to the particular DBStringEnum values, so construct your expression
	 * carefully.
	 *
	 * <p>
	 * This can also be used to transform an existing column into a DBStringEnum,
	 * the above warning still applies.
	 *
	 * @param stringExpression	stringExpression
	 */
	public DBStringEnum(StringExpression stringExpression) {
		super(stringExpression);
	}

	/**
	 * Creates a DBStringEnum with the value set to the value of the supplied Enum
	 * object..
	 *
	 * @param value	value
	 */
	public DBStringEnum(E value) {
		super(value);
	}

	@Override
	protected void validateLiteralValue(E enumValue) {
		Object localValue = enumValue.getCode();
		if (localValue != null) {
			if (!(localValue instanceof String)) {
				String enumMethodRef = enumValue.getClass().getName() + "." + enumValue.name() + ".getLiteralValue()";
				String literalValueTypeRef = localValue.getClass().getName();
				throw new IncompatibleClassChangeError("Enum literal type is not valid: "
						+ enumMethodRef + " returned a " + literalValueTypeRef + ", which is not valid for a " + this.getClass().getSimpleName());
			}
		}
	}

	@Override
	public String getSQLDatatype() {
		return new DBString().getSQLDatatype();
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
	 * Create an array of Strings containing the literal values of the provided
	 * Enums.
	 *
	 * <p>
	 * Provided as a convenience function
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String[] of the enums values.
	 */
	@SafeVarargs
	private final String[] convertToLiteralString(E... enumValues) {
		String[] result = new String[enumValues.length];
		for (int i = 0; i < enumValues.length; i++) {
			E enumValue = enumValues[i];
			result[i] = convertToLiteralString(enumValue);
		}
		return result;
	}

	/**
	 * Convert the enum collection to an array of String literal values.
	 *
	 */
	private String[] convertToLiteralString(Collection<E> enumValues) {
		ArrayList<String> result = new ArrayList<>();
		for (E e : enumValues) {
			result.add(convertToLiteralString(e));
		}
		return result.toArray(new String[]{});
	}

	/**
	 * Convert the enum to its String literal value.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the literal value of the enum.
	 */
	private String convertToLiteralString(E enumValue) {
		if (enumValue == null || enumValue.getCode() == null) {
			return null;
		} else {
			validateLiteralValue(enumValue);
			String newLiteralValue = enumValue.getCode();
			return newLiteralValue;
		}
	}

//	/**
//	 * Reduces the rows returned from a query to only those matching the provided
//	 * objects.
//	 *
//	 * @param permitted	permitted
//	 */
//	public void permittedValues(String... permitted) {
//		this.setOperator(new DBPermittedValuesOperator<String>(permitted));
//	}

	/**
	 * Reduces the rows returned from a query to only those matching the provided
	 * objects.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesIgnoreCase(String... permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
	}

	/**
	 * Reduces the rows returned from a query to only those matching the provided
	 * expression.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesIgnoreCase(StringExpression... permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(permitted));
	}

	/**
	 * Reduces the rows returned from a query to only those matching the provided
	 * objects.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesIgnoreCase(Collection<E> permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(convertToLiteralString(permitted)));
	}

	/**
	 * Reduces the rows returned from a query by excluding those matching the
	 * provided objects.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(String... excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
		negateOperator();
	}

	/**
	 * Reduces the rows returned from a query by excluding those matching the
	 * provided objects.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(StringExpression... excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
		negateOperator();
	}

	/**
	 * Reduces the rows returned from a query by excluding those matching the
	 * provided objects.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(List<String> excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
		negateOperator();
	}

	/**
	 * Reduces the rows returned from a query by excluding those matching the
	 * provided objects.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(Set<String> excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(excluded));
		negateOperator();
	}

//	/**
//	 * Reduces the rows returned from a query by excluding those matching the
//	 * provided objects.
//	 *
//	 * <p>
//	 * The case, upper or lower, will be ignored.
//	 *
//	 * <p>
//	 * Defining case for Unicode characters is complicated and may not work as
//	 * expected.
//	 *
//	 * @param excluded	excluded
//	 */
//	public void excludedValues(String... excluded) {
//		this.setOperator(new DBPermittedValuesOperator<String>(excluded));
//		negateOperator();
//	}

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
	 * <p>
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended up and exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended down and exclusive.
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
	 * <p>
	 * if both ends of the range are specified the lower-bound will included and
	 * the upper-bound will be excluded in the range. I.e excludedRange(1,3) will
	 * exclude 1,2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended up and inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended down and exclusive.
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
	 * <p>
	 * if both ends of the range are specified both the lower-bound and the
	 * upper-bound will be included in the range. I.e excludedRangeInclusive(1,3)
	 * will exclude 1,2,3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended up and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended down and inclusive.
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
	 * <p>
	 * if both ends of the range are specified both the lower-bound and the
	 * upper-bound will be excluded in the range. I.e excludedRangeExclusive(1,3)
	 * will exclude 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended up and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will exclude 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended down and exclusive.
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

//	/**
//	 *
//	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
//	 *
//	 * @param permitted	permitted
//	 */
//	@SafeVarargs
//	public final void permittedValues(E... permitted) {
//		this.setOperator(new DBPermittedValuesOperator<String>(convertToLiteral(permitted)));
//	}

	@Override
	@SafeVarargs
	protected final String[] convertToLiteral(E... enumValues) {
		return convertToLiteralString(enumValues);
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of Strings
	 * ignoring letter case.
	 *
	 * @param permitted	permitted
	 */
	@SafeVarargs
	public final void permittedValuesIgnoreCase(E... permitted) {
		this.setOperator(new DBPermittedValuesIgnoreCaseOperator(convertToLiteral(permitted)));
	}

	/**
	 * Reduces the rows to excluding the object, Set, List, Array, or vararg of
	 * Strings ignoring letter case.
	 *
	 * @param excluded	excluded
	 */
	@SafeVarargs
	public final void excludedValuesIgnoreCase(E... excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(convertToLiteralString(excluded)));
		negateOperator();
	}

	/**
	 * Reduces the rows to excluding the object, Set, List, Array, or vararg of
	 * Strings ignoring letter case.
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesIgnoreCase(Collection<E> excluded) {
		setOperator(new DBPermittedValuesIgnoreCaseOperator(convertToLiteralString(excluded)));
		negateOperator();
	}

//	/**
//	 *
//	 * excludes the object, Set, List, Array, or vararg of objects
//	 *
//	 *
//	 * @param excluded	excluded
//	 */
//	@SafeVarargs
//	public final void excludedValues(E... excluded) {
//		this.setOperator(new DBPermittedValuesOperator<String>(convertToLiteralString(excluded)));
//		negateOperator();
//	}

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
	public void permittedRange(E lowerBound, E upperBound) {
		setOperator(new DBPermittedRangeOperator<String>(convertToLiteralString(lowerBound), convertToLiteralString(upperBound)));
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
	public void permittedRangeInclusive(E lowerBound, E upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(convertToLiteralString(lowerBound), convertToLiteralString(upperBound)));
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
	public void permittedRangeExclusive(E lowerBound, E upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(convertToLiteralString(lowerBound), convertToLiteralString(upperBound)));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * if both ends of the range are specified the lower-bound will included and
	 * the upper-bound will be excluded in the range. I.e excludedRange(1,3) will
	 * exclude 1,2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended up and inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended down and exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will exclude 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(E lowerBound, E upperBound) {
		setOperator(new DBPermittedRangeOperator<String>(convertToLiteralString(lowerBound), convertToLiteralString(upperBound)));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * if both ends of the range are specified both the lower-bound and the
	 * upper-bound will be excluded in the range. I.e excludedRangeExclusive(1,3)
	 * will exclude 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended up and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will exclude 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended down and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will exclude 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeInclusive(E lowerBound, E upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(convertToLiteralString(lowerBound), convertToLiteralString(upperBound)));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * <p>
	 * if both ends of the range are specified both the lower-bound and the
	 * upper-bound will be excluded in the range. I.e excludedRangeExclusive(1,3)
	 * will exclude 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended up and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will exclude 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended down and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will exclude 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeExclusive(E lowerBound, E upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(convertToLiteralString(lowerBound), convertToLiteralString(upperBound)));
		negateOperator();
	}

	@Override
	public String getValue() {
		final Object value = super.getLiteralValue();
		if (value == null) {
			return null;
		} else if (value instanceof String) {
			return (String) value;
		} else {
			return value.toString();
		}
	}

	@Override
	protected String getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
		return resultSet.getString(fullColumnName);
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		setValue(encodedValue);
	}

	@Override
	public StringColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new StringColumn(row, this);
	}
}
