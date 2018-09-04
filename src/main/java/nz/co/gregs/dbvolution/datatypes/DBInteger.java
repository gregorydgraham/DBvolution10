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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import nz.co.gregs.dbvolution.columns.IntegerColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.IntegerResult;

/**
 * Encapsulates database values that are Integers.
 *
 * <p>
 * Use DBinteger when the column is a {@code INT} or {@code NUMBER(x)}, that is
 * any numeric datatype without a decimal or fractional part.
 *
 * <p>
 * Use {@link DBNumber} when the numbers do not have a decimal or fractional
 * part.
 *
 * <p>
 * Generally DBInteger is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBInteger myIntColumn = new DBInteger();}
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBInteger extends QueryableDatatype<Long> implements IntegerResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Create a DBInteger with the value set to the value provided..
	 *
	 * @param value	value
	 */
	public DBInteger(int value) {
		this(Integer.valueOf(value));
	}

	/**
	 * Create a DBInteger with the value set to the value provided..
	 *
	 * @param value	value
	 */
	public DBInteger(Integer value) {
		super(Long.valueOf(value));
	}

	/**
	 * Create a DBInteger with the value set to the value provided..
	 *
	 * @param value	value
	 */
	public DBInteger(long value) {
		this(Long.valueOf(value));
	}

	/**
	 * Create a DBInteger with the value set to the value provided..
	 *
	 * @param value	value
	 */
	public DBInteger(Long value) {
		super(value);
	}

	/**
	 * Create a DBInteger as a column expression.
	 *
	 * @param value	value
	 */
	public DBInteger(IntegerExpression value) {
		super(value);
	}

	/**
	 * Create a DBInteger as a column expression.
	 * 
	 * <p>Only the integer part of the number will be represented.
	 *
	 * @param value	value
	 */
	public DBInteger(NumberExpression value) {
		super(value.integerPart());
	}

	/**
	 * The default constructor for DBInteger.
	 *
	 * <p>
	 * Creates an unset undefined DBInteger object.
	 *
	 */
	public DBInteger() {
		super();
	}

	@Override
	public String getSQLDatatype() {
		return "INTEGER";
	}
	
	/**
	 * Returns a Long of the database value or NULL if the database value is null
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the long value or null
	 */
	@Override
	public Long getValue() {
		return this.getLiteralValue();
	}

	/**
	 * Returns an Integer of the database value or NULL if the database value is
	 * null
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the integer value or null
	 */
	public Integer intValue() {
		Long value = getValue();
		return value == null ? null : value.intValue();
	}

	/**
	 * Returns a Long of the database value or NULL if the database value is null
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the long value or null
	 */
	public Long longValue() {
		return getValue();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Long... permitted) {
		this.setOperator(new DBPermittedValuesOperator<Long>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(IntegerResult... permitted) {
		this.setOperator(new DBPermittedValuesOperator<IntegerResult>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(NumberResult... permitted) {
		List<IntegerResult> list = new ArrayList<>();
		for(NumberResult num: permitted){
			list.add(new NumberExpression(num).integerResult());
		}
		this.setOperator(new DBPermittedValuesOperator<IntegerResult>(list.toArray(new IntegerResult[]{})));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Number... permitted) {
		List<Long> ints = new ArrayList<>();
		for (Number dbint : permitted) {
			ints.add(dbint.longValue());
		}
		final Long[] longArray = ints.toArray(new Long[]{});
		this.setOperator(new DBPermittedValuesOperator<Long>(longArray));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(DBInteger... permitted) {
		List<Long> ints = new ArrayList<>();
		for (DBInteger dbint : permitted) {
			ints.add(dbint.getValue());
		}
		final Long[] longArray = ints.toArray(new Long[]{});
		this.setOperator(new DBPermittedValuesOperator<Long>(longArray));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(DBNumber... permitted) {
		List<Long> ints = new ArrayList<>();
		for (DBNumber dbint : permitted) {
			ints.add(dbint.getValue().longValue());
		}
		final Long[] longArray = ints.toArray(new Long[]{});
		this.setOperator(new DBPermittedValuesOperator<Long>(longArray));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Collection<Long> permitted) {
		this.setOperator(new DBPermittedValuesOperator<Long>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValuesInteger(Collection<Integer> permitted) {
		this.setOperator(new DBPermittedValuesOperator<Integer>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Integer... permitted) {
		this.setOperator(new DBPermittedValuesOperator<Integer>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(Long... excluded) {
		this.setOperator(new DBPermittedValuesOperator<Long>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(DBInteger... excluded) {
		this.setOperator(new DBPermittedValuesOperator<DBInteger>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(Integer... excluded) {
		this.setOperator(new DBPermittedValuesOperator<Integer>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesLong(List<Long> excluded) {
		this.setOperator(new DBPermittedValuesOperator<Long>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValuesInteger(List<Integer> excluded) {
		this.setOperator(new DBPermittedValuesOperator<Integer>(excluded));
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
	public void permittedRange(Long lowerBound, Long upperBound) {
		setOperator(new DBPermittedRangeOperator<Long>(lowerBound, upperBound));
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
	public void permittedRange(Integer lowerBound, Integer upperBound) {
		setOperator(new DBPermittedRangeOperator<Integer>(lowerBound, upperBound));
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
	public void permittedRangeInclusive(Long lowerBound, Long upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
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
	public void permittedRangeInclusive(Integer lowerBound, Integer upperBound) {
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
	public void permittedRangeExclusive(Long lowerBound, Long upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
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
	public void permittedRangeExclusive(Integer lowerBound, Integer upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e excludedRange(1,3) will return
	 * -1, 0, 3, 4...
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will return 5, 6, 7, 8, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(Long lowerBound, Long upperBound) {
		setOperator(new DBPermittedRangeOperator<Long>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e excludedRange(1,3) will return
	 * -1, 0, 3, 4...
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will return 5, 6, 7, 8, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(Integer lowerBound, Integer upperBound) {
		setOperator(new DBPermittedRangeOperator<Integer>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e excludedRangeInclusive(1,3) will return
	 * ..., -1, 0, 4, 5, 6, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will return 6, 7, 8, 9, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeInclusive(Long lowerBound, Long upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e excludedRangeInclusive(1,3) will return
	 * ..., -1, 0, 4, 5, 6, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will return 6, 7, 8, 9, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeInclusive(Integer lowerBound, Integer upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e excludedRangeExclusive(1,3) will return
	 * ..., -2-1,0,1,3,4, etc but not 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will return 1, 0, -1, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will return 5, 6, 7, 8, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeExclusive(Long lowerBound, Long upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e excludedRangeExclusive(1,3) will return
	 * ..., -2-1,0,1,3,4, etc but not 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will return 1, 0, -1, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will return 5, 6, 7, 8, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRangeExclusive(Integer lowerBound, Integer upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Sets the value of this DBInteger to the value provided.
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	public void setValue(DBNumber newLiteralValue) {
		setValue(newLiteralValue.getValue());
	}

	/**
	 * Sets the value of this DBInteger to the value provided.
	 * 
	 * <p>Convenience method that uses {@link  Long#Long(java.lang.String)} to set the value
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	public void setValue(String newLiteralValue) {
		setValue(Long.parseLong(newLiteralValue));
	}

	/**
	 * Sets the value of this DBInteger to the value provided.
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	public void setValue(DBInteger newLiteralValue) {
		setValue(newLiteralValue.getValue());
	}

	/**
	 * Sets the value of this DBInteger to the value provided.
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	public void setValue(Number newLiteralValue) {
		if (newLiteralValue == null) {
			super.setLiteralValue(null);
		} else {
			super.setLiteralValue(newLiteralValue.longValue());
		}
	}

	/**
	 * Sets the value of this DBInteger to the value provided.
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	@Override
	public void setValue(Long newLiteralValue) {
		if (newLiteralValue == null) {
			super.setLiteralValue(null);
		} else {
			super.setLiteralValue(newLiteralValue);
		}
	}

	/**
	 * Sets the value of this DBInteger to the value provided.
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	public void setValue(Integer newLiteralValue) {
		if (newLiteralValue == null) {
			super.setLiteralValue(null);
		} else {
			super.setLiteralValue(newLiteralValue.longValue());
		}
	}

	/**
	 *
	 * @param defn
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the underlying number formatted for a SQL statement
	 */
	@Override
	public String formatValueForSQLStatement(DBDefinition defn) {
		if (isNull()) {
			return defn.getNull();
		}
		return defn.beginNumberValue() + getLiteralValue() + defn.endNumberValue();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	protected Long getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
		return resultSet.getLong(fullColumnName);
	}

	@Override
	public DBInteger copy() {
		return (DBInteger) super.copy();
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	public StringExpression stringResult() {
		return IntegerExpression.value(this).stringResult();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		if (encodedValue.isEmpty()) {
			super.setLiteralValue(null);
		} else {
			try {
				Double parseDouble = Double.parseDouble(encodedValue);
				Long literalLong = parseDouble.longValue();
				setLiteralValue(literalLong);
			} catch (NumberFormatException noFormat) {
				setLiteralValue(null);
			}
		}
	}
	@Override
	public IntegerColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException{
		return new IntegerColumn(row, this);
	}

}
