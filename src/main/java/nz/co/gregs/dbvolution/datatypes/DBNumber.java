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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.NumberColumn;
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

/**
 * Encapsulates database values that are Numbers.
 *
 * <p>
 * Use DBNumber when the column is a {@code NUMBER} or {@code NUMBER(x,y)}, that
 * is any integer or real datatype..
 *
 * <p>
 * Use {@link DBInteger} when the numbers do not have a decimal or fractional
 * part.
 *
 * <p>
 * Generally DBNumber is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBNumber myIntColumn = new DBNumber();}
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBNumber extends QueryableDatatype<Number> implements NumberResult {

	private static final long serialVersionUID = 1;

	/**
	 * The default constructor for DBNumber.
	 *
	 * <p>
	 * Creates an unset undefined DBNumber object.
	 *
	 */
	public DBNumber() {
		super();
	}

	/**
	 * Creates a column expression with a number result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param numberExpression	numberExpression
	 */
	public DBNumber(NumberExpression... numberExpression) {
		super(numberExpression);
	}

	/**
	 * Creates a new DBNumber with the value set to the number provided.
	 *
	 * @param aNumber	aNumber
	 */
	public DBNumber(Number aNumber) {
		super(aNumber);
	}

	/**
	 *
	 * @param aNumber	aNumber
	 */
	public DBNumber(Long aNumber) {
		super(aNumber);
	}

	public DBNumber(IntegerExpression integerExpression) {
		this(integerExpression.numberResult()); 
	}

	@Override
	public DBNumber copy() {
		return (DBNumber) super.copy();
	}

	/**
	 * Sets the value of this DBNumber to the value of the DBNumber provided.
	 *
	 * <p>
	 * This allows DBNumbers to be treated somewhat like normal numbers. However {@link #setValue(java.lang.Number)
	 * } may be more useful in normal usage.
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	public void setValue(DBNumber newLiteralValue) {
		setValue((newLiteralValue).getValue());
	}

	/**
	 * Set the value of this DBNumber to the Double, Long, Integer, or other
	 * number provided.
	 *
	 * <p>
	 * This is probably the method you want to use to set or change the value of
	 * this DBNumber. When creating a new row or updating an existing row use this
	 * method or {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBNumber)} to
	 * correctly set the value.
	 *
	 * <p>
	 * Remember:</p>
	 *
	 * <ul>
	 * <li>Set the column to NULL using setValue((Number)null)</li>
	 * <li>Use {@link DBDatabase#insert(nz.co.gregs.dbvolution.DBRow...) } or {@link DBDatabase#update(nz.co.gregs.dbvolution.DBRow...)
	 * } to make the changes permanent.</li>
	 * </ul>
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	@Override
	public void setValue(Number newLiteralValue) {
		if (newLiteralValue == null) {
			super.setLiteralValue(null);
		} else {
			super.setLiteralValue(newLiteralValue.doubleValue());
		}
	}

	/**
	 * Set the value of this DBNumber to the Double, Long, Integer, or other
	 * number provided.
	 * 
	 * <p>A convenience method the uses Double.parseDouble(String) to create a number from the string.
	 *
	 * <p>
	 * This is probably the method you want to use to set or change the value of
	 * this DBNumber. When creating a new row or updating an existing row use this
	 * method or {@link #setValue(nz.co.gregs.dbvolution.datatypes.DBNumber)} to
	 * correctly set the value.
	 *
	 * <p>
	 * Remember:</p>
	 *
	 * <ul>
	 * <li>Set the column to NULL using setValue((Number)null)</li>
	 * <li>Use {@link DBDatabase#insert(nz.co.gregs.dbvolution.DBRow...) } or {@link DBDatabase#update(nz.co.gregs.dbvolution.DBRow...)
	 * } to make the changes permanent.</li>
	 * </ul>
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	public void setValue(String newLiteralValue) {
		setValue(Double.parseDouble(newLiteralValue));
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default database type as a string, may be gazumped by the
	 * DBDefinition
	 */
	@Override
	public String getSQLDatatype() {
		return "NUMERIC(" + getNumericPrecision() + "," + getNumericScale() + ")";
	}

	public static int getNumericScale() {
		return 16;
		// Theoretically 16 decimals is the maximum accuracy of floating point numbers.
		// given the max precision(28) of pre-2012 MS SQL Server, 16 decimals seems as accurate as is reasonable
	}

	public static int getNumericPrecision() {
		return 28;
		// 28 is the maximum precision for MS SQL Server pre-2012, other databases seem not to be limited
		// 38 is the maximum for MSSQLServer 2012+
	}

	/**
	 *
	 * @param defn the DBDefinition to used in formatting the value
	 * @return the underlying number formatted for a SQL statement
	 */
	@Override
	public String formatValueForSQLStatement(DBDefinition defn) {
		if (isNull()) {
			return defn.getNull();
		}
		return defn.beginNumberValue() + getLiteralValue().toString() + defn.endNumberValue();
	}

	/**
	 * Gets the current literal value of this DBNumber, without any formatting.
	 *
	 * <p>
	 * The literal value is undefined (and {@code null}) if using an operator
	 * other than {@code equals}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the literal value, if defined, which may be null
	 */
	@Override
	public Number getValue() {
		return numberValue();
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as a Number
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number as the original number class
	 */
	public Number numberValue() {
		if (getLiteralValue() == null) {
			return null;
		} else if (getLiteralValue() instanceof Number) {
			return getLiteralValue();
		} else {
			return Double.parseDouble(getLiteralValue().toString());
		}
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as a Double
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number as a Double
	 */
	@SuppressWarnings("deprecation")
	public Double doubleValue() {
		if (getLiteralValue() == null) {
			return null;
		} else {
			return getLiteralValue().doubleValue();
		}
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as a Long
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number as a Long
	 */
	@SuppressWarnings("deprecation")
	public Long longValue() {
		if (getLiteralValue() == null) {
			return null;
		} else {
			return getLiteralValue().longValue();
		}
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as an
	 * Integer
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number as an Integer
	 */
	@SuppressWarnings("deprecation")
	public Integer intValue() {
		if (getLiteralValue() == null) {
			return null;
		} else {
			return getLiteralValue().intValue();
		}
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
		return new HashSet<>();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Number... permitted) {
		this.setOperator(new DBPermittedValuesOperator<Number>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(Collection<Number> permitted) {
		this.setOperator(new DBPermittedValuesOperator<Number>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	public void permittedValues(NumberResult... permitted) {
		this.setOperator(new DBPermittedValuesOperator<NumberResult>(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(Number... excluded) {
		this.setOperator(new DBPermittedValuesOperator<Number>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(Collection<Number> excluded) {
		this.setOperator(new DBPermittedValuesOperator<Number>(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded	excluded
	 */
	public void excludedValues(NumberResult... excluded) {
		this.setOperator(new DBPermittedValuesOperator<NumberResult>(excluded));
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
	public void permittedRange(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeOperator<Number>(lowerBound, upperBound));
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
	public void permittedRange(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeOperator<NumberResult>(lowerBound, upperBound));
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
	public void permittedRangeInclusive(Number lowerBound, Number upperBound) {
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
	public void permittedRangeInclusive(NumberResult lowerBound, NumberResult upperBound) {
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
	public void permittedRangeExclusive(Number lowerBound, Number upperBound) {
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
	public void permittedRangeExclusive(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be within the
	 * range and the upper-bound outside. I.e excludedRange(1,3) will exclude 1
	 * and 2.
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
	public void excludedRange(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeOperator<Number>(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be within in
	 * the range and the upper-bound outside. I.e excludedRange(1,3) will exclude
	 * 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will exclude 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended and exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound lowerBound
	 * @param upperBound upperBound
	 */
	public void excludedRange(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeOperator<NumberResult>(lowerBound, upperBound));
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
	public void excludedRangeInclusive(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
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
	public void excludedRangeInclusive(NumberResult lowerBound, NumberResult upperBound) {
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
	public void excludedRangeExclusive(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
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
	public void excludedRangeExclusive(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	protected Number getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) throws SQLException {
		try {
			return resultSet.getBigDecimal(fullColumnName);
		} catch (SQLException ex) {
			try {
				return resultSet.getLong(fullColumnName);
			} catch (SQLException ex2) {
				return null;
			}
		}
	}

	@Override
	public StringExpression stringResult() {
		return NumberExpression.value(this).stringResult();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		setValue(Double.parseDouble(encodedValue));
	}

	@Override
	public NumberColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new NumberColumn(row, this);
	}
}
