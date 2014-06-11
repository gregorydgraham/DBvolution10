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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBLikeOperator;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 *
 * @author Gregory Graham
 */
public class DBNumber extends QueryableDatatype implements NumberResult {

	private static final long serialVersionUID = 1;

	public DBNumber() {
		super();
	}

	public DBNumber(NumberResult numberExpression) {
		super(numberExpression);
	}

	/**
	 *
	 * @param aNumber
	 */
	public DBNumber(Number aNumber) {
		super(aNumber);
	}

	/**
	 *
	 * @param aNumber
	 */
	public DBNumber(Integer aNumber) {
		super(aNumber);
	}

	/**
	 *
	 * @param aNumber
	 */
	public DBNumber(Long aNumber) {
		super(aNumber);
	}

	@Override
	public DBNumber copy() {
		return (DBNumber) super.copy();
	}

	@Override
	public void setValue(Object newLiteralValue) {
		if (newLiteralValue instanceof Number) {
			setValue((Number) newLiteralValue);
		} else if (newLiteralValue instanceof DBNumber) {
			setValue((DBNumber) newLiteralValue);
		} else {
			throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A " + newLiteralValue.getClass().getSimpleName() + ": Use only Numbers with this class");
		}
	}

	public void setValue(DBNumber newLiteralValue) {
		setValue((newLiteralValue).getValue());
	}

	public void setValue(Number newLiteralValue) {
		if (newLiteralValue == null) {
			super.setLiteralValue(null);
		} else {
			super.setLiteralValue(newLiteralValue);
		}
	}

	@Override
	public void blankQuery() {
		super.blankQuery();
	}

	@Override
	public String getWhereClause(DBDatabase db, String columnName) {
		if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
			throw new RuntimeException("NUMBER COLUMNS CAN'T USE \"LIKE\": " + columnName);
		} else if (this.getOperator() instanceof DBLikeOperator) {
			throw new RuntimeException("NUMBER COLUMNS CAN'T USE \"LIKE\": " + columnName);
		} else {
			return super.getWhereClause(db, columnName);
		}
	}

	/**
	 *
	 * @return the default database type as a string, may be gazumped by the
	 * DBDefinition
	 */
	@Override
	public String getSQLDatatype() {
		return "NUMERIC(15,5)";
	}

	/**
	 *
	 * @param db
	 * @return the underlying number formatted for a SQL statement
	 */
	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		DBDefinition defn = db.getDefinition();
		if (isNull()) {
			return defn.getNull();
		}
		return defn.beginNumberValue() + literalValue.toString() + defn.endNumberValue();
	}

	/**
	 * Gets the current literal value of this DBNumber, without any formatting.
	 *
	 * <p>
	 * The literal value is undefined (and {@code null}) if using an operator
	 * other than {@code equals}.
	 *
	 * @return the literal value, if defined, which may be null
	 */
	@Override
	public Number getValue() {
		return numberValue();
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as a
	 * Number
	 *
	 * @return the number as the original number class
	 */
	public Number numberValue() {
		if (literalValue == null) {
			return null;
		} else if (literalValue instanceof Number) {
			return (Number) literalValue;
		} else {
			return Double.parseDouble(literalValue.toString());
		}
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as a
	 * Double
	 *
	 * @return the number as a Double
	 */
	@SuppressWarnings("deprecation")
	public Double doubleValue() {
		if (literalValue == null) {
			return null;
		} else if (literalValue instanceof Number) {
			return ((Number) literalValue).doubleValue();
		} else {
			return Double.parseDouble(literalValue.toString());
		}
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as a Long
	 *
	 * @return the number as a Long
	 */
	@SuppressWarnings("deprecation")
	public Long longValue() {
		if (literalValue == null) {
			return null;
		} else if (literalValue instanceof Long) {
			return (Long) literalValue;
		} else if (literalValue instanceof Number) {
			return ((Number) literalValue).longValue();
		} else {
			return Long.parseLong(literalValue.toString());
		}
	}

	/**
	 * The current {@link #getValue()  literal value} of this DBNumber as an
	 * Integer
	 *
	 * @return the number as an Integer
	 */
	@SuppressWarnings("deprecation")
	public Integer intValue() {
		if (literalValue == null) {
			return null;
		} else if (literalValue instanceof Number) {
			return ((Number) literalValue).intValue();
		} else {
			return Integer.parseInt(literalValue.toString());
		}
	}

	/**
	 * Internal method to automatically set the value using information from the
	 * database
	 *
	 * @param resultSet
	 * @param fullColumnName
	 */
	@Override
	public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
		blankQuery();
		if (resultSet == null || fullColumnName == null) {
			this.setToNull();
		} else {
			BigDecimal dbValue;
			try {
				dbValue = resultSet.getBigDecimal(fullColumnName);
				if (resultSet.wasNull()) {
					dbValue = null;
				}
			} catch (SQLException ex) {
				dbValue = null;
			}
			if (dbValue == null) {
				this.setToNull();
			} else {
				this.setValue(dbValue);
			}
		}
		setUnchanged();
		setDefined(true);
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
		return new HashSet<DBRow>();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of
	 * objects
	 *
	 * @param permitted
	 */
	public void permittedValues(Number... permitted) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of
	 * objects
	 *
	 * @param permitted
	 */
	public void permittedValues(Collection<Number> permitted) {
		this.setOperator(new DBPermittedValuesOperator(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of
	 * objects
	 *
	 * @param permitted
	 */
	public void permittedValues(NumberResult... permitted) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded
	 */
	public void excludedValues(Number... excluded) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded
	 */
	public void excludedValues(Collection<Number> excluded) {
		this.setOperator(new DBPermittedValuesOperator(excluded));
		negateOperator();
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded
	 */
	public void excludedValues(NumberResult... excluded) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) excluded));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included
	 * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
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
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRange(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included
	 * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
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
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRange(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
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
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRangeExclusive(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}

	public void excludedRange(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
		negateOperator();
	}

	public void excludedRange(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
		negateOperator();
	}

	public void excludedRangeInclusive(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	public void excludedRangeInclusive(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	public void excludedRangeExclusive(Number lowerBound, Number upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	public void excludedRangeExclusive(NumberResult lowerBound, NumberResult upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}
}
