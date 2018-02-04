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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.BooleanArrayColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.BooleanArrayExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.BooleanArrayResult;

/**
 * Encapsulates database values that are BIT arrays of up to 64 bits.
 *
 * <p>
 * Use DBBoolean when the column is a {@code bool} or {@code bit(1)} datatype.
 *
 * <p>
 * Generally DBBooleanArray is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBBooleanArray myBoolColumn = new DBBooleanArray();}
 *
 * <p>
 * Yes/No Strings and 0/1 integer columns will need to use {@link DBString} and
 * {@link DBInteger} respectively. Depending on your requirements you should try
 * sub-classing DBString/DBInteger, extending DBStringEnum/DBIntegerEnum, or
 * using a {@link DBTypeAdaptor}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBBooleanArray extends QueryableDatatype<Boolean[]> implements BooleanArrayResult {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBBits.
	 *
	 * <p>
	 * Creates an unset undefined DBBits object.
	 *
	 */
	public DBBooleanArray() {
	}

	/**
	 * Creates a DBBits with the value provided.
	 *
	 * <p>
	 * The resulting DBBits will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * @param bools	bits
	 */
	public DBBooleanArray(Boolean[] bools) {
		super(bools);
	}

	/**
	 * Creates a column expression with a Boolean[] result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param bools	bits
	 */
	public DBBooleanArray(BooleanArrayExpression bools) {
		super(bools);
	}

	/**
	 * Implements the standard Java equals method.
	 *
	 * @param other	other
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if this object is the same as the other, otherwise FALSE.
	 */
	@Override
	public boolean equals(Object other) {
		if (other instanceof DBBooleanArray) {
			DBBooleanArray otherDBBits = (DBBooleanArray) other;
			return Arrays.equals(getValue(), otherDBBits.getValue());
		}
		return false;
	}

	@Override
	public String getSQLDatatype() {
		return "ARRAY";
	}

	@Override
	protected Boolean[] getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
		Boolean[] result = new Boolean[]{};
		if (database.supportsArraysNatively()) {
			Array sqlArray = resultSet.getArray(fullColumnName);
			if (!resultSet.wasNull()) {
				Object array = sqlArray.getArray();
				if (array instanceof Object[]) {
					Object[] objArray = (Object[]) array;
					if (objArray.length > 0) {
						result = new Boolean[objArray.length];
						for (int i = 0; i < objArray.length; i++) {
							if (objArray[i] instanceof Boolean) {
								result[i] = (Boolean) objArray[i];
							} else {
								Boolean bool = database.doBooleanArrayElementTransform(objArray[i]);
								result[i] = bool;
							}
						}
					}
				}
			} else {
				return null;
			}
		} else {
			String string = resultSet.getString(fullColumnName);
			result = database.doBooleanArrayResultInterpretation(string);
		}
		return result;
	}

	void setValue(DBBooleanArray newLiteralValue) {
		setValue(newLiteralValue.booleanArrayValue());
	}

	/**
	 * Sets the value of this DBBooleanArray to the value provided.
	 *
	 * @param newLiteralValue	newLiteralValue
	 */
	@Override
	public void setValue(Boolean[] newLiteralValue) {
		super.setLiteralValue(newLiteralValue);
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition defn) {
		if (getLiteralValue() != null) {
			Boolean[] booleanArray = getLiteralValue();
			return defn.doBooleanArrayTransform(booleanArray);
		}
		return defn.getNull();
	}

	/**
	 * Returns the defined or set value of this DBBooleanArray as an actual
	 * Boolean[].
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the value of this QDT as a Boolean[].
	 */
	public Boolean[] booleanArrayValue() {
		if (this.getLiteralValue() != null) {
			return this.getLiteralValue();
		} else {
			return null;
		}
	}

	@Override
	public DBBooleanArray copy() {
		return (DBBooleanArray) (BooleanArrayResult) super.copy();
	}

	@Override
	public Boolean[] getValue() {
		return booleanArrayValue();
	}

	@Override
	public DBBooleanArray getQueryableDatatypeForExpressionValue() {
		return new DBBooleanArray();
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
	 * Indicates whether this DBBooleanArray needs support for returning NULLs.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return FALSE.
	 */
	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public BooleanArrayColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new BooleanArrayColumn(row, this);
	}

	@Override
	public StringExpression stringResult() {
		return new BooleanArrayExpression(this).stringResult();
	}
}
