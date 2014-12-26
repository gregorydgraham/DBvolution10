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
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanArrayResult;

/**
 * Encapsulates database values that are BIT arrays of up to 64 bits.
 *
 * <p>
 * Use DBBoolean when the column is a {@code bool} or {@code bit(1)} datatype.
 *
 * <p>
 Generally DBBooleanArray is declared inside your DBRow sub-class as:
 {@code @DBColumn public DBBooleanArray myBoolColumn = new DBBooleanArray();}
 *
 * <p>
 * Yes/No Strings and 0/1 integer columns will need to use {@link DBString} and
 * {@link DBInteger} respectively. Depending on your requirements you should try
 * sub-classing DBString/DBInteger, extending DBStringEnum/DBIntegerEnum, or
 * using a {@link DBTypeAdaptor}.
 *
 * @author Gregory Graham
 */
public class DBBooleanArray extends QueryableDatatype implements BooleanArrayResult {

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
	 * The resulting DBBits will be set as having the value provided but will
	 * not be defined in the database.
	 *
	 * @param bits
	 */
	public DBBooleanArray(boolean[] bits) {
		super(bits);
	}

	/**
	 * Creates a column expression with a boolean result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param bits
	 */
	public DBBooleanArray(BooleanArrayResult bits) {
		super(bits);
	}

	/**
	 * Implements the standard Java equals method.
	 *
	 * @param other
	 * @return TRUE if this object is the same as the other, otherwise FALSE.
	 */
	@Override
	public boolean equals(QueryableDatatype other) {
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
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException{
		boolean[] result = new boolean[]{};
		Object array = resultSet.getArray(fullColumnName).getArray();
		if(array instanceof boolean[]){
			result = (boolean[]) array;
		}
		return result;
	}

	@Override
	void setValue(Object newLiteralValue) {
		if (newLiteralValue instanceof boolean[]) {
			setValue((boolean[]) newLiteralValue);
		} else if (newLiteralValue instanceof DBBooleanArray) {
			setValue(((DBBooleanArray) newLiteralValue).booleanArrayValue());
		} else {
			throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-boolean[]: Use only boolean[1-64] with this class");
		}
	}

	/**
	 * Sets the value of this DBBooleanArray to the value provided.
	 *
	 * @param newLiteralValue
	 */
	public void setValue(boolean[] newLiteralValue) {
		super.setLiteralValue(newLiteralValue);
	}

	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		DBDefinition defn = db.getDefinition();
		if (getLiteralValue() != null) {
			boolean[] boolArray = (boolean[]) getLiteralValue();
			return defn.doBitsValueTransform(boolArray);
		}
		return defn.getNull();
	}

	/**
	 * Returns the defined or set value of this DBBooleanArray as an actual boolean[].
	 *
	 * @return the value of this QDT as a boolean.
	 */
	public boolean[] booleanArrayValue() {
		if (this.getLiteralValue() != null) {
			return (boolean[]) this.getLiteralValue();
		} else {
			return null;
		}
	}

	@Override
	public DBBooleanArray copy() {
		return (DBBooleanArray) (BooleanArrayResult) super.copy();
	}

	@Override
	public boolean[] getValue() {
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
		return new HashSet<DBRow>();
	}

	/**
	 * Indicates whether this DBBooleanArray needs support for returning NULLs.
	 *
	 * @return FALSE.
	 */
	@Override
	public boolean getIncludesNull() {
		return false;
	}

//	@Override
//	protected byte[] getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
//		byte[] dbValue = resultSet.getBytes(fullColumnName);
//		if (resultSet.wasNull()) {
//			dbValue = null;
//		}
//		return dbValue;
//	}

}
