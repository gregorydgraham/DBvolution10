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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BitsResult;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 * Encapsulates database values that are BIT arrays of up to 64 bits.
 *
 * <p>
 * Use DBBoolean when the column is a {@code bool} or {@code bit(1)} datatype.
 *
 * <p>
 * Generally DBBits is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBBits myBoolColumn = new DBBits();}
 *
 * <p>
 * Yes/No Strings and 0/1 integer columns will need to use {@link DBString} and
 * {@link DBInteger} respectively. Depending on your requirements you should try
 * sub-classing DBString/DBInteger, extending DBStringEnum/DBIntegerEnum, or
 * using a {@link DBTypeAdaptor}.
 *
 * @author Gregory Graham
 */
public class DBBits extends QueryableDatatype implements BitsResult {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBBits.
	 *
	 * <p>
	 * Creates an unset undefined DBBits object.
	 *
	 */
	public DBBits() {
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
	public DBBits(byte[] bits) {
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
	public DBBits(BitsResult bits) {
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
		if (other instanceof DBBits) {
			DBBits otherDBBits = (DBBits) other;
			return Arrays.equals(getValue(), otherDBBits.getValue());
		}
		return false;
	}

	@Override
	public String getSQLDatatype() {
		return "BIT(64)";
	}

	@Override
	void setValue(Object newLiteralValue) {
		if (newLiteralValue instanceof byte[]) {
			setValue((Byte[]) newLiteralValue);
		} else if (newLiteralValue instanceof DBBits) {
			setValue(((QueryableDatatype) newLiteralValue).getValue());
		} else {
			throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Byte[]: Use only Byte[1-64] with this class");
		}
	}

	/**
	 * Sets the value of this DBBits to the value provided.
	 *
	 * @param newLiteralValue
	 */
	public void setValue(Byte[] newLiteralValue) {
		super.setLiteralValue(newLiteralValue);
	}

	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		DBDefinition defn = db.getDefinition();
		if (getLiteralValue() != null) {
			byte[] boolValue = (byte[]) getLiteralValue();
			return defn.doBitsValueTransform(boolValue);
//			return defn.beginNumberValue() + (boolValue ? 1 : 0) + defn.endNumberValue();
		}
		return defn.getNull();
	}

	/**
	 * Returns the defined or set value of this DBBits as an actual byte[].
	 *
	 * @return the value of this QDT as a boolean.
	 */
	public byte[] byteArrayValue() {
		if (this.getLiteralValue() != null) {
			return (byte[]) this.getLiteralValue();
		} else {
			return null;
		}
	}

	@Override
	public DBBits copy() {
		return (DBBits) (BitsResult) super.copy();
	}

	@Override
	public byte[] getValue() {
		return byteArrayValue();
	}

	@Override
	public DBBits getQueryableDatatypeForExpressionValue() {
		return new DBBits();
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
	 * Indicates whether this DBBits needs support for returning NULLs.
	 *
	 * @return FALSE.
	 */
	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	protected byte[] getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] dbValue = resultSet.getBytes(fullColumnName);
		if (resultSet.wasNull()) {
			dbValue = null;
		}
		return dbValue;
	}

}
