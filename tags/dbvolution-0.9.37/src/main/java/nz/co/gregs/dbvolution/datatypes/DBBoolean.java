/*
 * Copyright 2013 gregorygraham.
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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.BooleanResult;
import nz.co.gregs.dbvolution.operators.DBBooleanPermittedValuesOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 * Encapsulates database values that are Booleans.
 *
 * <p>
 * Use DBBoolean when the column is a {@code bool} or {@code bit(1)} datatype.
 *
 * <p>
 * Generally DBBoolean is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBBoolean myBoolColumn = new DBBoolean();}
 *
 * <p>
 * Yes/No Strings and 0/1 integer columns will need to use {@link DBString} and
 * {@link DBInteger} respectively. Sub-classing DBString/DBInteger or using a
 * {@link DBTypeAdaptor} may help.
 *
 * @author Gregory Graham
 */
public class DBBoolean extends QueryableDatatype implements BooleanResult {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBBoolean.
	 *
	 * <p>
	 * Creates an unset undefined DBBoolean object.
	 *
	 */
	public DBBoolean() {
	}

	/**
	 * Creates a DBBoolean with the value provided.
	 *
	 * <p>
	 * The resulting boolean will be set as having the value provided but will
	 * not be defined in the database.
	 *
	 * @param bool
	 */
	public DBBoolean(Boolean bool) {
		super(bool);
	}

	/**
	 * Creates a column expression with a boolean result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param bool
	 */
	public DBBoolean(BooleanResult bool) {
		super(bool);
	}

	@Override
	public String getSQLDatatype() {
		return "BIT(1)";
	}

	@Override
	public void setValue(Object newLiteralValue) {
		if (newLiteralValue instanceof Boolean) {
			setValue((Boolean) newLiteralValue);
		} else if (newLiteralValue instanceof DBBoolean) {
			setValue(((DBBoolean) newLiteralValue).getValue());
		} else {
			throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Boolean: Use only Booleans with this class");
		}
	}

	/**
	 * Sets the value of this DBBoolean to the value provided.
	 *
	 * @param newLiteralValue
	 */
	public void setValue(Boolean newLiteralValue) {
		super.setLiteralValue(newLiteralValue);
	}

	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		DBDefinition defn = db.getDefinition();
		if (literalValue instanceof Boolean) {
			Boolean boolValue = (Boolean) literalValue;
			return defn.beginNumberValue() + (boolValue ? 1 : 0) + defn.endNumberValue();
		}
		return defn.getNull();
	}

	/**
	 * Returns the defined or set value of this DBBoolean as an actual Boolean.
	 *
	 * @return the value of this QDT as a boolean.
	 */
	public Boolean booleanValue() {
		if (this.literalValue instanceof Boolean) {
			return (Boolean) this.literalValue;
		} else {
			return null;
		}
	}

	@Override
	public DBBoolean copy() {
		return (DBBoolean) (BooleanResult) super.copy();
	}

	@Override
	public Boolean getValue() {
		return booleanValue();
	}

	@Override
	public DBBoolean getQueryableDatatypeForExpressionValue() {
		return new DBBoolean();
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
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted
	 */
	public void permittedValues(Boolean permitted) {
		this.setOperator(new DBBooleanPermittedValuesOperator(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded
	 */
	public void excludedValues(Boolean excluded) {
		this.setOperator(new DBPermittedValuesOperator(excluded));
		negateOperator();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted
	 */
	public void permittedValues(BooleanExpression permitted) {
		this.setOperator(new DBPermittedValuesOperator(permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded
	 */
	public void excludedValues(BooleanExpression excluded) {
		this.setOperator(new DBPermittedValuesOperator(excluded));
		negateOperator();
	}

	/**
	 * Indicates whether this DBBoolean needs support for returning NULLs.
	 *
	 * @return FALSE.
	 */
	@Override
	public boolean getIncludesNull() {
		return false;
	}

//	@Override
//	public void setIncludesNull(boolean nullsAreIncluded) {
//		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//	}
}
