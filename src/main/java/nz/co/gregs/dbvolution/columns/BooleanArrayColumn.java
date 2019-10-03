/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.columns;

import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.expressions.BooleanArrayExpression;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing an array of bits.
 *
 * <p>
 * Bits are not BLOBs or large objects, rather they are a collection of boolean
 * values stored as a single field in a byte array, or integer.
 * <p>
 * This class adds the necessary methods to use a collection of bits like a
 * byte[].
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in BooleanArrayExpression to insert the column into the expression.
 *
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see BooleanArrayExpression
 */
public class BooleanArrayColumn extends BooleanArrayExpression implements ColumnProvider {

	private final static long serialVersionUID = 1l;

	private final AbstractColumn column;

	/**
	 * Create a BooleanColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field representing the column
	 */
	public BooleanArrayColumn(RowDefinition row, boolean[] field) {
		this.column = new AbstractColumn(row, new DBBooleanArray(field));
	}

	/**
	 * Create a BooleanColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field representing the column
	 */
	public BooleanArrayColumn(RowDefinition row, DBBooleanArray field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	public BooleanArrayColumn copy() {
		final AbstractColumn col = getColumn();
		final DBRow row = col.getInstanceOfRow();
		BooleanArrayColumn newInstance = new BooleanArrayColumn(row, (DBBooleanArray) col.getAppropriateQDTFromRow(row));
		return newInstance;
	}

	@Override
	public AbstractColumn getColumn() {
		return column;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return column.getTablesInvolved();
	}

	@Override
	public void setUseTableAlias(boolean useTableAlias) {
		this.column.setUseTableAlias(useTableAlias);
	}

	@Override
	public boolean isPurelyFunctional() {
		return getTablesInvolved().isEmpty();
	}

	@Override
	public boolean isAggregator() {
		return column.isAggregator();
	}

	/**
	 * Creates an expression that will compare this column to the other column.
	 *
	 * @param boolArrayColumn the column to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(DBBooleanArray boolArrayColumn) {
		return super.is(boolArrayColumn);
	}

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}
}
