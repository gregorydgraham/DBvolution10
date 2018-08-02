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
package nz.co.gregs.dbvolution.columns;

import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing a boolean value.
 *
 * <p>
 * This class adds the necessary methods to use a boolean column like a boolean
 * expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in BooleanExpression to insert the column into the expression.
 *
 * <p>
 * Generally you get a BooleanColumn using
 * {@link RowDefinition#column(java.lang.Boolean) RowDefinition.column(DBBoolean)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see BooleanExpression
 */
public class BooleanColumn extends BooleanExpression implements ColumnProvider {

	private final static long serialVersionUID = 1l;

	private final AbstractColumn column;

	/**
	 * Create a BooleanColumn for the supplied field of the supplied row
	 *
	 * @param row the row that the field belongs to
	 * @param field the field that represents the column
	 */
	public BooleanColumn(RowDefinition row, Boolean field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a BooleanColumn for the supplied field of the supplied row
	 *
	 * @param row the row that the field belongs to
	 * @param field the field that represents the column
	 */
	public BooleanColumn(RowDefinition row, DBBoolean field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	public BooleanColumn copy() {
		final AbstractColumn col = getColumn();
		final DBRow row = col.getInstanceOfRow();
		BooleanColumn newInstance = new BooleanColumn(row, (DBBoolean) col.getAppropriateQDTFromRow(row));
		return newInstance;

//		return (BooleanColumn) super.copy();
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
	 * Create an expression to compare this column to the other column using
	 * EQUALS.
	 *
	 * @param boolColumn an instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(DBBoolean boolColumn) {
		return super.is(boolColumn);
	}

	@Override
	public boolean isBooleanStatement() {
		return false;
	}

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}
}
