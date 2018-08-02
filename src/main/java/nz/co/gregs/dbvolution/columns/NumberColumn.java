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
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing a number value.
 *
 * <p>
 * This class adds the necessary methods to use a number column like a number
 * expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in NumberExpression to insert the column into the expression.
 *
 * <p>
 * Generally you get a NumberColummn using
 * {@link RowDefinition#column(nz.co.gregs.dbvolution.datatypes.DBNumber)  RowDefinition.column(DBNumber)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see NumberExpression
 */
public class NumberColumn extends NumberExpression implements ColumnProvider {

	private final static long serialVersionUID = 1l;

	private AbstractColumn column;

	private NumberColumn() {
	}

	/**
	 * Create a NumberColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public NumberColumn(RowDefinition row, Number field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a NumberColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public NumberColumn(RowDefinition row, DBNumber field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	public synchronized NumberColumn copy() {
		final AbstractColumn col = getColumn();
		final DBRow row = col.getInstanceOfRow();
		NumberColumn newInstance = new NumberColumn(row, (DBNumber) col.getAppropriateQDTFromRow(row));
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
	 * Create an expression to compare this column to the other column using
	 * EQUALS.
	 *
	 * @param column the value to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(DBNumber column) {
		return super.is(column);
	}

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}
}
