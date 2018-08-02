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
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.expressions.LargeObjectExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing a large object value.
 *
 * <p>
 * This class adds the necessary methods to use a large object column like a
 * large object expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in {@link LargeObjectExpression} to insert the column into the
 * expression.
 *
 * <p>
 * Generally you get a LargeObjectColumn using
 * {@link RowDefinition#column(nz.co.gregs.dbvolution.datatypes.DBLargeObject)  RowDefinition.column(DBlargeObject)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see LargeObjectExpression
 */
public class LargeObjectColumn extends LargeObjectExpression implements ColumnProvider {

	private final static long serialVersionUID = 1l;

	private final AbstractColumn column;

	/**
	 * Create a LargeObjectColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public LargeObjectColumn(RowDefinition row, DBLargeObject<?> field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	public LargeObjectColumn copy() {
		final DBRow row = column.getInstanceOfRow();
		return new LargeObjectColumn(row, (DBLargeObject<?>) column.getAppropriateQDTFromRow(row));
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

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}
}
