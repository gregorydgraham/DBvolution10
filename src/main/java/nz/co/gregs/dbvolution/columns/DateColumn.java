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

import java.util.Date;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing a date value.
 *
 * <p>
 * This class adds the necessary methods to use a date column like a date
 * expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in DateExpression to insert the column into the expression.
 *
 * <p>
 * Generally you get a DateColumn using
 * {@link RowDefinition#column(nz.co.gregs.dbvolution.datatypes.DBDate) RowDefinition.column(DBDate)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see DateExpression
 */
public class DateColumn extends DateExpression implements ColumnProvider {

	private static final long serialVersionUID = 1l;

	private AbstractColumn column;

	private DateColumn() {
	}

	/**
	 * Create a DateColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public DateColumn(RowDefinition row, Date field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a DateColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public DateColumn(RowDefinition row, DBDate field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	public synchronized DateColumn copy() {
		final AbstractColumn col = getColumn();
		final DBRow row = col.getInstanceOfRow();
		DateColumn newInstance = new DateColumn(row, (DBDate) col.getAppropriateQDTFromRow(row));
		return newInstance;
//		try {
//			DateColumn newInstance = this.getClass().newInstance();
//			newInstance.column = this.column.copy();
//			return newInstance;
//		} catch (InstantiationException | IllegalAccessException ex) {
//			throw new RuntimeException(ex);
//		}
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
	 * @param dateColumn an instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(DBDate dateColumn) {
		return super.is(dateColumn);
	}

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}
}
