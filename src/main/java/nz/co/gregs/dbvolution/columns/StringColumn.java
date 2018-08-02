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
import nz.co.gregs.dbvolution.datatypes.DBEnum;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing a string or character value.
 *
 * <p>
 * This class adds the necessary methods to use a string column like a string
 * expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in StringExpression to insert the column into the expression.
 *
 * <p>
 * Generally you get a StringColumn using
 * {@link RowDefinition#column(nz.co.gregs.dbvolution.datatypes.DBString)  RowDefinition.column(DBString)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see StringExpression
 */
public class StringColumn extends StringExpression implements ColumnProvider {

	private final static long serialVersionUID = 1l;

	private AbstractColumn column;

	private StringColumn() {

	}

	/**
	 * Create a StringColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public StringColumn(RowDefinition row, String field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a StringColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public StringColumn(RowDefinition row, DBString field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a StringColumn for the supplied field of the supplied row
	 *
	 * @param <E>
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public <E extends Enum<E> & DBEnumValue<String>> StringColumn(RowDefinition row, DBEnum<E, String> field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	public synchronized StringColumn copy() {
		final AbstractColumn col = getColumn();
		final DBRow row = col.getInstanceOfRow();
		StringColumn newInstance = new StringColumn(row, (DBString) col.getAppropriateQDTFromRow(row));
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
	 * @param column the value to compare this column's value to.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(DBString column) {
		return super.is(column);
	}

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}

}
