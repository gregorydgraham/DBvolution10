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
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringEnum;
import nz.co.gregs.dbvolution.expressions.StringExpression;
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
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see StringExpression
 */
public class StringColumn extends StringExpression implements ColumnProvider {

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
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public StringColumn(RowDefinition row, DBStringEnum<?> field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return column.toSQLString(db);
	}

	@Override
	public synchronized StringColumn copy() {
		StringColumn newInstance;
		try {
			newInstance = this.getClass().newInstance();
			newInstance.column = column;
			return newInstance;
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
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
		return getTablesInvolved().size()==0;
	}

}
