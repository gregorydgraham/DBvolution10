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
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBIntegerEnum;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing an integer value.
 *
 * <p>
 * This class adds the necessary methods to use an integer column like an
 * integer expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in {@link NumberExpression} to insert the column into the expression.
 *
 * <p>
 * Generally you get an IntegerColumn using
 * {@link RowDefinition#column(nz.co.gregs.dbvolution.datatypes.DBInteger) RowDefinition.column(DBinteger)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see NumberExpression
 */
public class IntegerColumn extends IntegerExpression implements ColumnProvider {

	private AbstractColumn column;

	private IntegerColumn() {
	}

	/**
	 * Create a IntegerColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public IntegerColumn(RowDefinition row, Long field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a IntegerColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public IntegerColumn(RowDefinition row, Integer field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a IntegerColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public IntegerColumn(RowDefinition row, DBInteger field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a IntegerColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public IntegerColumn(RowDefinition row, DBIntegerEnum<?> field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return column.toSQLString(db);
	}

	@Override
	public synchronized IntegerColumn copy() {
		try {
			IntegerColumn newInstance = this.getClass().newInstance();
			newInstance.column = this.column;
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
	 * @param integerColumn the value we wish to find in this column.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(DBInteger integerColumn) {
		return super.is(integerColumn);
	}
}
