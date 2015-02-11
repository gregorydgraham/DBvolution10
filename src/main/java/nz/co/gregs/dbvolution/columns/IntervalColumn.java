/*
 * Copyright 2015 gregory.graham.
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
import nz.co.gregs.dbvolution.datatypes.DBInterval;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.IntervalExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.joda.time.Period;


public class IntervalColumn extends IntervalExpression implements ColumnProvider {
	
	private AbstractColumn column;

	private IntervalColumn() {
	}

	/**
	 * Create a DateColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public IntervalColumn(RowDefinition row, Period field) {
		this.column = new AbstractColumn(row, field);
	}


	/**
	 * Create a DateColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public IntervalColumn(RowDefinition row, DBInterval field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return column.toSQLString(db);
	}

	@Override
	public synchronized IntervalColumn copy() {
		try {
			IntervalColumn newInstance = this.getClass().newInstance();
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
	
	/**
	 * Create an expression to compare this column to the other column using EQUALS.
	 *
	 * @param intervalColumn  
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(DBInterval intervalColumn){
		return super.is(intervalColumn);
	}
}
