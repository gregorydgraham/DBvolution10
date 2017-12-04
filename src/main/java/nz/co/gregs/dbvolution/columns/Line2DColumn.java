/*
 * Copyright 2015 gregorygraham.
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

import java.util.Objects;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.expressions.Line2DExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Provides a portable representation of a column of Line2D values.
 *
 * @author Gregory Graham
 */
public class Line2DColumn extends Line2DExpression implements ColumnProvider {

	private final AbstractColumn column;

	/**
	 * Creates a portable reference to the column represented by the field of
	 * the row.
	 *
	 * @param row the table defining object that the field is a component of.
	 * @param field a component of the row.
	 */
	public Line2DColumn(RowDefinition row, DBLine2D field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public AbstractColumn getColumn() {
		return column;
	}

	@Override
	public void setUseTableAlias(boolean useTableAlias) {
		this.column.setUseTableAlias(useTableAlias);
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return column.toSQLString(db);
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return column.getTablesInvolved();
	}

	@Override
	public boolean isPurelyFunctional() {
		return column.isPurelyFunctional();
	}

	@Override
	public boolean isAggregator() {
		return column.isAggregator();
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Line2DColumn) {
			return column.equals(other); //To change body of generated methods, choose Tools | Templates.
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 89 * hash + Objects.hashCode(this.column);
		return hash;
	}
}
