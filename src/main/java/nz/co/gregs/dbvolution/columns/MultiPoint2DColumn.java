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

import com.vividsolutions.jts.geom.MultiPoint;
import java.util.Objects;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.expressions.spatial2D.MultiPoint2DExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Provides a portable representation of a column of Line2D values.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MultiPoint2DColumn extends MultiPoint2DExpression implements ColumnProvider {

	private final static long serialVersionUID = 1l;

	private final AbstractColumn column;

	/**
	 * Creates a portable reference to the column represented by the field of the
	 * row.
	 *
	 * @param row the table defining object that includes the field.
	 * @param field the field that defines the column.
	 */
	public MultiPoint2DColumn(RowDefinition row, DBMultiPoint2D field) {
		this.column = new AbstractColumn(row, field);
	}

	public MultiPoint2DColumn(RowDefinition row, MultiPoint field) {
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
	public String toSQLString(DBDefinition db) {
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
		if (other instanceof MultiPoint2DColumn) {
			return column.equals(((MultiPoint2DColumn) other).column);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 59 * hash + Objects.hashCode(this.column);
		return hash;
	}

	@Override
	public synchronized MultiPoint2DColumn copy() {
		final AbstractColumn col = getColumn();
		final DBRow row = col.getInstanceOfRow();
		MultiPoint2DColumn newInstance = new MultiPoint2DColumn(row, (DBMultiPoint2D) col.getAppropriateQDTFromRow(row));
		return newInstance;
	}

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}

}
