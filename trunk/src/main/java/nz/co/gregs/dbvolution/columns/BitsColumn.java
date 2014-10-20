/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.datatypes.DBBits;
import nz.co.gregs.dbvolution.expressions.BitsExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;


public class BitsColumn extends BitsExpression implements ColumnProvider{

	private final AbstractColumn column;

	/**
	 * Create a BooleanColumn for the supplied field of the supplied row
	 *
	 * @param row
	 * @param field
	 */
	public BitsColumn(RowDefinition row, byte[] field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a BooleanColumn for the supplied field of the supplied row
	 *
	 * @param row
	 * @param field
	 */
	public BitsColumn(RowDefinition row, DBBits field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return column.toSQLString(db);
	}

	@Override
	public BitsColumn copy() {
		return (BitsColumn) super.copy();
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
}
