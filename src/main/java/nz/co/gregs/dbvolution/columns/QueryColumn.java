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
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.AnyExpression;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 * Represents a database column storing a boolean value.
 *
 * <p>
 * This class adds the necessary methods to use a boolean column like a boolean
 * expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in BooleanExpression to insert the column into the expression.
 *
 * <p>
 * Generally you get a BooleanColumn using
 * {@link RowDefinition#column(java.lang.Boolean) RowDefinition.column(DBBoolean)}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <B>
 * @param <R>
 * @param <D>
 * @see RowDefinition
 * @see AbstractColumn
 * @see BooleanExpression
 */
public class QueryColumn<B, R extends AnyResult<B>, D extends QueryableDatatype<B>> extends AnyExpression<B, R, D> implements ColumnProvider {

	private static final long serialVersionUID = 1l;

	private final AbstractQueryColumn column;
	private final DBQuery query;
	private final D field;

	/**
	 * Create a QueryColumn for the supplied field of the supplied query
	 *
	 * @param query the query that the field belongs to
	 * @param field the field that represents the column
	 */
	public QueryColumn(DBQuery query, D field) {
		this.column = new AbstractQueryColumn(query, field);
		this.query = query;
		this.field = field;
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	@SuppressWarnings("unchecked")
	public QueryColumn<B, R, D> copy() {
		return new QueryColumn<B, R, D>(query, (D) field.copy());
	}

	@Override
	public AbstractQueryColumn getColumn() {
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
	public R expression(B value) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public R expression(R value) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public R expression(D value) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	@SuppressWarnings("unchecked")
	public D asExpressionColumn() {
		return (D) field.copy();
	}

	@Override
	@SuppressWarnings("unchecked")
	public D getQueryableDatatypeForExpressionValue() {
		return field;
	}

	@Override
	public SortProvider getSortProvider() {
		return new SortProvider(this);
	}
}
