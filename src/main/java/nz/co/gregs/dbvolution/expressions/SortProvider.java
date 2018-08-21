/*
 * Copyright 2018 gregorygraham.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.expressions;

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.AbstractColumn;
import nz.co.gregs.dbvolution.columns.AbstractQueryColumn;
import nz.co.gregs.dbvolution.columns.QueryColumn;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBUnknownDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 * Provides the sorting expressions used in the ORDER BY clause.
 *
 * <p>
 * The best way to create a SortProvider is to use the appropriate {@link RangeExpression#ascending()
 * } or {@link RangeExpression#descending() } method of an expression or
 * column</p>
 *
 * <p>
 * For instance:
 * myDBRow.column(myDBRow.myDBString).substring(0,2).ascending()</p>
 *
 * @author gregorygraham
 */
public class SortProvider implements DBExpression {

	private final AnyExpression<?,?,?> innerExpression;
	private QueryColumn<?, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> queryColumn;

	public SortProvider() {
		innerExpression = null;
	}

	public SortProvider(AnyExpression<?,?,?> exp) {
		this.innerExpression = exp;
	}

	public <A, B extends AnyResult<A>, C extends QueryableDatatype<A>> SortProvider(QueryColumn<A, B, C> exp) {
		this();
		this.queryColumn = exp;
	}

	@Override
	public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
		if (hasQueryColumn()) {
			return getQueryColumn().asExpressionColumn();
		} else if (getInnerExpression() == null) {
			return new DBUnknownDatatype();
		} else {
			return this.getInnerExpression().getQueryableDatatypeForExpressionValue();
		}
	}

	@Override
	public boolean isAggregator() {
		if (hasQueryColumn()) {
			return getQueryColumn().isAggregator();
		} else if (getInnerExpression() == null) {
			return false;
		} else {
			return this.getInnerExpression().isAggregator();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Set<DBRow> getTablesInvolved() {
		if (hasQueryColumn()) {
			return getQueryColumn().getTablesInvolved();
		} else {
			Set<DBRow> result = new HashSet<DBRow>(0);
			final AnyExpression<?,?,?> innerExpression1 = this.getInnerExpression();
			if (innerExpression1 != null) {
				result = innerExpression1.getTablesInvolved();
			}
			return result;
		}
	}

	@Override
	public boolean isPurelyFunctional() {
		if (hasQueryColumn()) {
			return getQueryColumn().isPurelyFunctional();
		} else if (getInnerExpression() == null) {
			return true;
		} else {
			return this.getInnerExpression().isPurelyFunctional();
		}
	}

	@Override
	public boolean isComplexExpression() {
		if (hasQueryColumn()) {
			return getQueryColumn().isComplexExpression();
		} else if (getInnerExpression() == null) {
			return false;
		} else {
			return this.getInnerExpression().isComplexExpression();
		}
	}

	@Override
	public String createSQLForFromClause(DBDatabase database) {
		if (hasQueryColumn()) {
			return getQueryColumn().createSQLForFromClause(database);
		} else if (getInnerExpression() == null) {
			return database.getDefinition().getTrueOperation();
		} else {
			return this.getInnerExpression().createSQLForFromClause(database);
		}
	}

	@Override
	public String createSQLForGroupByClause(DBDatabase database) {
		if (hasQueryColumn()) {
			return getQueryColumn().createSQLForGroupByClause(database);
		} else if (getInnerExpression() == null) {
			return database.getDefinition().getTrueOperation();
		} else {
			return this.getInnerExpression().createSQLForGroupByClause(database);
		}
	}

	/**
	 * @return the innerExpression
	 */
	public AnyExpression<?,?,?> getInnerExpression() {
		return innerExpression;
	}

	public boolean hasQueryColumn() {
		return queryColumn != null;
	}

	public void setQueryColumn(QueryColumn<?, ?, ?> qc) {
		this.queryColumn = qc;
	}

	public QueryColumn<?, ?, ?> getQueryColumn() {
		return queryColumn;
	}

	public QueryableDatatype<?> asExpressionColumn() {
		if (hasQueryColumn()) {
			return getQueryColumn().asExpressionColumn();
		} else if (getInnerExpression() == null) {
			return new DBUnknownDatatype();
		} else {
			return getInnerExpression().asExpressionColumn();
		}
	}

	@Override
	public String toSQLString(DBDefinition defn) {
		String exprSQL = null;
		if (hasQueryColumn()) {
			exprSQL = getQueryColumn().toSQLString(defn);
		} else if (hasInnerExpression()) {
			exprSQL = getInnerExpression().toSQLString(defn);
		}
		return exprSQL == null
				? defn.getTrueOperation()
				: exprSQL
				+ getSortDirectionSQL(defn);
	}

	public boolean hasInnerExpression() {
		return getInnerExpression() != null;
	}

	@Override
	public DBExpression copy() {
		return new SortProvider(innerExpression);
	}

	/**
	 * Returns the sort order set for the provider.
	 * 
	 * <p>defaults to ASCENDING</p>
	 *
	 * @param defn
	 * @return SQL for the sort direction.
	 */
	public String getSortDirectionSQL(DBDefinition defn) {
		if (hasQueryColumn()) {
			final AbstractQueryColumn column = getQueryColumn().getColumn();
			return defn.getOrderByDirectionClause(column.getSortDirection());
		}
		return defn.getOrderByDirectionClause(QueryableDatatype.SORT_ASCENDING);
	}

	public static class Ascending extends SortProvider {

		public Ascending(AnyExpression<?,?,?> exp) {
			super(exp);
		}

		@Override
		public String getSortDirectionSQL(DBDefinition defn) {
			return defn.getOrderByDirectionClause(QueryableDatatype.SORT_ASCENDING);
		}

		@Override
		public DBExpression copy() {
			return new Ascending(getInnerExpression());
		}
	}

	public static class Descending extends SortProvider {

		public Descending(AnyExpression<?,?,?> exp) {
			super(exp);
		}

		@Override
		public String getSortDirectionSQL(DBDefinition defn) {
			return defn.getOrderByDirectionClause(QueryableDatatype.SORT_DESCENDING);
		}

		@Override
		public DBExpression copy() {
			return new Descending(getInnerExpression());
		}
	}

	public static class Column extends SortProvider {

		private final AbstractColumn innerColumn;

		public Column(AbstractColumn column) {
			super();
			this.innerColumn = column;
		}

		@Override
		public DBExpression copy() {
			return new Column(innerColumn);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return innerColumn.toSQLString(defn)
					+ getSortDirectionSQL(defn);
		}

		@Override
		public String getSortDirectionSQL(DBDefinition defn) {
			return defn.getOrderByDirectionClause(innerColumn.getSortDirection());
		}

		@Override
		public QueryableDatatype<?> asExpressionColumn() {
			return innerColumn.getQueryableDatatypeForExpressionValue();
		}

		@Override
		public AnyExpression<?,?,?> getInnerExpression() {
			return super.getInnerExpression();
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			if (innerColumn == null) {
				return database.getDefinition().getTrueOperation();
			} else {
				return innerColumn.createSQLForGroupByClause(database);
			}
		}

		@Override
		public String createSQLForFromClause(DBDatabase database) {
			if (innerColumn == null) {
				return database.getDefinition().getTrueOperation();
			} else {
				return innerColumn.createSQLForGroupByClause(database);
			}
		}

		@Override
		public boolean isComplexExpression() {
			return innerColumn.isComplexExpression();
		}

		@Override
		public boolean isPurelyFunctional() {
			return innerColumn.isPurelyFunctional();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return innerColumn.getTablesInvolved();
		}

		@Override
		public boolean isAggregator() {
			return innerColumn.isAggregator();
		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			return innerColumn.getQueryableDatatypeForExpressionValue();
		}

		public PropertyWrapper getPropertyWrapper() {
			return innerColumn.getPropertyWrapper();
		}
	}
}
