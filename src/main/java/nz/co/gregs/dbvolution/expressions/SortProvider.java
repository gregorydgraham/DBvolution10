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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 * Provides the sorting expressions used in the ORDER BY clause.
 *
 * <p>
 * The best way to create a SortProvider is to use the appropriate {@link AnyExpression#ascending()
 * } or {@link AnyExpression#descending() } method of an expression or
 * column</p>
 *
 * <p>
 * For instance:
 * myDBRow.column(myDBRow.myDBString).substring(0,2).ascending()</p>
 *
 * @author gregorygraham
 */
public class SortProvider implements DBExpression, Serializable {

	private static final long serialVersionUID = 1L;

	public static SortProvider[] getSortProviders(ColumnProvider[] columns) {
		SortProvider[] sorts = new SortProvider[columns.length];
		for (int i = 0; i < columns.length; i++) {
			sorts[i] = columns[i].ascending();
		}
		return sorts;
	}

	private final AnyExpression<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> innerExpression;
	private QueryColumn<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> queryColumn = null;
	private AbstractColumn innerColumn = null;
	protected Ordering direction = Ordering.UNDEFINED;
	protected OrderOfNulls nullsOrdering = OrderOfNulls.UNDEFINED;

	public List<String> getGroupByClauses(DBDefinition defn) {
		return new ArrayList<>();
	}

	public static enum Ordering {
		ASCENDING,
		UNDEFINED,
		DESCENDING;
	}

	public static enum OrderOfNulls {
		UNDEFINED(),
		LOWEST(),
		HIGHEST(),
		FIRST(),
		LAST();
	}

	protected SortProvider() {
		innerExpression = null;
	}

	protected SortProvider(SortProvider sort) {
		this.innerExpression = sort.innerExpression;
		this.direction = sort.direction;
		this.queryColumn = sort.queryColumn;
		this.innerColumn = sort.innerColumn;
	}

	protected <A, B extends AnyResult<A>, C extends QueryableDatatype<A>> SortProvider(AnyExpression<A, B, C> exp) {
		this.innerExpression = exp;
	}

	protected SortProvider(AnyExpression<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> exp, Ordering direction) {
		this.innerExpression = exp;
		this.direction = direction;
	}

	public SortProvider(AbstractColumn exp) {
		this();
		this.innerColumn = exp;
	}

	public <A, B extends AnyResult<A>, C extends QueryableDatatype<A>> SortProvider(QueryColumn<A, B, C> exp) {
		this();
		this.queryColumn = exp;
	}

	@Override
	public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
		if (hasQueryColumn()) {
			return getQueryColumn().asExpressionColumn();
		} else if (hasColumn()) {
			return getColumn().getQueryableDatatypeForExpressionValue();
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
		} else if (hasColumn()) {
			return getColumn().isAggregator();
		} else if (getInnerExpression() == null) {
			return false;
		} else {
			return this.getInnerExpression().isAggregator();
		}
	}

	@Override
	public boolean isWindowingFunction() {
		if (hasQueryColumn()) {
			return getQueryColumn().isWindowingFunction();
		} else if (hasColumn()) {
			return getColumn().isWindowingFunction();
		} else if (getInnerExpression() == null) {
			return false;
		} else {
			return this.getInnerExpression().isWindowingFunction();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Set<DBRow> getTablesInvolved() {
		if (hasQueryColumn()) {
			return getQueryColumn().getTablesInvolved();
		} else if (hasColumn()) {
			return getColumn().getTablesInvolved();
		} else {
			Set<DBRow> result = new HashSet<DBRow>(0);
			final AnyExpression<?, ?, ?> innerExpression1 = this.getInnerExpression();
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
		} else if (hasColumn()) {
			return getColumn().isPurelyFunctional();
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
		} else if (hasColumn()) {
			return getColumn().isComplexExpression();
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
		}
		if (hasColumn()) {
			return getColumn().createSQLForFromClause(database);
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
		} else if (hasColumn()) {
			return getColumn().createSQLForGroupByClause(database);
		} else if (getInnerExpression() == null) {
			return database.getDefinition().getTrueOperation();
		} else {
			return this.getInnerExpression().createSQLForGroupByClause(database);
		}
	}

	/**
	 * @return the innerExpression
	 */
	public AnyExpression<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> getInnerExpression() {
		if (innerExpression != null) {
			return innerExpression;
		} else {
			return (AnyExpression<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>>) innerColumn.getExpression();
		}
	}

	public boolean hasQueryColumn() {
		return queryColumn != null;
	}

	public boolean hasColumn() {
		return innerColumn != null;
	}

	protected AbstractColumn getColumn() {
		return innerColumn;
	}

	public void setQueryColumn(QueryColumn<?, ? extends AnyResult<?>, ?> qc) {
		this.queryColumn = qc;
	}

	public QueryColumn<?, ?, ?> getQueryColumn() {
		return queryColumn;
	}

	public QueryableDatatype<?> asExpressionColumn() {
		if (hasQueryColumn()) {
			return getQueryColumn().asExpressionColumn();
		} else if (hasColumn()) {
			return getColumn().getQueryableDatatypeForExpressionValue();
		} else if (getInnerExpression() == null) {
			return new DBUnknownDatatype();
		} else {
			return getInnerExpression().asExpressionColumn();
		}
	}

	@Override
	public String toSQLString(DBDefinition defn) {
		String exprSQL = getExpressionSQL(defn);
		return (exprSQL.isEmpty()
				? defn.getTrueOperation()
				: exprSQL)
				+ getSortDirectionSQL(defn);
	}

	protected String getExpressionSQL(DBDefinition defn) {
		String exprSQL = "";
		if (hasQueryColumn()) {
			exprSQL = getQueryColumn().toSQLString(defn);
		} else if (hasInnerExpression()) {
			exprSQL = defn.transformToSortableType(getInnerExpression()).toSQLString(defn);
		} else if (hasColumn()) {
			exprSQL = defn.transformToSortableType(getColumn()).toSQLString(defn);
		}
		return exprSQL;
	}

	protected boolean usesEmptyStringForNull(DBDefinition defn) {
		if (defn.supportsDifferenceBetweenNullAndEmptyStringNatively()) {
			if (hasQueryColumn()) {
				final Object queryColumn1 = getQueryColumn().asExpressionColumn();
				return (queryColumn1 instanceof StringExpression) && defn.requiredToProduceEmptyStringsForNull();
			} else if (hasInnerExpression()) {
				AnyExpression<?, ?, ?> expr = getInnerExpression();
				return (expr instanceof StringExpression) && defn.requiredToProduceEmptyStringsForNull();
			} else if (hasColumn()) {
				DBExpression expr = getColumn().asExpression();
				return (expr instanceof StringExpression) && defn.requiredToProduceEmptyStringsForNull();
			}
		}

		return false;
	}

	public boolean hasInnerExpression() {
		return getInnerExpression() != null || getColumn().hasExpression();
	}

	@Override
	public SortProvider copy() {
		return new SortProvider(this);
	}

	/**
	 * Returns the sort order set for the provider.
	 *
	 * <p>
	 * defaults to ASCENDING</p>
	 *
	 * @param defn the database definition
	 * @return SQL for the sort direction.
	 */
	public String getSortDirectionSQL(DBDefinition defn) {
		if (hasQueryColumn()) {
			final AbstractQueryColumn column = getQueryColumn().getColumn();
			return defn.getOrderByDirectionClause(column.getSortDirection());
		} else if (hasColumn()) {
			return defn.getOrderByDirectionClause(getColumn().getSortDirection());
		}
		return defn.getOrderByDirectionClause(getOrdering());
	}

	public Ordering getOrdering() {
		return direction;
	}

	public SortProvider nullsLast() {
		return new SortProvider.NullsLast(this);
	}

	public SortProvider nullsFirst() {
		return new SortProvider.NullsFirst(this);
	}

	public SortProvider nullsHighest() {
		return new SortProvider.NullsHighest(this);
	}

	public SortProvider nullsLowest() {
		return new SortProvider.NullsLowest(this);

	}

	public static class Ascending extends SortProvider {

		public Ascending(AnyExpression<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> exp) {
			super(exp, Ordering.ASCENDING);
		}

		@Override
		public String getSortDirectionSQL(DBDefinition defn) {
			return defn.getOrderByDirectionClause(QueryableDatatype.SORT_ASCENDING);
		}

		@Override
		public Ascending copy() {
			return new Ascending(getInnerExpression());
		}
	}

	public static class DefaultSort extends SortProvider {

		public DefaultSort(AnyExpression<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> exp) {
			super(exp, Ordering.UNDEFINED);
		}

		@Override
		public String getSortDirectionSQL(DBDefinition defn) {
			return defn.getOrderByDirectionClause(QueryableDatatype.SORT_UNSORTED);
		}

		@Override
		public DefaultSort copy() {
			return new DefaultSort(getInnerExpression());
		}
	}

	public static class Descending extends SortProvider {

		public Descending(AnyExpression<? extends Object, ? extends AnyResult<?>, ? extends QueryableDatatype<?>> exp) {
			super(exp, Ordering.DESCENDING);
		}

		@Override
		public String getSortDirectionSQL(DBDefinition defn) {
			return defn.getOrderByDirectionClause(QueryableDatatype.SORT_DESCENDING);
		}

		@Override
		public Descending copy() {
			return new Descending(getInnerExpression());
		}
	}

	public static class Column extends SortProvider {

		public Column(AbstractColumn column) {
			super(column);
		}

		@Override
		public QueryableDatatype<?> asExpressionColumn() {
			return getColumn().getQueryableDatatypeForExpressionValue();
		}

		public PropertyWrapper<?, ?, ?> getPropertyWrapper() {
			return getColumn().getPropertyWrapper();
		}

		public SortProvider descending() {
			return new Column(getColumn()) {
				@Override
				public Ordering getOrdering() {
					return Ordering.DESCENDING;
				}

				@Override
				public String getSortDirectionSQL(DBDefinition defn) {
					return defn.getOrderByDirectionClause(QueryableDatatype.SORT_DESCENDING);
				}
			};
		}

		public SortProvider ascending() {
			return new Column(getColumn()) {
				@Override
				public Ordering getOrdering() {
					return Ordering.ASCENDING;
				}

				@Override
				public String getSortDirectionSQL(DBDefinition defn) {
					return defn.getOrderByDirectionClause(QueryableDatatype.SORT_ASCENDING);
				}
			};
		}
	}

	public static abstract class NullsOrderer extends SortProvider {

		protected NullsOrderer(SortProvider sort, OrderOfNulls nullsOrdering) {
			super(sort);
			this.nullsOrdering = nullsOrdering;
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			String exprSQL = getExpressionSQL(defn);
			if (exprSQL == null || exprSQL.isEmpty()) {
				return defn.getTrueOperation();
			} else {
				String sortingSQL;
				if (!defn.requiredToProduceEmptyStringsForNull() && defn.supportsNullsOrderingStandard()) {
					// The standard "mycolumn ASC NULLS LAST" sort
					sortingSQL = getNullsOrderingNativeSQL(exprSQL, defn);
				} else {
					// The hacky "isnull(mycolumn, 1, 0) ASC, mycolumn ASC" alternative
					sortingSQL = getNullsOrderingSimulatedSQL(defn) + ", " + exprSQL + " " + getSortDirectionSQL(defn);
				}
				return sortingSQL;
			}
		}

		@Override
		public List<String> getGroupByClauses(DBDefinition defn) {
			List<String> listOfClauses = new ArrayList<>();
			String exprSQL = getExpressionSQL(defn);
			if (exprSQL == null || exprSQL.isEmpty()) {
				;
			} else {
				if (!defn.requiredToProduceEmptyStringsForNull() && defn.supportsNullsOrderingStandard()) {
					// The standard "mycolumn ASC NULLS LAST" sort
					;
				} else {
					// The hacky "isnull(mycolumn, 1, 0) ASC, mycolumn ASC" alternative
					final String nullsOrderingSimulatedSQL = getNullsOrderingSimulatedSQLExpression(defn)
							+ "/*SortProvider.getGroupByClauses*/";
					listOfClauses.add(nullsOrderingSimulatedSQL);
				}
			}
			return listOfClauses;
		}

		private String getNullsOrderingNativeSQL(String exprSQL, DBDefinition defn) {
			return exprSQL + getSortDirectionSQL(defn) + " " + getNullsOrderingStandardSQL(defn);
		}

		protected abstract String getNullsOrderingStandardSQL(DBDefinition defn);

		protected final String simulateNullsFirst(DBDefinition defn) {
			return getNullsOrderingSimulatedSQLForNullsFirst(defn)
					+ " "
					+ defn.getOrderByAscending();
		}

		protected String getNullsOrderingSimulatedSQLForNullsFirst(DBDefinition defn) {
			if (usesEmptyStringForNull(defn)) {
				return defn.doIfEmptyStringThenElse(getExpressionSQL(defn), "0", "1");
			} else {
				return defn.doIfThenElseTransform(defn.doIsNullTransform(getExpressionSQL(defn)), "0", "1");
			}
		}

		protected final String simulateNullsLast(DBDefinition defn) {
			return getNullsOrderingSimulatedSQLForNullsLast(defn)
					+ " "
					+ defn.getOrderByAscending();
		}

		protected String getNullsOrderingSimulatedSQLForNullsLast(DBDefinition defn) {
			if (usesEmptyStringForNull(defn)) {
				return defn.doIfEmptyStringThenElse(getExpressionSQL(defn), "1", "0");
			} else {
				return defn.doIfThenElseTransform(defn.doIsNullTransform(getExpressionSQL(defn)), "1", "0");
			}
		}

		abstract String getNullsOrderingSimulatedSQL(DBDefinition defn);

		abstract String getNullsOrderingSimulatedSQLExpression(DBDefinition defn);
	}

	public static class NullsLast extends NullsOrderer {

		public NullsLast(SortProvider sort) {
			super(sort, OrderOfNulls.LAST);
		}

		@Override
		public String getNullsOrderingStandardSQL(DBDefinition defn) {
			return defn.getNullsLast();
		}

		@Override
		String getNullsOrderingSimulatedSQL(DBDefinition defn) {
			return simulateNullsLast(defn);
		}

		@Override
		String getNullsOrderingSimulatedSQLExpression(DBDefinition defn) {
			return getNullsOrderingSimulatedSQLForNullsLast(defn);
		}
	}

	private static class NullsFirst extends NullsOrderer {

		public NullsFirst(SortProvider sort) {
			super(sort, OrderOfNulls.FIRST);
		}

		@Override
		public String getNullsOrderingStandardSQL(DBDefinition defn) {
			return defn.getNullsFirst();
		}

		@Override
		String getNullsOrderingSimulatedSQL(DBDefinition defn) {
			return simulateNullsFirst(defn);
		}

		@Override
		String getNullsOrderingSimulatedSQLExpression(DBDefinition defn) {
			return getNullsOrderingSimulatedSQLForNullsFirst(defn);
		}
	}

	private static class NullsHighest extends NullsOrderer {

		public NullsHighest(SortProvider sort) {
			super(sort, OrderOfNulls.HIGHEST);
		}

		@Override
		public String getNullsOrderingStandardSQL(DBDefinition defn) {
			switch (getOrdering()) {
				case ASCENDING:
					return defn.getNullsLast();
				case DESCENDING:
					return defn.getNullsFirst();
				default:
					return defn.getNullsLast();
			}
		}

		@Override
		String getNullsOrderingSimulatedSQL(DBDefinition defn) {
			switch (getOrdering()) {
				case ASCENDING:
					return simulateNullsLast(defn);
				case DESCENDING:
					return simulateNullsFirst(defn);
				default:
					return simulateNullsLast(defn);
			}
		}

		@Override
		String getNullsOrderingSimulatedSQLExpression(DBDefinition defn) {
			switch (getOrdering()) {
				case ASCENDING:
					return getNullsOrderingSimulatedSQLForNullsLast(defn);
				case DESCENDING:
					return getNullsOrderingSimulatedSQLForNullsFirst(defn);
				default:
					return getNullsOrderingSimulatedSQLForNullsLast(defn);
			}
		}

	}

	private static class NullsLowest extends NullsOrderer {

		public NullsLowest(SortProvider sort) {
			super(sort, OrderOfNulls.LOWEST);
		}

		@Override
		public String getNullsOrderingStandardSQL(DBDefinition defn) {
			switch (getOrdering()) {
				case ASCENDING:
					return defn.getNullsFirst();
				case DESCENDING:
					return defn.getNullsLast();
				default:
					return defn.getNullsFirst();
			}
		}

		@Override
		String getNullsOrderingSimulatedSQL(DBDefinition defn) {
			switch (getOrdering()) {
				case ASCENDING:
					return simulateNullsFirst(defn);
				case DESCENDING:
					return simulateNullsLast(defn);
				default:
					return simulateNullsFirst(defn);
			}
		}

		@Override
		String getNullsOrderingSimulatedSQLExpression(DBDefinition defn) {
			switch (getOrdering()) {
				case ASCENDING:
					return getNullsOrderingSimulatedSQLForNullsFirst(defn);
				case DESCENDING:
					return getNullsOrderingSimulatedSQLForNullsLast(defn);
				default:
					return getNullsOrderingSimulatedSQLForNullsFirst(defn);
			}
		}
	}
}
