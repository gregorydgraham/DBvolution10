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

import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.results.RangeResult;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.EqualResult;
import nz.co.gregs.dbvolution.results.RangeComparable;

/**
 *
 * @author gregorygraham
 * @param <B> a base type like Number, String, or Date
 * @param <R> some RangeResult type like NumberResult that returns type B
 * @param <D> some QDT that works with type B
 *
 */
public abstract class RangeExpression<B, R extends RangeResult<B>, D extends QueryableDatatype<B>> extends InExpression<B, R, D> implements RangeComparable<B, R> {

	private static final long serialVersionUID = 1L;
	/**
	 *
	 * @param only
	 */
	protected RangeExpression(R only) {
		super(only);
	}

	protected RangeExpression() {
		super();
	}

	/**
	 *
	 * @param only
	 */
	protected RangeExpression(AnyResult<?> only) {
		super(only);
	}

	/* Default implementations*/
	@Override
	public BooleanExpression isLessThan(B value) {
		return isLessThan(this.expression(value));
	}

	public BooleanExpression isLessThan(D value) {
		return isLessThan(this.expression(value));
	}

	@Override
	public BooleanExpression isGreaterThan(B value) {
		return isGreaterThan(this.expression(value));
	}

	public BooleanExpression isGreaterThan(D value) {
		return isGreaterThan(this.expression(value));
	}

	@Override
	public BooleanExpression isLessThanOrEqual(B value) {
		return isLessThanOrEqual(this.expression(value));
	}

	public BooleanExpression isLessThanOrEqual(D value) {
		return isLessThanOrEqual(this.expression(value));
	}

	@Override
	public BooleanExpression isGreaterThanOrEqual(B value) {
		return isGreaterThanOrEqual(this.expression(value));
	}

	public BooleanExpression isGreaterThanOrEqual(D value) {
		return isGreaterThanOrEqual(this.expression(value));
	}

	@Override
	public BooleanExpression isLessThan(B value, BooleanExpression fallBackWhenEquals) {
		return isLessThan(this.expression(value), fallBackWhenEquals);
	}

	public BooleanExpression isLessThan(D value, BooleanExpression fallBackWhenEquals) {
		return isLessThan(this.expression(value), fallBackWhenEquals);
	}

	@Override
	public BooleanExpression isGreaterThan(B value, BooleanExpression fallBackWhenEquals) {
		return isGreaterThan(this.expression(value), fallBackWhenEquals);
	}

	public BooleanExpression isGreaterThan(D value, BooleanExpression fallBackWhenEquals) {
		return isGreaterThan(this.expression(value), fallBackWhenEquals);
	}

	@Override
	public BooleanExpression isBetween(R lowerBound, B upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetween(B lowerBound, R upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(R lowerBound, D upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(D lowerBound, R upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetween(B lowerBound, B upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(D lowerBound, D upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(B lowerBound, D upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(D lowerBound, B upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenInclusive(R lowerBound, B upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenInclusive(B lowerBound, R upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(R lowerBound, D upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(D lowerBound, R upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenInclusive(B lowerBound, B upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(D lowerBound, D upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(B lowerBound, D upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(D lowerBound, B upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenExclusive(R lowerBound, B upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenExclusive(B lowerBound, R upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(R lowerBound, D upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(D lowerBound, R upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenExclusive(B lowerBound, B upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(D lowerBound, D upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(B lowerBound, D upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(D lowerBound, B upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public static class UniqueRankingExpression<B, R extends EqualResult<B>, D extends QueryableDatatype<B>, X extends EqualExpression<B, R, D>> extends EqualExpression.DBUnaryFunction<B, R, D, X> {

		private final static long serialVersionUID = 1l;

		public UniqueRankingExpression(X only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return defn.formatExpressionAlias(this);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		protected String afterValue(DBDefinition db) {
			return "";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public boolean isComplexExpression() {
			return true;
		}

		@Override
		@SuppressWarnings("unchecked")
		public String createSQLForFromClause(DBDatabase database) {

			DBDefinition defn = database.getDefinition();
			final ColumnProvider inputExpression = (ColumnProvider) getInnerResult();

			DBRow table1 = inputExpression.getTablesInvolved().toArray(new DBRow[]{})[0];

			QueryableDatatype<?> inputExpressionQDT = inputExpression.getColumn().getAppropriateQDTFromRow(table1);
			final RangeExpression inputRangeExpression = (RangeExpression) inputExpression;
			final QueryableDatatype<?> pkQDT = table1.getPrimaryKeys().get(0);
			final ColumnProvider pkColumn = table1.column(pkQDT);
			final RangeExpression pkRangeExpression = (RangeExpression) pkColumn;
			final ColumnProvider t1ValueColumn = (ColumnProvider) inputRangeExpression;

			DBQuery dbQuery = database.getDBQuery(table1);

			final RangeExpression<?,?,?> t2UpdateCount = (RangeExpression) inputRangeExpression.copy();
			final RangeExpression<?,?,?> t2UIDMarque = (RangeExpression) pkRangeExpression.copy();
			Set<DBRow> tablesInvolved = t2UpdateCount.getTablesInvolved();
			for (DBRow table : tablesInvolved) {
				table.setTableVariantIdentifier("a2");
				dbQuery.addOptional(table);
			}
			tablesInvolved = t2UIDMarque.getTablesInvolved();
			for (DBRow table : tablesInvolved) {
				table.setTableVariantIdentifier("a2");
				dbQuery.addOptional(table);
			}

			dbQuery.setBlankQueryAllowed(true);

			dbQuery.addCondition(
					BooleanExpression.seekGreaterThan(
							inputRangeExpression, t2UpdateCount,
							pkRangeExpression, t2UIDMarque
					)
			);

			final DBInteger t1CounterExpr = inputRangeExpression.count().asExpressionColumn();
			final String counterKey = "Counter" + this;
			dbQuery.addExpressionColumn(counterKey, t1CounterExpr);
			ColumnProvider t1CounterColumn = dbQuery.column(t1CounterExpr);

			final QueryableDatatype<?> t1ValueExpr = inputRangeExpression.asExpressionColumn();
			final String valueExprKey = "Value" + this;
			dbQuery.addExpressionColumn(valueExprKey, t1ValueExpr);

			inputExpressionQDT.setSortOrderDescending();
			pkQDT.setSortOrderDescending();

			dbQuery.setSortOrder(t1ValueColumn.getSortProvider(), pkColumn.getSortProvider());
			dbQuery.setReturnFields(t1CounterColumn, t1ValueColumn, pkColumn);

			String sql = "(" + dbQuery.getSQLForQuery().replaceAll("; *$", "") + ") " + getFirstTableModeName(defn);
			return sql;
		}

		public String getInternalTableName(DBDatabase database) {
			return database.getDefinition().getTableAliasForObject(this);
		}

		private synchronized String getFirstTableModeName(DBDefinition defn) {
			return defn.formatExpressionAlias(this);
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			return "";
		}

	}
	
	public static class MedianExpression<B, R extends EqualResult<B>, D extends QueryableDatatype<B>, X extends EqualExpression<B, R, D>> extends EqualExpression.DBUnaryFunction<B, R, D, X> {

		private final static long serialVersionUID = 1l;

		public MedianExpression(X only) {
			super(only);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return defn.formatExpressionAlias(this);
		}

		@Override
		String getFunctionName(DBDefinition db) {
			return "";
		}

		@Override
		protected String afterValue(DBDefinition db) {
			return "";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public boolean isComplexExpression() {
			return true;
		}

		@Override
		public String createSQLForFromClause(DBDatabase database) {
			DBDefinition defn = database.getDefinition();
			

			String sql = "() " + getFirstTableModeName(defn);
			return sql;
		}

		public String getInternalTableName(DBDatabase database) {
			return database.getDefinition().getTableAliasForObject(this);
		}

		private synchronized String getFirstTableModeName(DBDefinition defn) {
			return defn.formatExpressionAlias(this);
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			return "";
		}

	}
}
