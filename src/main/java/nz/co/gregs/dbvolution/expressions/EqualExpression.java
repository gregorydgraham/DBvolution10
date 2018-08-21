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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.BooleanResult;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.EqualResult;

/**
 *
 * @author gregorygraham
 * @param <B>
 * @param <R>
 * @param <D>
 */
public abstract class EqualExpression<B, R extends EqualResult<B>, D extends QueryableDatatype<B>> extends AnyExpression<B, R, D> implements EqualResult<B>, EqualComparable<B, R> {

	private static final long serialVersionUID = 1L;

	/**
	 *
	 * @param only
	 */
	protected EqualExpression(R only) {
		super(only);
	}

	/**
	 *
	 * @param only
	 */
	protected EqualExpression(AnyResult<?> only) {
		super(only);
	}

	protected EqualExpression() {
		super();
	}

	public BooleanExpression is(D value) {
		return this.is(this.expression(value));
	}

	public BooleanExpression isNot(D value) {
		return this.isNot(this.expression(value));
	}

	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	/**
	 * Aggregrator that counts all the rows of the query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the count of all the values from the column.
	 */
	public static IntegerExpression countAll() {
		return new IntegerExpression(new CountAllExpression());
	}

	/**
	 * Creates an expression that will count all the values of the column
	 * supplied.
	 *
	 * <p>
	 * Count is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public IntegerExpression count() {
		return new IntegerExpression(new CountExpression(this));
	}

	/**
	 * Aggregrator that counts this row if the booleanResult is true.
	 *
	 * @param booleanResult an value that will be TRUE when the row needs to be
	 * counted.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The number of rows where the test is true.
	 */
	public static IntegerExpression countIf(BooleanResult booleanResult) {
		return new IntegerExpression(new BooleanExpression(booleanResult).ifThenElse(1L, 0L)).sum();
	}

	private static abstract class DBNonaryFunction extends IntegerExpression {

	private final static long serialVersionUID = 1l;

		DBNonaryFunction() {
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return " " + getFunctionName(db) + "";
		}

		protected String afterValue(DBDefinition db) {
			return " ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + this.afterValue(db);
		}

		@Override
		public DBNonaryFunction copy() {
			DBNonaryFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return new HashSet<DBRow>();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			return true;
		}
	}

	protected static abstract class DBUnaryFunction<B, R extends EqualResult<B>, D extends QueryableDatatype<B>, X extends EqualExpression<B, R, D>> extends EqualExpression<B, R, D> implements EqualResult<B> {

		private static final long serialVersionUID = 1L;
		protected X only;

		DBUnaryFunction(X only) {
			super(only);
			this.only = only;
		}

		abstract String getFunctionName(DBDefinition db);

		protected String beforeValue(DBDefinition db) {
			return " " + getFunctionName(db) + "";
		}

		protected String afterValue(DBDefinition db) {
			return " ";
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return this.beforeValue(db) + this.afterValue(db);
		}

		@Override
		@SuppressWarnings("unchecked")
		public DBUnaryFunction<B, R, D, X> copy() {
			DBUnaryFunction<B, R, D, X> newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.only = (X) this.only.copy();
			return newInstance;
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			return true;
		}

		@Override
		public R expression(B value) {
			return only.expression(value);
		}

		@Override
		public R expression(R value) {
			return only.expression(value);
		}

		@Override
		public R expression(D value) {
			return only.expression(value);
		}

		@Override
		public D asExpressionColumn() {
			return only.asExpressionColumn();
		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			return only.getQueryableDatatypeForExpressionValue();
		}

		@Override
		public StringExpression stringResult() {
			return only.stringResult();
		}

		@Override
		public BooleanExpression is(R anotherInstance) {
			return only.is(anotherInstance);
		}

		@Override
		public BooleanExpression isNot(R anotherInstance) {
			return only.isNot(anotherInstance);
		}

		@Override
		public BooleanExpression is(B anotherInstance) {
			return only.is(anotherInstance);
		}

		@Override
		public BooleanExpression isNot(B anotherInstance) {
			return only.isNot(anotherInstance);
		}
	}

	public static class ModeSimpleExpression<B, R extends EqualResult<B>, D extends QueryableDatatype<B>, X extends EqualExpression<B, R, D>> extends DBUnaryFunction<B, R, D, X> {

		private final static long serialVersionUID = 1l;

		public ModeSimpleExpression(X only) {
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

			final X expr = only;

			DBInteger count = expr.count().asExpressionColumn();

			count.setSortOrderDescending();

			Set<DBRow> tablesInvolved = this.getTablesInvolved();
			List<DBRow> tablesToUse = new ArrayList<>(0);
			for (DBRow dBRow : tablesInvolved) {
				tablesToUse.add(DBRow.copyDBRow(dBRow));
			}

			DBQuery query = database.getDBQuery(tablesToUse);

			query.setBlankQueryAllowed(true)
					.setReturnFieldsToNone()
					.addExpressionColumn(this, expr.asExpressionColumn())
					.addExpressionColumn("mode count", count)
					.setSortOrder(query.column(count).descending())
					.setRowLimit(1);
			String tableAliasForObject = getInternalTableName(database);

			String sql = "(" + query.getSQLForQuery().replaceAll("; *$", "") + ") " + tableAliasForObject;
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
			DBDefinition defn = database.getDefinition();
			return ""
					+ getInternalTableName(database) + "." + getFirstTableModeName(defn);
		}

		@Override
		@SuppressWarnings("unchecked")
		public ModeSimpleExpression<B, R, D, X> copy() {
			return new ModeSimpleExpression<B, R, D, X>((X) (only == null ? null : only.copy()));
		}

	}

	public static class ModeStrictExpression<B, R extends EqualResult<B>, D extends QueryableDatatype<B>, X extends EqualExpression<B, R, D>> extends DBUnaryFunction<B, R, D, X> {

		private final static long serialVersionUID = 1l;

		private String tableAlias = null;
		private String firstTableCounterName = null;
		private String secondTableCounterName = null;
		private String firstTableModeName = null;
		private String secondTableModeName = null;
		private String firstTableName = null;
		private String secondTableName = null;
		private final IntegerExpression expr1;
		private final IntegerExpression expr2;
		private final DBInteger mode1;
		private final DBInteger mode2;
		private final DBInteger count1;
		private final DBInteger count2;

		private static final Object COUNTER1KEY = new Object();
		private static final Object MODE1KEY = new Object();
		private static final Object COUNTER2KEY = new Object();
		private static final Object MODE2KEY = new Object();

		public ModeStrictExpression(X only) {
			super(only);
			expr1 = new IntegerExpression(getInnerResult());
			expr2 = new IntegerExpression(getInnerResult());

			mode1 = expr1.asExpressionColumn();
			count1 = expr1.count().asExpressionColumn();
			count1.setSortOrderDescending();

			mode2 = expr2.asExpressionColumn();
			count2 = expr2.count().asExpressionColumn();
			count2.setSortOrderDescending();
		}

		@Override
		public String toSQLString(DBDefinition defn) {

			return "case when " + getFirstTableName(defn) + "." + getFirstTableCounterName(defn)
					+ " = " + getSecondTableName(defn) + "." + getSecondTableCounterName(defn)
					+ " then null else " + getFirstTableName(defn) + "." + getFirstTableModeName(defn) + " end ";
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

			final DBDefinition defn = database.getDefinition();

			Set<DBRow> tablesInvolved = this.getTablesInvolved();
			List<DBRow> tablesToUse = new ArrayList<>(0);

			for (DBRow dBRow : tablesInvolved) {
				tablesToUse.add(DBRow.copyDBRow(dBRow));
			}

			DBQuery query1 = database.getDBQuery(tablesToUse);

			query1.setBlankQueryAllowed(true)
					.setReturnFieldsToNone()
					.addExpressionColumn(MODE1KEY, mode1)
					.addExpressionColumn(COUNTER1KEY, count1)
					.setSortOrder(query1.column(count1).descending())
					.setRowLimit(1);

			DBQuery query2 = database.getDBQuery(tablesToUse);

			query2.setBlankQueryAllowed(true)
					.setReturnFieldsToNone()
					.addExpressionColumn(MODE2KEY, mode2)
					.addExpressionColumn(COUNTER2KEY, count2)
					.setSortOrder(query2.column(count2).descending())
					.setRowLimit(1)
					.setPageRequired(1);
			final boolean useANSISyntax = query1.getQueryDetails().getOptions().isUseANSISyntax();
			final String linefeed = System.getProperty("line.separator");

			String sql = "(" + query1.getSQLForQuery().replaceAll("; *$", "") + ") " + getFirstTableName(defn)
					+ linefeed
					+ (useANSISyntax ? " join " : ", ")
					+ linefeed
					+ "(" + query2.getSQLForQuery().replaceAll("; *$", "") + ") " + getSecondTableName(defn);

			if (useANSISyntax && defn.requiresOnClauseForAllJoins()) {
				sql = sql.replaceAll(getFirstTableName(defn),
						getFirstTableName(defn)
						+ defn.beginOnClause()
						+ (BooleanExpression.trueExpression().toSQLString(defn))
						+ defn.endOnClause());
			}
			return sql;
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			DBDefinition defn = database.getDefinition();
			return ""
					+ getFirstTableName(defn) + "." + getFirstTableModeName(defn) + ", "
					+ getFirstTableName(defn) + "." + getFirstTableCounterName(defn) + ", "
					+ getSecondTableName(defn) + "." + getSecondTableCounterName(defn);
		}

		public synchronized String getInternalTableName(DBDefinition database) {
			if (tableAlias == null) {
				tableAlias = database.getTableAliasForObject(this);
			}
			return tableAlias;
		}

		private synchronized String getFirstTableName(DBDefinition database) {
			if (firstTableName == null) {
				firstTableName = getInternalTableName(database) + 1;
			}
			return firstTableName;
		}

		private synchronized String getSecondTableName(DBDefinition database) {
			if (secondTableName == null) {
				secondTableName = getInternalTableName(database) + 2;
			}
			return secondTableName;
		}

		private synchronized String getFirstTableCounterName(DBDefinition defn) {
			if (firstTableCounterName == null) {
				firstTableCounterName = defn.formatExpressionAlias(COUNTER1KEY);
			}
			return firstTableCounterName;
		}

		private synchronized String getFirstTableModeName(DBDefinition defn) {
			if (firstTableModeName == null) {
				firstTableModeName = defn.formatExpressionAlias(MODE1KEY);
			}
			return firstTableModeName;
		}

		private synchronized String getSecondTableModeName(DBDefinition defn) {
			if (secondTableModeName == null) {
				secondTableModeName = defn.formatExpressionAlias(MODE2KEY);
			}
			return secondTableModeName;
		}

		private synchronized String getSecondTableCounterName(DBDefinition defn) {
			if (secondTableCounterName == null) {
				secondTableCounterName = defn.formatExpressionAlias(COUNTER2KEY);
			}
			return secondTableCounterName;
		}

		@Override
		@SuppressWarnings("unchecked")
		public ModeStrictExpression<B, R, D, X> copy() {
			return new ModeStrictExpression<B, R, D, X>((X) (only == null ? null : only.copy()));
		}

	}

	private static class CountAllExpression extends DBNonaryFunction {

		public CountAllExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		String getFunctionName(DBDefinition db) {
			return db.getCountFunctionName();
		}

		@Override
		protected String afterValue(DBDefinition db) {
			return "(*)";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public CountAllExpression copy() {
			return new CountAllExpression();
		}

	}

	private static class CountExpression extends IntegerExpression {

		public CountExpression(AnyResult<?> only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getCountFunctionName() + "(" + getInnerResult().toSQLString(db) + ")";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public CountExpression copy() {
			return new CountExpression(
					(AnyResult<?>) (getInnerResult() == null ? null : getInnerResult().copy())
			);
		}

	}

}
