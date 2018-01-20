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

import nz.co.gregs.dbvolution.results.UntypedResult;
import nz.co.gregs.dbvolution.datatypes.DBUntypedValue;
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

	/**
	 * Aggregrator that counts all the rows of the query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the count of all the values from the column.
	 */
	public static IntegerExpression countAll() {
		return new IntegerExpression(new DBNonaryFunction() {
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
		});
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
		return new IntegerExpression(new IntegerExpression(this) {
			@Override
			public String toSQLString(DBDefinition db) {
				return db.getCountFunctionName() + "(" + getInnerResult().toSQLString(db) + ")";
			}

			String getFunctionName(DBDefinition db) {
				return db.getCountFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	/**
	 * Creates an expression that will return the most common value of the column
	 * supplied.
	 *
	 * <p>
	 * MODE: The number which appears most often in a set of numbers. For example:
	 * in {6, 3, 9, 6, 6, 5, 9, 3} the Mode is 6.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public AnyExpression<?, ?, ?> modeSimple() {
		ModeSimpleExpression modeExpr
				= new ModeSimpleExpression(this);

		return modeExpr;
	}

	/**
	 * Creates an expression that will return the most common value of the column
	 * supplied.
	 *
	 * <p>
	 * MODE: The number which appears most often in a set of numbers. For example:
	 * in {6, 3, 9, 6, 6, 5, 9, 3} the Mode is 6.</p>
	 *
	 * <p>
	 * This version of Mode implements a stricter definition that will return null
	 * if the mode is undefined. The mode can be undefined if there are 2 or more
	 * values with the highest frequency value. </p>
	 *
	 * <p>
	 * For example in the list {0,0,0,0,1,1,2,2,2,2,3,4} both 0 and 2 occur four
	 * times and no other value occurs more frequently so the mode is undefined.
	 * {@link #modeSimple() The modeSimple()} method would return either 0 or 2
	 * randomly for the same set.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the mode or null if undefined.
	 */
	public IntegerExpression modeStrict() {
		IntegerExpression modeExpr
				= new IntegerExpression(new ModeStrictExpression(this));
		return modeExpr;
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

	private static abstract class DBUnaryFunction extends UntypedExpression implements UntypedResult {

		protected final EqualResult<?> only;

		DBUnaryFunction(EqualResult<?> only) {
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
		public DBUnaryFunction copy() {
			DBUnaryFunction newInstance;
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
		public boolean getIncludesNull() {
			return false;
		}

		@Override
		public boolean isPurelyFunctional() {
			return true;
		}
	}

	protected static class ModeSimpleExpression extends DBUnaryFunction {

		public ModeSimpleExpression(EqualResult<?> only) {
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
			final IntegerExpression expr = new IntegerExpression(getInnerResult());

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
					.setSortOrder(query.column(count))
					.setRowLimit(1);
			String tableAliasForObject = getInternalTableName(database);

			String sql = "(" + query.getSQLForQuery().replaceAll("; *$", "") + ") " + tableAliasForObject;
//			if (query.getQueryDetails().getOptions().isUseANSISyntax() && defn.requiresOnClauseForAllJoins()) {
//				sql = sql.replaceAll(tableAliasForObject,
//						tableAliasForObject
//						+ defn.beginOnClause()
//						+ (BooleanExpression.trueExpression().toSQLString(defn))
//						+ defn.endOnClause()
//				);
//			}
			return sql;
		}

		public String getInternalTableName(DBDatabase database) {
			return database.getDefinition().getTableAliasForObject(this);
		}

		@Override
		public UntypedResult expression(Object value) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public UntypedResult expression(UntypedResult value) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public UntypedResult expression(DBUntypedValue value) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public DBUntypedValue asExpressionColumn() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public StringExpression stringResult() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}
	}

	protected static class ModeStrictExpression extends DBUnaryFunction {

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

		public ModeStrictExpression(EqualResult<?> only) {
			super(only);
			expr1 = new IntegerExpression(getInnerResult());
			expr2 = new IntegerExpression(getInnerResult());

			mode1 = expr1.asExpressionColumn();
			mode2 = expr2.asExpressionColumn();

			count1 = expr1.count().asExpressionColumn();
			count1.setSortOrderDescending();

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
					.setSortOrder(query1.column(count1))
					.setRowLimit(1);

			DBQuery query2 = database.getDBQuery(tablesToUse);

			query2.setBlankQueryAllowed(true)
					.setReturnFieldsToNone()
					.addExpressionColumn(MODE2KEY, mode2)
					.addExpressionColumn(COUNTER2KEY, count2)
					.setSortOrder(query2.column(count2))
					.setRowLimit(1)
					.setPageRequired(1);
			final boolean useANSISyntax = query1.getQueryDetails().getOptions().isUseANSISyntax();

			String sql = "(" + query1.getSQLForQuery().replaceAll("; *$", "") + ") " + getFirstTableName(defn)
					+ System.getProperty("line.separator")
					+ (useANSISyntax ? " join " : ", ")
					+ System.getProperty("line.separator")
					+ "(" + query2.getSQLForQuery().replaceAll("; *$", "") + ") " + getSecondTableName(defn);

			if (useANSISyntax && defn.requiresOnClauseForAllJoins()) {
				sql = sql.replaceAll(getFirstTableName(defn),
						getFirstTableName(defn)
						+ defn.beginOnClause()
						+ (BooleanExpression.trueExpression().toSQLString(defn))
						+ defn.endOnClause());
//				sql = sql.replaceAll(getSecondTableName(defn),
//						getSecondTableName(defn)
//						+ defn.beginOnClause()
//						+ (BooleanExpression.trueExpression().toSQLString(defn))
//						+ defn.endOnClause());
			}
			return sql;
		}

		@Override
		public UntypedResult expression(Object value) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public UntypedResult expression(UntypedResult value) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public UntypedResult expression(DBUntypedValue value) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public DBUntypedValue asExpressionColumn() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public StringExpression stringResult() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
	}
}
