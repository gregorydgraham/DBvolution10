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
import nz.co.gregs.dbvolution.results.IntegerResult;

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
		return new IntegerExpression(new DBUnaryFunction(this) {
			@Override
			public String toSQLString(DBDefinition db) {
				final String toSQLString = super.toSQLString(db);
				System.out.println(toSQLString);
				return toSQLString; 
			}
			
			@Override
			String getFunctionName(DBDefinition db) {
				return db.getCountFunctionName();
			}

			@Override
			protected String afterValue(DBDefinition db) {
				return "(" + only.toSQLString(db) + ")";
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
	public IntegerExpression modeSimple() {
		IntegerExpression modeExpr = new IntegerExpression(
				new DBUnaryFunction(this) {
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
				final IntegerExpression expr = new IntegerExpression(getInnerResult());

				DBInteger count = expr.count().asExpressionColumn();

				count.setSortOrderDescending();

				Set<DBRow> tablesInvolved = this.getTablesInvolved();

				DBQuery query = database.getDBQuery(tablesInvolved);

				query.setBlankQueryAllowed(true)
						.setReturnFieldsToNone()
						.addExpressionColumn("mode", expr.asExpressionColumn())
						.addExpressionColumn("mode count", count)
						.setSortOrder(query.column(count))
						.setRowLimit(1);
				String tableAliasForObject = getInternalTableName(database);

				return "(" + query.getSQLForQuery().replaceAll("; *$", "") + ") " + tableAliasForObject;
			}

			public String getInternalTableName(DBDatabase database) {
				return database.getDefinition().getTableAliasForObject(this);
			}
		});

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

	private static abstract class DBUnaryFunction extends IntegerExpression {

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
}
