/*
 * Copyright 2019 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
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
package nz.co.gregs.dbvolution.expressions.windows;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.EqualExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 *
 * @author gregorygraham
 * @param <A>
 */
public class WindowFunction<A extends EqualExpression<?,?,?>> implements WindowingFunctionInterface<A> {

	private final A innerExpression;

	public WindowFunction(A expression) {
		super();
		this.innerExpression = expression;
	}

	@Override
	public Partitioned<A> partition(ColumnProvider... cols) {
		return new WindowFunction.Partitioned<A>(this, cols);
	}

	public A allRows() {
		return this.partition().unsorted();
	}
	
	public A getInnerExpression(){
		return innerExpression;
	}

//	public A allRowsPreceding() {
//		return this.partition().unsortedWithFrame().rows().unboundedPrecedingAndCurrentRow();
//	}

	public A AllRowsAndOrderBy(SortProvider... sorts) {
		if (sorts.length > 0) {
			if (sorts.length > 1) {
				SortProvider sort = sorts[0];
				SortProvider[] newSorts = new SortProvider[sorts.length - 1];
				System.arraycopy(sorts, 1, newSorts, 0, sorts.length - 1);
				return this.partition().orderBy(sort, newSorts);
			} else {
				return this.partition().orderBy(sorts[0]);
			}
		} else {
			return this.partition().unsorted();
		}
	}

	@Override
	public String toSQLString(DBDefinition defn) {
		return innerExpression.toSQLString(defn) + " OVER (";
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<A> getRequiredExpressionClass() {
		return (Class<A>) innerExpression.getClass();
	}

	@SuppressWarnings("unchecked")
	@Override
	public WindowFunction<A> copy() {
		return new WindowFunction<A>((A) this.innerExpression.copy());
	}

	@Override
	public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public boolean isAggregator() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public boolean isPurelyFunctional() {
		boolean functional = innerExpression.isPurelyFunctional();
		return functional;
	}

	@Override
	public boolean isComplexExpression() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String createSQLForFromClause(DBDatabase database) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String createSQLForGroupByClause(DBDatabase database) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public boolean isWindowingFunction() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public static class Partitioned<A extends EqualExpression<?,?,?>> implements WindowingFunctionInterface.Partitioned<A> {

		private final WindowFunction<A> innerExpression;
		private final ColumnProvider[] columns;

		private Partitioned(WindowFunction<A> expression, ColumnProvider... cols) {
			super();
			this.innerExpression = expression;
			this.columns = cols;
		}

		@Override
		public A orderBy(SortProvider sort, SortProvider... sorts) {
			SortProvider[] newSorts = new SortProvider[sorts.length + 1];
			newSorts[0] = sort;
			System.arraycopy(sorts, 0, newSorts, 1, sorts.length);
			return new WindowFunction.Sorted<A>(this, newSorts).getRequiredExpression();
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			StringBuilder partitionClause = new StringBuilder();
			if (columns.length > 0) {
				partitionClause.append("PARTITION BY ");
				String separator = "";
				for (ColumnProvider partitionByColumn : columns) {
					partitionClause.append(separator).append(partitionByColumn.toSQLString(defn));
					separator = ", ";
				}
			}
			return innerExpression.toSQLString(defn) + partitionClause;
		}

		@Override
		public Class<A> getRequiredExpressionClass() {
			return innerExpression.getRequiredExpressionClass();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Partitioned<A> copy() {
			return new Partitioned<A>(this.innerExpression.copy(), this.columns);
		}

		@Override
		@SuppressWarnings("unchecked")
		public A unsorted() {
			return this.orderBy(BooleanExpression.trueExpression().ascending());
		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean isAggregator() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean isPurelyFunctional() {
			boolean functional = innerExpression.isPurelyFunctional();
			if (functional == true) {
				for (ColumnProvider column : columns) {
					functional = functional && column.isPurelyFunctional();
				}
			}
			return functional;
		}

		@Override
		public boolean isComplexExpression() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public String createSQLForFromClause(DBDatabase database) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean isWindowingFunction() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public A orderByWithPrimaryKeys(SortProvider... partitionFields) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public A orderByWithPrimaryKeys(ColumnProvider... partitionFields) {
			return orderByWithPrimaryKeys(SortProvider.getSortProviders(partitionFields));
		}

		@Override
		public A unordered() {
			return this.unsorted();
		}

	}

	public static class Sorted<A extends EqualExpression<?,?,?>> implements WindowingFunctionInterface.Sorted<A>, AnyResult<A> {

		private final WindowFunction.Partitioned<A> innerExpression;
		private final SortProvider[] sorts;

		public Sorted(WindowFunction.Partitioned<A> expression, SortProvider... sorts) {
			super();
			this.innerExpression = expression;
			this.sorts = sorts;
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			StringBuilder orderByClause = new StringBuilder();
			if (sorts.length > 0) {
				orderByClause.append(" ORDER BY ");
				String separator = "";
				for (SortProvider partitionByColumn : sorts) {
					orderByClause.append(separator).append(partitionByColumn.toSQLString(defn));
					separator = ", ";
				}
			}
			return innerExpression.toSQLString(defn) + orderByClause+")";
		}

		@Override
		public A getRequiredExpression() {
			try {
				final Class<A> clazz = innerExpression.getRequiredExpressionClass();
				Constructor<?>[] constructors = clazz.getDeclaredConstructors();
				for (Constructor<?> constructor : constructors) {
					if (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0].equals(AnyResult.class)) {
						constructor.setAccessible(true);
						@SuppressWarnings("unchecked")
						A newInstance = (A) constructor.newInstance((AnyResult) this);
						return newInstance;
					}
				}
			} catch (SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				Logger.getLogger(WindowFunction.class.getName()).log(Level.SEVERE, null, ex);
			}
			return null;
		}

		@Override
		public Class<A> getRequiredExpressionClass() {
			return innerExpression.getRequiredExpressionClass();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Sorted<A> copy() {
			return new Sorted<A>(this.innerExpression.copy(), this.sorts);
		}

		@Override
		public boolean isPurelyFunctional() {
			boolean functional = innerExpression.isPurelyFunctional();
			if (functional == true) {
				for (SortProvider sort : sorts) {
					functional = functional && sort.isPurelyFunctional();
				}
			}
			return functional;
		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return innerExpression.getTablesInvolved();
		}

		@Override
		public boolean isComplexExpression() {
			return false;
		}

		@Override
		public String createSQLForFromClause(DBDatabase database) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean isWindowingFunction() {
			return true;
		}

		@Override
		public boolean getIncludesNull() {
			return true;
		}
	}
	
	public static abstract class FrameEnd<A extends EqualExpression<?,?,?>> implements WindowingFunctionFramableInterface.WindowEnd<A>, AnyResult<A> {

		private final WindowPart<A> start;
		private final IntegerExpression offset;

		protected FrameEnd(WindowPart<A> start) {
			this.start = start;
			this.offset = IntegerExpression.value(0);
		}

		protected FrameEnd(WindowPart<A> start, int offset) {
			this.start = start;
			this.offset = IntegerExpression.value(offset);
		}

		protected FrameEnd(WindowPart<A> start, IntegerExpression offset) {
			this.start = start;
			this.offset = offset;
		}

		/**
		 * @return the and
		 */
		protected WindowPart<A> getStart() {
			return start;
		}

		/**
		 * @return the offset
		 */
		protected IntegerExpression getOffset() {
			return offset;
		}

		@Override
		public A getRequiredExpression() {
			try {
				final Class<A> clazz = getStart().getRequiredExpressionClass();
				Constructor<?>[] constructors = clazz.getDeclaredConstructors();
				for (Constructor<?> constructor : constructors) {
					if (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0].equals(AnyResult.class)) {
						constructor.setAccessible(true);
						@SuppressWarnings("unchecked")
						A newInstance = (A) constructor.newInstance((AnyResult) this);
						return newInstance;
					}
				}
			} catch (SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				Logger.getLogger(WindowFunction.class.getName()).log(Level.SEVERE, null, ex);
			}
			return null;
		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

//		@Override
//		public boolean isPurelyFunctional() {
//			throw new UnsupportedOperationException("Not supported yet.");
//		}
		@Override
		public boolean isComplexExpression() {
			return false;
		}

		@Override
		public String createSQLForFromClause(DBDatabase database) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean isWindowingFunction() {
			return true;
		}

		@Override
		public boolean getIncludesNull() {
			return true;
		}
	}
}
