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
package nz.co.gregs.dbvolution.expressions;

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
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 *
 * @author gregorygraham
 * @param <A>
 */
public class WindowFunction<A extends EqualExpression> implements WindowingFunctionInterface<A> {

	private final A innerExpression;

	protected WindowFunction(A expression) {
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

	public A AllRowsAndOrderBy(SortProvider... sorts) {
		if (sorts.length > 0) {
			if (sorts.length > 1) {
				SortProvider sort = sorts[0];
				SortProvider[] newSorts = new SortProvider[sorts.length-1];
				System.arraycopy(sorts, 1, newSorts, 0, sorts.length-1);
				return this.partition().orderBy(sort, newSorts).rows().unboundedPreceding().currentRow();
			} else {
				return this.partition().orderBy(sorts[0]).rows().unboundedPrecedingAndCurrentRow();
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
	private WindowFunction<A> copy() {
		return new WindowFunction<A>((A) this.innerExpression.copy());
	}

	public static class Partitioned<A extends EqualExpression> implements WindowingFunctionInterface.Partitioned<A> {

		private final WindowFunction<A> innerExpression;
		private final ColumnProvider[] columns;

		private Partitioned(WindowFunction<A> expression, ColumnProvider... cols) {
			super();
			this.innerExpression = expression;
			this.columns = cols;
		}

		@Override
		public WindowFunction.Sorted<A> orderBy(SortProvider sort, SortProvider... sorts) {
			SortProvider[] newSorts = new SortProvider[sorts.length + 1];
			newSorts[0] = sort;
			System.arraycopy(sorts, 0, newSorts, 1, sorts.length);
			return new WindowFunction.Sorted<A>(this, newSorts);
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
			return this.orderBy(BooleanExpression.trueExpression().ascending()).defaultFrame();
		}

		@Override
		@SuppressWarnings("unchecked")
		public A unordered() {
			return this.orderBy(BooleanExpression.trueExpression().ascending()).defaultFrame();
		}

	}

	public static class Sorted<A extends EqualExpression> implements WindowingFunctionInterface.Sorted<A> {

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
			return innerExpression.toSQLString(defn) + orderByClause;
		}

		public A defaultFrame() {
			return this.rows().unboundedPreceding().unboundedFollowing();
		}

		@Override
		public Rows<A> rows() {
			return new Rows<A>(this);
		}

		/* MS SQL Server 2017 does not support groups */
//		@Override
//		public Groups<A> groups() {
//			return new Groups<A>(this);
//		}

		@Override
		public Range<A> range() {
			return new Range<A>(this);
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

	}

	public static abstract class FrameType<A extends EqualExpression> implements WindowingFunctionInterface.FrameType<A> {

		private final Sorted<A> sorted;

		public FrameType(Sorted<A> sorted) {
			super();
			this.sorted = sorted;
		}

		protected Sorted getSorted() {
			return sorted;
		}

		public A unboundedPrecedingAndCurrentRow() {
			return this.unboundedPreceding().currentRow();
		}

		public A offsetPrecedingAndCurrentRow(int offset) {
			return this.preceding(offset).currentRow();
		}

		public A offsetPrecedingAndCurrentRow(IntegerExpression offset) {
			return this.preceding(offset).currentRow();
		}

		public A onlyCurrentRow() {
			return this.currentRow().currentRow();
		}

		@Override
		public FrameStart<A> unboundedPreceding() {
			return new UnboundedPrecedingStart<A>(this);
		}

		@Override
		public FrameStart<A> preceding(int offset) {
			return new OffsetPrecedingStart<A>(this, offset);
		}

		@Override
		public FrameStart<A> preceding(IntegerExpression offset) {
			return new OffsetPrecedingStart<A>(this, offset);
		}

		@Override
		public FrameStart<A> currentRow() {
			return new CurrentRowStart<A>(this);
		}

		@Override
		public FrameStart<A> following(int offset) {
			return new OffsetFollowingStart<A>(this, offset);
		}

		@Override
		public FrameStart<A> following(IntegerExpression offset) {
			return new OffsetFollowingStart<A>(this, offset);
		}

		@Override
		public FrameStart<A> unboundedFollowing() {
			return new UnboundedFollowingStart<A>(this);
		}

		@Override
		public Class<A> getRequiredExpressionClass() {
			return sorted.getRequiredExpressionClass();
		}

		@Override
		public abstract FrameType<A> copy();
	}

	public static class Range<A extends EqualExpression> extends FrameType<A> {

		public Range(Sorted<A> sorted) {
			super(sorted);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getSorted().toSQLString(defn) + " RANGE BETWEEN ";
		}

		@Override
		@SuppressWarnings("unchecked")
		public Range<A> copy() {
			return new Range<A>(this.getSorted().copy());
		}
	}

	public static class Rows<A extends EqualExpression> extends FrameType<A> {

		public Rows(Sorted<A> sorted) {
			super(sorted);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getSorted().toSQLString(defn) + " ROWS BETWEEN ";
		}

		@Override
		@SuppressWarnings("unchecked")
		public Rows<A> copy() {
			return new Rows<A>(this.getSorted().copy());
		}
	}

	public static class Groups<A extends RangeExpression> extends FrameType<A> {

		public Groups(Sorted<A> sorted) {
			super(sorted);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getSorted().toSQLString(defn) + " GROUPS BETWEEN ";
		}

		@Override
		@SuppressWarnings("unchecked")
		public Groups<A> copy() {
			return new Groups<A>(this.getSorted().copy());
		}
	}

	public static abstract class FrameStart<A extends EqualExpression> implements WindowingFunctionInterface.FrameStart<A> {

		protected final FrameType<A> type;
		protected final IntegerExpression offset;

		public FrameStart(FrameType<A> type) {
			super();
			this.type = type;
			this.offset = IntegerExpression.value(1);
		}

		public FrameStart(FrameType<A> type, int offset) {
			super();
			this.type = type;
			this.offset = IntegerExpression.value(offset);
		}

		public FrameStart(FrameType<A> type, IntegerExpression offset) {
			super();
			this.type = type;
			this.offset = offset;
		}

		@Override
		@SuppressWarnings("unchecked")
		public A unboundedPreceding() {
			return new UnboundedPrecedingEnd<A>(this).getRequiredExpression();
		}

		@Override
		@SuppressWarnings("unchecked")
		public A preceding(int offset) {
			return new OffsetPrecedingEnd<A>(this, offset).getRequiredExpression();
		}

		@Override
		public A preceding(IntegerExpression offset) {
			return new OffsetPrecedingEnd<A>(this, offset).getRequiredExpression();
		}

		@Override
		public A currentRow() {
			return new CurrentRowEnd<A>(this).getRequiredExpression();
		}

		@Override
		public A following(int offset) {
			return new OffsetFollowingEnd<A>(this, offset).getRequiredExpression();
		}

		@Override
		public A following(IntegerExpression offset) {
			return new OffsetFollowingEnd<A>(this, offset).getRequiredExpression();
		}

		@Override
		public A unboundedFollowing() {
			return new UnboundedFollowingEnd<A>(this).getRequiredExpression();
		}

		@Override
		public Class<A> getRequiredExpressionClass() {
			return type.getRequiredExpressionClass();
		}

		@SuppressWarnings("unchecked")
		@Override
		public abstract FrameStart<A> copy();
	}

	private static class UnboundedPrecedingStart<A extends EqualExpression> extends FrameStart<A> {

		public UnboundedPrecedingStart(FrameType<A> type) {
			super(type);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return type.toSQLString(defn) + " UNBOUNDED PRECEDING ";
		}

		@Override
		public UnboundedPrecedingStart<A> copy() {
			return new UnboundedPrecedingStart<>(type.copy());
		}
	}

	private static class OffsetPrecedingStart<A extends EqualExpression> extends FrameStart<A> {

		public OffsetPrecedingStart(FrameType<A> type, int offset) {
			super(type, offset);
		}

		public OffsetPrecedingStart(FrameType<A> type, IntegerExpression offset) {
			super(type, offset);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return type.toSQLString(defn) + " " + offset.toSQLString(defn) + " PRECEDING ";
		}

		@Override
		public OffsetPrecedingStart<A> copy() {
			return new OffsetPrecedingStart<>(type.copy(), offset.copy());
		}
	}

	private static class CurrentRowStart<A extends EqualExpression> extends FrameStart<A> {

		public CurrentRowStart(FrameType<A> type) {
			super(type);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return type.toSQLString(defn) + " CURRENT ROW ";
		}

		@Override
		public CurrentRowStart<A> copy() {
			return new CurrentRowStart<>(type.copy());
		}
	}

	private static class OffsetFollowingStart<A extends EqualExpression> extends FrameStart<A> {

		public OffsetFollowingStart(FrameType<A> type, int offset) {
			super(type, offset);
		}

		public OffsetFollowingStart(FrameType<A> type, IntegerExpression offset) {
			super(type, offset);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return type.toSQLString(defn) + " " + offset.toSQLString(defn) + " FOLLOWING ";
		}

		@Override
		public OffsetFollowingStart<A> copy() {
			return new OffsetFollowingStart<>(type.copy(), offset.copy());
		}
	}

	private static class UnboundedFollowingStart<A extends EqualExpression> extends FrameStart<A> {

		public UnboundedFollowingStart(FrameType<A> type) {
			super(type);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return type.toSQLString(defn) + " UNBOUNDED FOLLOWING ";
		}

		@Override
		public UnboundedFollowingStart<A> copy() {
			return new UnboundedFollowingStart<>(type.copy());
		}
	}

	public static abstract class FrameEnd<A extends EqualExpression> implements WindowingFunctionInterface.WindowEnd<A>, AnyResult<A> {

		private final FrameStart<A> start;
		private final IntegerExpression offset;

		public FrameEnd(FrameStart<A> start) {
			this.start = start;
			this.offset = IntegerExpression.value(0);
		}

		public FrameEnd(FrameStart<A> start, int offset) {
			this.start = start;
			this.offset = IntegerExpression.value(offset);
		}

		public FrameEnd(FrameStart<A> start, IntegerExpression offset) {
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
				final Class<A> clazz = start.getRequiredExpressionClass();
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

		@Override
		public boolean isPurelyFunctional() {
			throw new UnsupportedOperationException("Not supported yet.");
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

	public static class UnboundedPrecedingEnd<A extends EqualExpression> extends FrameEnd<A> {

		public UnboundedPrecedingEnd(FrameStart<A> start) {
			super(start);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getStart().toSQLString(defn) + " AND UNBOUNDED PRECEDING )";
		}

		@Override
		public UnboundedPrecedingEnd<A> copy() {
			return new UnboundedPrecedingEnd<A>((FrameStart<A>) getStart().copy());
		}
	}

	public static class OffsetPrecedingEnd<A extends EqualExpression> extends FrameEnd<A> {

		public OffsetPrecedingEnd(FrameStart<A> aThis, int offset) {
			super(aThis, offset);
		}

		public OffsetPrecedingEnd(FrameStart<A> aThis, IntegerExpression offset) {
			super(aThis, offset);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getStart().toSQLString(defn) + " AND " + getOffset().toSQLString(defn) + " PRECEDING )";
		}

		@Override
		public OffsetPrecedingEnd<A> copy() {
			return new OffsetPrecedingEnd<A>((FrameStart<A>) getStart().copy(), getOffset().copy());
		}
	}

	public static class CurrentRowEnd<A extends EqualExpression> extends FrameEnd<A> {

		public CurrentRowEnd(FrameStart<A> start) {
			super(start);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getStart().toSQLString(defn) + " AND CURRENT ROW )";
		}

		@Override
		public CurrentRowEnd<A> copy() {
			return new CurrentRowEnd<A>((FrameStart<A>) getStart().copy());
		}
	}

	private static class OffsetFollowingEnd<A extends EqualExpression> extends FrameEnd<A> {

		public OffsetFollowingEnd(FrameStart<A> aThis, int offset) {
			super(aThis, offset);
		}

		public OffsetFollowingEnd(FrameStart<A> aThis, IntegerExpression offset) {
			super(aThis, offset);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getStart().toSQLString(defn) + " AND " + getOffset().toSQLString(defn) + " FOLLOWING) ";
		}

		@Override
		public OffsetFollowingEnd<A> copy() {
			return new OffsetFollowingEnd<A>((FrameStart<A>) getStart().copy(), getOffset().copy());
		}
	}

	public static class UnboundedFollowingEnd<A extends EqualExpression> extends FrameEnd<A> {

		public UnboundedFollowingEnd(FrameStart<A> aThis) {
			super(aThis);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getStart().toSQLString(defn) + " AND UNBOUNDED FOLLOWING )";
		}

		@Override
		public UnboundedFollowingEnd<A> copy() {
			return new UnboundedFollowingEnd<A>((FrameStart<A>) getStart().copy());
		}
	}

	public static abstract class WindowEnd<A extends EqualExpression> implements WindowingFunctionInterface.WindowEnd<A>, AnyResult<A> {

		private final WindowPart<A> start;

		public WindowEnd(WindowPart<A> start) {
			this.start = start;
		}

		/**
		 * @return the and
		 */
		protected WindowPart<A> getStart() {
			return start;
		}

		@Override
		public A getRequiredExpression() {
			try {
				final Class<A> clazz = start.getRequiredExpressionClass();
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

		@Override
		public boolean isPurelyFunctional() {
			throw new UnsupportedOperationException("Not supported yet.");
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
}
