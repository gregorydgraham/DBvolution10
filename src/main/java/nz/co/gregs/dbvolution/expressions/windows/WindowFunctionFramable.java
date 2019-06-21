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
 * @param <A> the expression type returned by this windowing function, e.g. IntegerExpression
 */
public class WindowFunctionFramable<A extends EqualExpression<?, ?, ?>> implements WindowingFunctionFramableInterface<A> {

	private final A innerExpression;

	public WindowFunctionFramable(A expression) {
		super();
		this.innerExpression = expression;
	}

	@Override
	public Partitioned<A> partition(ColumnProvider... cols) {
		return new WindowFunctionFramable.Partitioned<A>(this, cols);
	}

	public A allRows() {
		return this.partition().unsorted();
	}

	public A getInnerExpression() {
		return innerExpression;
	}

	public A allRowsPreceding() {
		return this.partition().unsortedWithFrame().rows().unboundedPrecedingAndCurrentRow();
	}

	public A AllRowsAndOrderBy(SortProvider... sorts) {
		if (sorts.length > 0) {
			if (sorts.length > 1) {
				SortProvider sort = sorts[0];
				SortProvider[] newSorts = new SortProvider[sorts.length - 1];
				System.arraycopy(sorts, 1, newSorts, 0, sorts.length - 1);
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
	@Override
	public WindowFunctionFramable<A> copy() {
		return new WindowFunctionFramable<A>((A) this.innerExpression.copy());
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

	public static class Partitioned<A extends EqualExpression<?, ?, ?>> implements WindowingFunctionFramableInterface.Partitioned<A> {

		private final WindowFunctionFramable<A> innerExpression;
		private final ColumnProvider[] columns;

		private Partitioned(WindowFunctionFramable<A> expression, ColumnProvider... cols) {
			super();
			this.innerExpression = expression;
			this.columns = cols;
		}

		@Override
		public WindowFunctionFramable.Sorted<A> orderBy(SortProvider sort, SortProvider... sorts) {
			SortProvider[] newSorts = new SortProvider[sorts.length + 1];
			newSorts[0] = sort;
			System.arraycopy(sorts, 0, newSorts, 1, sorts.length);
			return new WindowFunctionFramable.Sorted<A>(this, newSorts);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			StringBuilder partitionClause = new StringBuilder();
			if (getColumns().length > 0) {
				partitionClause.append("PARTITION BY ");
				String separator = "";
				for (ColumnProvider partitionByColumn : getColumns()) {
					partitionClause.append(separator).append(partitionByColumn.toSQLString(defn));
					separator = ", ";
				}
			}
			return getInnerExpression().toSQLString(defn) + partitionClause;
		}

		@Override
		public Class<A> getRequiredExpressionClass() {
			return getInnerExpression().getRequiredExpressionClass();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Partitioned<A> copy() {
			return new Partitioned<A>(this.getInnerExpression().copy(), this.getColumns());
		}

		@Override
		@SuppressWarnings("unchecked")
		public A unsorted() {
			final Sorted<A> orderBy = new UnSorted<A>(this);
			return orderBy.withoutFrame();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Sorted<A> unsortedWithFrame() {
			return this.orderBy(BooleanExpression.trueExpression().ascending());
		}

		@Override
		@SuppressWarnings("unchecked")
		public A unordered() {
			return unsorted();
		}

		@Override
		@SuppressWarnings("unchecked")
		public Sorted<A> unorderedWithFrame() {
			return unsortedWithFrame();
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
			boolean functional = getInnerExpression().isPurelyFunctional();
			if (functional == true) {
				for (ColumnProvider column : getColumns()) {
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
		public Sorted<A> orderByWithPrimaryKeys(SortProvider... partitionFields) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Sorted<A> orderByWithPrimaryKeys(ColumnProvider... partitionFields) {
			return orderByWithPrimaryKeys(SortProvider.getSortProviders(partitionFields));
		}

		/**
		 * @return the innerExpression
		 */
		protected WindowFunctionFramable<A> getInnerExpression() {
			return innerExpression;
		}

		/**
		 * @return the columns
		 */
		protected ColumnProvider[] getColumns() {
			return columns;
		}

		private class UnSorted<A extends EqualExpression<?, ?, ?>> extends Sorted<A> {

			public UnSorted(Partitioned<A> expression) {
				super(expression, BooleanExpression.trueExpression().ascending());
			}

			@Override
			public String toSQLString(DBDefinition defn) {
				if (defn.supportsComparingBooleanResults()) {
					return super.toSQLString(defn);
				} else {
					return getInnerExpression().toSQLString(defn);
				}
			}
		}
	}

	public static class Sorted<A extends EqualExpression<?, ?, ?>> implements WindowingFunctionFramableInterface.Sorted<A> {

		private final WindowFunctionFramable.Partitioned<A> innerExpression;
		private final SortProvider[] sorts;

		public Sorted(WindowFunctionFramable.Partitioned<A> expression, SortProvider... sorts) {
			super();
			this.innerExpression = expression;
			this.sorts = sorts;
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			StringBuilder orderByClause = new StringBuilder();
			if (getSorts().length > 0) {
				orderByClause.append(" ORDER BY ");
				String separator = "";
				for (SortProvider partitionByColumn : getSorts()) {
					orderByClause.append(separator).append(partitionByColumn.toSQLString(defn));
					separator = ", ";
				}
			}
			return getInnerExpression().toSQLString(defn) + orderByClause;
		}

		public A defaultFrame() {
			return this.rows().unboundedPreceding().unboundedFollowing();
		}

		@Override
		public A withoutFrame() {
			return new EmptyFrameEnd<>(this).getRequiredExpression();
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
			return getInnerExpression().getRequiredExpressionClass();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Sorted<A> copy() {
			return new Sorted<A>(this.getInnerExpression().copy(), this.getSorts());
		}

		@Override
		public boolean isPurelyFunctional() {
			boolean functional = getInnerExpression().isPurelyFunctional();
			if (functional == true) {
				for (SortProvider sort : getSorts()) {
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
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			throw new UnsupportedOperationException("Not supported yet.");
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

		/**
		 * @return the innerExpression
		 */
		protected WindowFunctionFramable.Partitioned<A> getInnerExpression() {
			return innerExpression;
		}

		/**
		 * @return the sorts
		 */
		protected SortProvider[] getSorts() {
			return sorts;
		}

	}

	public static abstract class FrameType<A extends EqualExpression<?, ?, ?>> implements WindowingFunctionFramableInterface.FrameType<A> {

		private final Sorted<A> sorted;

		public FrameType(Sorted<A> sorted) {
			super();
			this.sorted = sorted;
		}

		protected Sorted<A> getSorted() {
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
		public UnboundedPrecedingStart<A> unboundedPreceding() {
			return new UnboundedPrecedingStart<A>(this);
		}

		@Override
		public OffsetPrecedingStart<A> preceding(int offset) {
			return new OffsetPrecedingStart<A>(this, offset);
		}

		@Override
		public OffsetPrecedingStart<A> preceding(IntegerExpression offset) {
			return new OffsetPrecedingStart<A>(this, offset);
		}

		@Override
		public CurrentRowStart<A> currentRow() {
			return new CurrentRowStart<A>(this);
		}

		@Override
		public OffsetFollowingStart<A> following(int offset) {
			return new OffsetFollowingStart<A>(this, offset);
		}

		@Override
		public OffsetFollowingStart<A> following(IntegerExpression offset) {
			return new OffsetFollowingStart<A>(this, offset);
		}

		@Override
		public Class<A> getRequiredExpressionClass() {
			return sorted.getRequiredExpressionClass();
		}

		@Override
		public abstract FrameType<A> copy();

		@Override
		public final boolean isPurelyFunctional() {
			return getSorted().isPurelyFunctional();
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
			return true;
		}
	}

	public static class Range<A extends EqualExpression<?, ?, ?>> extends FrameType<A> {

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

	public static class Rows<A extends EqualExpression<?, ?, ?>> extends FrameType<A> {

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

	public static class Groups<A extends EqualExpression<?, ?, ?>> extends FrameType<A> {

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

	public static abstract class FrameStart<A extends EqualExpression<?, ?, ?>> implements WindowingFunctionFramableInterface.EmptyFrameStart<A> {

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

		public FrameStart(FrameType<A> type, long offset) {
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
		public Class<A> getRequiredExpressionClass() {
			return type.getRequiredExpressionClass();
		}

		@SuppressWarnings("unchecked")
		@Override
		public abstract FrameStart<A> copy();

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
	}

	private static abstract class FrameStartAbsolute<A extends EqualExpression<?, ?, ?>> extends FrameStart<A> {

		public FrameStartAbsolute(FrameType<A> type) {
			super(type);
		}

		@Override
		public final boolean isPurelyFunctional() {
			return type.isPurelyFunctional();
		}
	}

	private static abstract class FrameStartOffset<A extends EqualExpression<?, ?, ?>> extends FrameStart<A> {

		public FrameStartOffset(FrameType<A> type, int offset) {
			super(type, offset);
		}

		public FrameStartOffset(FrameType<A> type, long offset) {
			super(type, offset);
		}

		public FrameStartOffset(FrameType<A> type, IntegerExpression offset) {
			super(type, offset);
		}

		public FrameStartOffset(FrameType<A> type) {
			super(type);
		}

		@Override
		public final boolean isPurelyFunctional() {
			return type.isPurelyFunctional() && offset.isPurelyFunctional();
		}
	}

	public static class UnboundedPrecedingStart<A extends EqualExpression<?, ?, ?>> extends FrameStartAbsolute<A> implements FrameStartAllPreceding<A> {

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

	}

	public static class OffsetPrecedingStart<A extends EqualExpression<?, ?, ?>> extends FrameStartOffset<A> implements FrameStartPreceding<A> {

		public OffsetPrecedingStart(FrameType<A> type, int offset) {
			super(type, offset);
		}

		public OffsetPrecedingStart(FrameType<A> type, long offset) {
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
		public A preceding(int offset) {
			return new OffsetPrecedingEnd<A>(this, offset).getRequiredExpression();
		}

		@Override
		public A preceding(IntegerExpression offset) {
			return new OffsetPrecedingEnd<A>(this, offset).getRequiredExpression();
		}

		@Override
		public OffsetPrecedingStart<A> copy() {
			return new OffsetPrecedingStart<>(type.copy(), offset.copy());
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

	}

	public static class CurrentRowStart<A extends EqualExpression<?, ?, ?>> extends FrameStartAbsolute<A> implements FrameStartCurrentRow<A> {

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

	}

	public static class OffsetFollowingStart<A extends EqualExpression<?, ?, ?>> extends FrameStartOffset<A> implements FrameStartFollowing<A> {

		public OffsetFollowingStart(FrameType<A> type, int offset) {
			super(type, offset);
		}

		public OffsetFollowingStart(FrameType<A> type, long offset) {
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
	}

	public static abstract class FrameEnd<A extends EqualExpression<?, ?, ?>> implements WindowingFunctionFramableInterface.WindowEnd<A>, AnyResult<A> {

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
				Logger.getLogger(WindowFunctionFramable.class.getName()).log(Level.SEVERE, null, ex);
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

	public static class UnboundedPrecedingEnd<A extends EqualExpression<?, ?, ?>> extends FrameEnd<A> {

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

		@Override
		public boolean isPurelyFunctional() {
			return getStart().isPurelyFunctional();
		}
	}

	public static class OffsetPrecedingEnd<A extends EqualExpression<?, ?, ?>> extends FrameEnd<A> {

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

		@Override
		public boolean isPurelyFunctional() {
			return getStart().isPurelyFunctional() && getOffset().isPurelyFunctional();
		}
	}

	public static class CurrentRowEnd<A extends EqualExpression<?, ?, ?>> extends FrameEnd<A> {

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

		@Override
		public boolean isPurelyFunctional() {
			return getStart().isPurelyFunctional();
		}
	}

	private static class OffsetFollowingEnd<A extends EqualExpression<?, ?, ?>> extends FrameEnd<A> {

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

		@Override
		public boolean isPurelyFunctional() {
			return getStart().isPurelyFunctional() && getOffset().isPurelyFunctional();
		}
	}

	public static class UnboundedFollowingEnd<A extends EqualExpression<?, ?, ?>> extends FrameEnd<A> {

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

		@Override
		public boolean isPurelyFunctional() {
			return getStart().isPurelyFunctional();
		}
	}

	public static class EmptyFrameEnd<A extends EqualExpression<?, ?, ?>> extends FrameEnd<A> {

		public EmptyFrameEnd(WindowPart<A> aThis) {
			super(aThis);
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return getStart().toSQLString(defn) + " )";
		}

		@Override
		public EmptyFrameEnd<A> copy() {
			return new EmptyFrameEnd<A>(getStart().copy());
		}

		@Override
		public boolean isPurelyFunctional() {
			return getStart().isPurelyFunctional();
		}
	}
}
