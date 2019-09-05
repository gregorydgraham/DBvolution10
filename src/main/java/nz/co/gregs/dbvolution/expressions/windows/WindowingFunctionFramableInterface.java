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
 *     Under the forFollowing terms:
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

import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.EqualExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;

/**
 *
 * @author gregorygraham
 * @param <A> the Expression type produced by this windowing function, e.g. IntegerExpression
 */
public interface WindowingFunctionFramableInterface<A extends EqualExpression<?,?,?>> extends DBExpression {

	Partitioned<A> partition(EqualExpression... cols);

	Partitioned<A> unpartitioned();

	Class<A> getRequiredExpressionClass();

	public interface WindowPart<A extends EqualExpression<?,?,?>> extends DBExpression {

		Class<A> getRequiredExpressionClass();

		@Override
		abstract WindowPart<A> copy();
	}

	public interface Partitioned<A extends EqualExpression<?,?,?>> extends WindowPart<A> {

		A unsorted();

		/**
		 * A synonym for {@link #unsorted() }
		 *
		 * @return an expression
		 */
		A unordered();

		Sorted<A> orderBy(SortProvider... sorts);

		Sorted<A> unsortedWithFrame();

		/**
		 * Orders by the provided sort providers and adds the required primary keys
		 * as well.
		 *
		 * @param sorts the columns to sort by
		 * @return an expression
		 */
		Sorted<A> orderByWithPrimaryKeys(SortProvider... sorts);

		/**
		 * Orders by the provided sort providers and adds the required primary keys
		 * as well.
		 * 
		 * <p>The default ordering will be used for all columns provided.</p>
		 *
		 * @param sorts the columns to sort by
		 * @return an expression
		 */
		Sorted<A> orderByWithPrimaryKeys(ColumnProvider... sorts);

		/**
		 * A synonym for {@link #unsortedWithFrame() }
		 *
		 * @return an expression
		 */
		Sorted<A> unorderedWithFrame();
	}

	public interface Sorted<A extends EqualExpression<?,?,?>> extends WindowPart<A> {

		FrameType<A> rows();

		/* MS SQL Server 2017 does not support groups*/
//		FrameType<A> groups();
		FrameType<A> range();

		A withoutFrame();
	}

	public interface FrameType<A extends EqualExpression<?,?,?>> extends WindowPart<A> {

		FrameStartAllPreceding<A> unboundedPreceding();

		FrameStartPreceding<A> preceding(int offset);

		FrameStartPreceding<A> preceding(IntegerExpression offset);

		FrameStartCurrentRow<A> currentRow();

		FrameStartFollowing<A> forFollowing(int offset);

		FrameStartFollowing<A> forFollowing(IntegerExpression offset);
	}

	public interface EmptyFrameStart<A extends EqualExpression<?,?,?>> extends WindowPart<A> {

	}

	public interface FrameStart<A extends EqualExpression<?,?,?>> extends EmptyFrameStart<A> {

		A forFollowing(int offset);

		A forFollowing(IntegerExpression offset);

		A unboundedFollowing();
	}

	public interface FrameStartAllPreceding<A extends EqualExpression<?,?,?>> extends FrameStart<A> {

		A preceding(int offset);

		A preceding(IntegerExpression offset);

		A currentRow();

	}

	public interface FrameStartPreceding<A extends EqualExpression<?,?,?>> extends FrameStart<A> {

		A preceding(int offset);

		A preceding(IntegerExpression offset);

		A currentRow();

	}

	public interface FrameStartCurrentRow<A extends EqualExpression<?,?,?>> extends FrameStart<A> {

		A toCurrentRow();

	}

	public interface FrameStartFollowing<A extends EqualExpression<?,?,?>> extends FrameStart<A> {

	}

	public interface WindowEnd<A extends EqualExpression<?,?,?>> extends DBExpression {

		A build();
	}
}
