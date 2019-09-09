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

import nz.co.gregs.dbvolution.expressions.EqualExpression;

/**
 * @author gregorygraham
 * @param <A> the expression type returned by this windowing function, e.g.
 * IntegerExpression
 */
public interface CanBeWindowingFunctionWithFrame<A extends EqualExpression<?, ?, ?>> {

	/**
	 * Over creates a window function.
	 *
	 * <p>
	 * Window functions allow access to data in the records right before and after
	 * the current record. A window function defines a frame or window of rows
	 * with a given length around the current row, and performs a calculation
	 * across the set of data in the window. Wikipedia has more information at
	 * https://en.wikipedia.org/wiki/SQL_window_function</p>
	 *
	 *
	 *
	 * @return a windowing function start point
	 */
	public WindowFunctionFramable<A> over();

	/**
	 * Synonym for over.
	 *
	 * <p>
	 * Check {@link #over() } for details</p>
	 *
	 * @return a windowing function start point
	 */
	public default WindowFunctionFramable<A> window() {
		return over();
	}

}
