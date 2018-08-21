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

import nz.co.gregs.dbvolution.results.SimpleNumericResult;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 *
 * Used to group NumberExpression and IntegerExpression.
 *
 * Simple in this sense means that imaginary numbers are probably not handled.
 *
 * @author gregorygraham
 * @param <B> a base numeric type, that is Number or Integer or similar
 * @param <R> a RangeResult&lt;B&gt;
 * @param <D> a QueryableDatatype&lt;B&gt;
 */
public abstract class SimpleNumericExpression<B, R extends SimpleNumericResult<B>, D extends QueryableDatatype<B>> extends RangeExpression<B, R, D> implements SimpleNumericResult<B>{

	private static final long serialVersionUID = 1L;
	public abstract NumberExpression numberResult();
	public abstract IntegerExpression integerResult();
	
	/**
	 *
	 * @param only
	 */
	protected SimpleNumericExpression(R only) {
		super(only);
	}

	protected SimpleNumericExpression() {
		super();
	}
	/**
	 *
	 * @param only
	 */
	protected SimpleNumericExpression(AnyResult<?> only) {
		super(only);
	}
}
