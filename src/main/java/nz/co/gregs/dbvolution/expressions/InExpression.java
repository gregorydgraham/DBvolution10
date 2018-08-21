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
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.InResult;

/**
 * A value class that covers most datatypes.
 *
 * <p>
 * This class brings together the related classes used to work with database
 * values: the base type, the result type, and the QueryableDatatype.<p>
 *
 * <p>
 * </p>
 *
 * @author gregorygraham
 * @param <B> a base type similar to Integer or String that is ultimately what
 * you want to work with.
 * @param <R> the *Result type that will allow you to make expressions for
 * working with the base type.
 * @param <D> the QDT that will store and retrieve the base from the database.
 */
public abstract class InExpression<B, R extends InResult<B>, D extends QueryableDatatype<B>> extends EqualExpression<B, R, D> {

	private static final long serialVersionUID = 1L;
	/**
	 *
	 * @param only
	 */
	protected InExpression(R only) {
		super(only);
	}

	protected InExpression() {
		super();
	}
	/**
	 *
	 * @param only
	 */
	protected InExpression(AnyResult<?> only) {
		super(only);
	}

	abstract public BooleanExpression isIn(R... value);

	@SuppressWarnings("unchecked")
	public BooleanExpression isIn(B... possibleValues) {
		List<R> exps = new ArrayList<R>(0);
		for (B possibleValue : possibleValues) {
			final R expression = this.expression(possibleValue);
			exps.add(expression);
		}
		return this.isIn((R[]) exps.toArray());
	}

	@SuppressWarnings("unchecked")
	public BooleanExpression isIn(D... possibleValues) {
		List<R> exps = new ArrayList<R>(0);
		for (D possibleValue : possibleValues) {
			final R expression = this.expression(possibleValue);
			exps.add(expression);
		}
		return this.isIn((R[]) exps.toArray());
	}
}
