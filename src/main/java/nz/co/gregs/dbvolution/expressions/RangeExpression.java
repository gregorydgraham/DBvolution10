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

import nz.co.gregs.dbvolution.results.RangeResult;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.RangeComparable;

/**
 *
 * @author gregorygraham
 * @param <B> a base type like Number, String, or Date
 * @param <R> some RangeResult type like NumberResult that returns type B
 * @param <D> some QDT that works with type B
 *
 */
public abstract class RangeExpression<B, R extends RangeResult<B>, D extends QueryableDatatype<B>> extends InExpression<B, R, D> implements RangeComparable<B, R> {

//	@Override
//	public abstract R getInnerResult() ;
//
//	@Override
//	abstract public R expression(B value);
//
//	@Override
//	abstract public R expression(R value);
//
//	@Override
//	abstract public R expression(D value);
//
//	@Override
//	abstract public BooleanExpression is(R value);
//	
//	@Override
//	abstract public BooleanExpression isNot(R value);
//
//	@Override
//	abstract public BooleanExpression isIn(R... value);

//	abstract public BooleanExpression isLessThan(R value);
//
//	public abstract BooleanExpression isGreaterThan(R value);
//
//	public abstract BooleanExpression isLessThanOrEqual(R value);
//
//	public abstract BooleanExpression isGreaterThanOrEqual(R value);
//
//	public abstract BooleanExpression isLessThan(R value, BooleanExpression fallBackWhenEquals);
//
//	public abstract BooleanExpression isGreaterThan(R value, BooleanExpression fallBackWhenEquals);
//
//	public abstract BooleanExpression isBetween(R lowerBound, R upperBound);
//
//	public abstract BooleanExpression isBetweenInclusive(R lowerBound, R upperBound);
//
//	public abstract BooleanExpression isBetweenExclusive(R lowerBound, R upperBound);

	/* Default implementations*/
	@Override
	public BooleanExpression isLessThan(B value) {
		return isLessThan(this.expression(value));
	}

	public BooleanExpression isLessThan(D value) {
		return isLessThan(this.expression(value));
	}

	@Override
	public BooleanExpression isGreaterThan(B value) {
		return isGreaterThan(this.expression(value));
	}

	public BooleanExpression isGreaterThan(D value) {
		return isGreaterThan(this.expression(value));
	}

	@Override
	public BooleanExpression isLessThanOrEqual(B value) {
		return isLessThanOrEqual(this.expression(value));
	}

	public BooleanExpression isLessThanOrEqual(D value) {
		return isLessThanOrEqual(this.expression(value));
	}

	@Override
	public BooleanExpression isGreaterThanOrEqual(B value) {
		return isGreaterThanOrEqual(this.expression(value));
	}

	public BooleanExpression isGreaterThanOrEqual(D value) {
		return isGreaterThanOrEqual(this.expression(value));
	}

	@Override
	public BooleanExpression isLessThan(B value, BooleanExpression fallBackWhenEquals) {
		return isLessThan(this.expression(value), fallBackWhenEquals);
	}

	public BooleanExpression isLessThan(D value, BooleanExpression fallBackWhenEquals) {
		return isLessThan(this.expression(value), fallBackWhenEquals);
	}

	@Override
	public BooleanExpression isGreaterThan(B value, BooleanExpression fallBackWhenEquals) {
		return isGreaterThan(this.expression(value), fallBackWhenEquals);
	}

	public BooleanExpression isGreaterThan(D value, BooleanExpression fallBackWhenEquals) {
		return isGreaterThan(this.expression(value), fallBackWhenEquals);
	}

	@Override
	public BooleanExpression isBetween(R lowerBound, B upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetween(B lowerBound, R upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(R lowerBound, D upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(D lowerBound, R upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetween(B lowerBound, B upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(D lowerBound, D upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(B lowerBound, D upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetween(D lowerBound, B upperBound) {
		return this.isBetween(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenInclusive(R lowerBound, B upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenInclusive(B lowerBound, R upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(R lowerBound, D upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(D lowerBound, R upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenInclusive(B lowerBound, B upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(D lowerBound, D upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(B lowerBound, D upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenInclusive(D lowerBound, B upperBound) {
		return this.isBetweenInclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenExclusive(R lowerBound, B upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenExclusive(B lowerBound, R upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(R lowerBound, D upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(D lowerBound, R upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	@Override
	public BooleanExpression isBetweenExclusive(B lowerBound, B upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(D lowerBound, D upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(B lowerBound, D upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

	public BooleanExpression isBetweenExclusive(D lowerBound, B upperBound) {
		return this.isBetweenExclusive(this.expression(lowerBound), this.expression(upperBound));
	}

}
