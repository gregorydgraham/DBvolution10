/*
 * Copyright 2020 Gregory Graham.
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

import nz.co.gregs.dbvolution.datatypes.DBLargeBinary;
import nz.co.gregs.dbvolution.results.LargeBinaryResult;

/**
 *
 * @author gregorygraham
 */
public class LargeBinaryExpression extends LargeObjectExpression<byte[], LargeBinaryResult, DBLargeBinary> implements LargeBinaryResult, ExpressionColumn<DBLargeBinary>{

	private static final long serialVersionUID = 1L;

	public LargeBinaryExpression() {
		super();
	}

	public LargeBinaryExpression(LargeBinaryResult originalBlob) {
		super(originalBlob);
	}

	public LargeBinaryExpression(DBLargeBinary originalBlob) {
		super(originalBlob);
	}

	@Override
	public LargeBinaryResult expression(byte[] value) {
		return new LargeBinaryExpression(new DBLargeBinary(value));
	}

	@Override
	public LargeBinaryResult expression(LargeBinaryResult value) {
		return new LargeBinaryExpression(value);
	}

	@Override
	public LargeBinaryResult expression(DBLargeBinary value) {
		return new LargeBinaryExpression(value);
	}

	@Override
	public DBLargeBinary asExpressionColumn() {
		return new DBLargeBinary(this);
	}

	@Override
	@SuppressWarnings("unchecked")
	public LargeBinaryExpression copy() {
		return new LargeBinaryExpression((LargeBinaryResult) getInnerResult());
	}

	@Override
	@SuppressWarnings("unchecked")
	public DBLargeBinary toExpressionColumn() {
		return new DBLargeBinary((LargeBinaryResult) getInnerResult());
	}

	@Override
	public DBLargeBinary getQueryableDatatypeForExpressionValue() {
		return new DBLargeBinary();
	}
	
}
