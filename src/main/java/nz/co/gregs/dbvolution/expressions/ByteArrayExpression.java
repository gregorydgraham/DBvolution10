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

import nz.co.gregs.dbvolution.datatypes.DBByteArray;
import nz.co.gregs.dbvolution.results.ByteArrayResult;

/**
 *
 * @author gregorygraham
 */
@SuppressWarnings("deprecation")
public class ByteArrayExpression extends LargeObjectExpression<byte[], ByteArrayResult, DBByteArray> implements ByteArrayResult, ExpressionColumn<DBByteArray>{

	private static final long serialVersionUID = 1L;

	protected ByteArrayExpression() {
		super();
	}
	@SuppressWarnings("deprecation")
	public ByteArrayExpression(byte[] value) {
		super(new DBByteArray(value));
	}
	
	public ByteArrayExpression(ByteArrayResult originalBlob) {
		super(originalBlob);
	}	
	
	public ByteArrayExpression(DBByteArray originalBlob) {
		super(originalBlob);
	}	

	@Override
	public ByteArrayExpression expression(byte[] value) {
		return new ByteArrayExpression(value);
	}

	public ByteArrayExpression expression(ByteArrayResult value) {
		return new ByteArrayExpression(value);
	}

	@Override
	public ByteArrayExpression expression(DBByteArray value) {
		return new ByteArrayExpression(value);
	}

	@Override
	public DBByteArray asExpressionColumn() {
		return new DBByteArray(this);
	}

	@Override
	public ByteArrayResult copy() {
		return new ByteArrayExpression((ByteArrayResult) getInnerResult());
	}

	@Override
	public DBByteArray getQueryableDatatypeForExpressionValue() {
		return new DBByteArray();
	}

}
