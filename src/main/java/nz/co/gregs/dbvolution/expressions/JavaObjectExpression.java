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

import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.results.JavaObjectResult;

/**
 *
 * @author gregorygraham
 * @param <OBJECT> the java object returned by this expression
 */
public class JavaObjectExpression<OBJECT>
		extends LargeObjectExpression<OBJECT, JavaObjectResult<OBJECT>, DBJavaObject<OBJECT>>
		implements JavaObjectResult<OBJECT>, ExpressionColumn<DBJavaObject<OBJECT>> {

	private static final long serialVersionUID = 1L;

	public JavaObjectExpression() {
		super();
	}

	public JavaObjectExpression(JavaObjectResult<OBJECT> originalBlob) {
		super(originalBlob);
	}

	private JavaObjectExpression(DBJavaObject<OBJECT> value) {
		super(value);
	}

	@Override
	public JavaObjectResult<OBJECT> expression(OBJECT value) {
		return new JavaObjectExpression<OBJECT>(new DBJavaObject<OBJECT>(value));
	}

	@Override
	public JavaObjectResult<OBJECT> expression(JavaObjectResult<OBJECT> value) {
		return new JavaObjectExpression<>(value);
	}

	@Override
	public JavaObjectResult<OBJECT> expression(DBJavaObject<OBJECT> value) {
		return new JavaObjectExpression<OBJECT>(value);
	}

	@Override
	public DBJavaObject<OBJECT> asExpressionColumn() {
		return new DBJavaObject<>(this);
	}

	@Override
	@SuppressWarnings("unchecked")
	public JavaObjectExpression<OBJECT> copy() {
		return new JavaObjectExpression<OBJECT>((JavaObjectResult<OBJECT>) getInnerResult());
	}

	@Override
	@SuppressWarnings("unchecked")
	public DBJavaObject<OBJECT> toExpressionColumn() {
		return new DBJavaObject<OBJECT>((JavaObjectResult<OBJECT>) getInnerResult());
	}

	@Override
	public DBJavaObject<OBJECT> getQueryableDatatypeForExpressionValue() {
		return new DBJavaObject<OBJECT>();
	}

}
