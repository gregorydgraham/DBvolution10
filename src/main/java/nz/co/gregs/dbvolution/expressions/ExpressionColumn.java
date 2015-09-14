/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.expressions;

/**
 * Provides simple access to the expression column version of an expression
 *
 * @author gregorygraham
 * @param <T> the type that should be used with this expression
 */
public interface ExpressionColumn<T> {

	/**
	 * Creates a QueryableDatatype version of the expression suitable for use as a column.
	 *
	 * @return a QDT version of the expression
	 */
	public T asExpressionColumn();

}
