/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.expressions;

/**
 * Provides simple access to the column expression version of an expression
 *
 * @author gregorygraham
 */
interface ColumnExpression<T> {

		/**
	 * Creates a QueryableDatatype version of the expression suitable for use as a expression column.
	 *
	 * @return a QDT version of the expression
	 */
	public T asColumnExpression();

}
