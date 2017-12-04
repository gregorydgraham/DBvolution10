/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.expressions;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Provides simple access to the expression column version of an expression
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 * @param <T> the type that should be used with this expression
 */
public interface ExpressionColumn<T extends QueryableDatatype<?>> {

	/**
	 * Creates a QueryableDatatype version of the expression suitable for use as a
	 * column.
	 *
	 * <p>
	 * For example: 	 <code>@DBColumn public DBString title =
	 * person.column(person.fullname).substringBefore("
	 * ").asExpressionColumn();</code>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a QDT version of the expression
	 */
	public T asExpressionColumn();

}
