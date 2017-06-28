/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.results;

import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * @author greg
 */
public interface Point3DResult extends 
		DBExpression, 
		ExpressionCanHaveNullValues, 
		Spatial3DResult
{
	
}
