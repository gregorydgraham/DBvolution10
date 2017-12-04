/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.exceptions;

import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.internal.properties.JavaProperty;
import nz.co.gregs.dbvolution.internal.properties.RowDefinitionClassWrapper;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class ReferenceToUndefinedPrimaryKeyException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Please use another Exception as this one is too generic.
	 *
	 * @param message	message
	 */
	public ReferenceToUndefinedPrimaryKeyException(String message) {
		super(message);
	}

	/**
	 *
	 *
	 * @param adaptee
	 * @param referencedClassWrapper
	 */
	public ReferenceToUndefinedPrimaryKeyException(JavaProperty adaptee, RowDefinitionClassWrapper referencedClassWrapper) {
		super("Property " + adaptee.qualifiedName() + " references class " + referencedClassWrapper.javaName()
				+ ", which does not have a primary key. Please identify the primary key on that class or specify the column in the"
				+ " @" + DBForeignKey.class.getSimpleName() + " declaration.");
	}
}
