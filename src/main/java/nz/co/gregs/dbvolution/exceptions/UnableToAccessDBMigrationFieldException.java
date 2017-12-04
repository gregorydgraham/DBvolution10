/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.exceptions;

import java.lang.reflect.Field;

/**
 * Thrown when DBvolution is unable to access a field it needs.
 *
 * <p>
 * A lot of reflection is used in DBV, please ensure that the fields are
 * publicly accessible and non-null.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class UnableToAccessDBMigrationFieldException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when DBvolution is unable to access a field it needs.
	 *
	 * <p>
	 * A lot of reflection is used in DBV, please ensure that the fields are
	 * publicly accessible and non-null.
	 *
	 * @param target
	 * @param field field
	 * @param ex ex
	 */
	public UnableToAccessDBMigrationFieldException(Object target, Field field, Exception ex) {
		super("Unable To Access DBMigration Field: please ensure that all fields on " + target.getClass().getSimpleName() + " are Public and Non-Null: Especially field: " + field.getName(), ex);
	}

	/**
	 * Thrown when DBvolution is unable to access a field it needs.
	 *
	 * <p>
	 * A lot of reflection is used in DBV, please ensure that the fields are
	 * publicly accessible and non-null.
	 *
	 * @param target aMapping
	 * @param ex ex
	 */
	public UnableToAccessDBMigrationFieldException(Object target, Exception ex) {
		super("Unable To Access DBMigration Field: please ensure that all fields on " + target.getClass().getSimpleName() + " are Public and Non-Null.", ex);
	}

}
