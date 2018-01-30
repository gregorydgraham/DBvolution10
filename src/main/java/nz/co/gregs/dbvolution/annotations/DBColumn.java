/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * Used to indicate that this field is associated with a database column and the
 * name of that column.
 * <code>
 * <br>
 * &#64;DBColumn("my_column")<br>
 * public DBString myColumn = new DBString();<br>
 * <br>
 * </code>
 * <p>
 * DBColumn allows you to change the field name without affecting database
 * functionality and is highly recommended.</p>
 *
 * <p>
 * Using a QueryableDatatype is sufficient to indicate that the field is
 * associated with a column, however this causes the class API to be tightly
 * bound to the database and subtracts from the benefits of DBvolution.</p>
 *
 * <p>
 * DBColumn is generated automatically by DBTableClassGenerator.</p>
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see DBForeignKey
 * @see DBPrimaryKey
 * @see DBAutoIncrement
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBColumn{

	/**
	 * The raw column name as stored in the database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the raw column name from the database
	 */
	String value() default "";

}
