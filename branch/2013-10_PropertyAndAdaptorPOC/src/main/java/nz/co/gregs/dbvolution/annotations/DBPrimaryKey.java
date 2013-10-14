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
 * DBPrimaryKey indicates that the field is the primary key for the table
 *
 * @DBPrimaryKey
 * @DBColumn("my_table_id")
 * public DBInteger myPrimaryKey = new DBInteger();
 * 
 * DBPrimaryKey is used extensively within DBvolution and should be specified in most tables, many-to-many link tables being a notable exception.
 * 
 * DBPrimaryKey works with DBForeignKey and DBQuery to make Natural Joins happen automatically.
 * 
 * DBPrimaryKey does not require a primary key relationship to exist within the database and does not enforce referential integrity.
 * 
 * DBPrimaryKey is generated automatically by DBTableClassGenerator if the primary key is specified within the database.
 *
 * @author gregory.graham
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBPrimaryKey {
    
}
