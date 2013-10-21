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
 * Used to indicate that this field is associated with a database column and the name of that column
 * 
 * \@DBColumn("my_column")
 * public DBString myColumn = new DBString();
 * 
 * DBColumn allows you to change the field name without affecting database functionality and is highly recommended.
 * 
 * Using a QueryableDatatype is sufficient to indicate that the field is associated with a column, 
 * however this causes the class API to be tightly bound to the database and subtracts from the benefits of DBvolution
 * 
 * DBColumn is generated automatically by DBTableClassGenerator.
 *
 * @author gregory.graham
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBColumn {
    String value() default "";
    
}
