package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * DBTableName indicates the name of the database table that this DBRow implementation refers to.
 * 
 * \@DBTableName("my_table")
 * public class MyTable extends DBRow {
 * 
* DBTableName allows you to change the class name without affecting database functionality and is highly recommended.
 * 
 * Extending DBRow is sufficient to indicate that class is associated with a table, 
 * however this causes the class to be tightly bound to the database and subtracts from the benefits of DBvolution
 * 
 * DBTableName is generated automatically by DBTableClassGenerator.
 *
 * @author gregory.graham
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DBTableName {
    String value();
    
}
