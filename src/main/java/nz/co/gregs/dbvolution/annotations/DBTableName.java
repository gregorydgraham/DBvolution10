package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code DBTableName} indicates the name of the database table that this
 * {@code DBRow} implementation refers to.  
 * <code>
 * <br>
 * &#64;DBTableName("my_table")
 * public class MyTable extends DBRow {
 * </code>
 *
 * <p>
 * {@code DBTableName} allows you to change the class name without affecting
 * database functionality and is highly recommended. Extending DBRow is
 * sufficient to indicate that class is associated with a table, however this
 * causes the class to be tightly bound to the database and subtracts from the
 * benefits of DBvolution.
 *
 * <p>
 * This annotation is inherited by subclasses.
 *
 * <p>
 * DBTableName is generated automatically by DBTableClassGenerator.
 *
 * @author Gregory Graham
 */
@Target(ElementType.TYPE)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface DBTableName {

	String value();
}
