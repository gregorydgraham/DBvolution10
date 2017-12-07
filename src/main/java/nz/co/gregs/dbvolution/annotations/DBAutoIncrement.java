/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;

/**
 *
 * Used to indicate that this field is an auto-incrementing column in the
 * database.
 *
 * <p>
 * DBAutoIncrement allows you to insert rows without specifying the primary key,
 * have the PK generated automatically, and have the row inserted updated with
 * the new PK value:</p>
 * <code>
 * <br>
 * MyRow row = new MyRow();<br>
 * row.name.setValue("example");<br>
 * database.insert(row);<br>
 * row.pkColumn.getValue(); // now contains the primary key created by the
 * database.<br>
 * <br>
 * </code>
 * <p>
 * It also allows DBvolution to create the correct data types to automatically
 * populate primary keys.</p>
 *
 * <p>
 * Example of use:</p>
 * <code>
 * <br>
 * public class MyRow extends DBRow{<br>
 * <br>
 * &#64;DBColumn("primary_key_col")<br>
 * &#64;DBPrimaryKey<br>
 * <span style="font-weight: bold">&#64;DBAutoIncrement</span><br>
 * public DBInteger pkColumn = new DBInteger();<br>
 * <br>
 * &#64;DBColumn<br>
 * public DBInteger name = new DBInteger();<br>
 * }<br>
 * <br>
 * </code>
 * <p>
 * DBAutoIncrement has no effect unless the field is also a DBColumn and a
 * DBPrimaryKey. It should also be a DBNumber or DBinteger as shown above.</p>
 *
 * <p>
 * DBAutoIncrement is generated automatically by DBTableClassGenerator.</p>
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see DBColumn
 * @see DBPrimaryKey
 * @see DBInteger
 * @see DBNumber
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBAutoIncrement {
//    String value() default "";
}
