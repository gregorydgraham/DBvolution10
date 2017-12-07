package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * DBPrimaryKey indicates that the field is the primary key for the table.
 * <code>
 * <br>
 * <span style="font-weight:bold">&#64;DBPrimaryKey</span><br>
 * &#64;DBColumn("my_table_id")<br>
 * public DBInteger myPrimaryKey = new DBInteger();<br>
 * <br>
 * </code>
 * <p>
 * DBPrimaryKey is used extensively within DBvolution and should be specified in
 * most tables, many-to-many link tables being a notable exception.
 * <p>
 * DBPrimaryKey works with DBForeignKey and DBQuery to make Natural Joins happen
 * automatically.
 * <p>
 * DBPrimaryKey does not require a primary key relationship to exist within the
 * database and does not enforce referential integrity.
 * <p>
 * DBPrimaryKey is generated automatically by DBTableClassGenerator if the
 * primary key is specified within the database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see DBForeignKey
 * @see DBColumn
 * @see DBAutoIncrement
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBPrimaryKey {

}
