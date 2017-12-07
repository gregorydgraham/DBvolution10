/*
 * Copyright 2013 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * Used to indicate the class references a SQL query rather than a table or view
 * and the query itself.
 * <code>
 * <br>
 * <span style="font-weight:bold"> &#64;DBSelectQuery("select my_table_id,
 * other_table_fk from my_table") </span><br>
 * &#64;DBTableName("my_table")<br>
 * public class MyTableQuery extends DBRow {<br>
 * <br>
 * </code>
 * <p>
 * This class is provided to help with the 1% of queries that DBvolution is not
 * designed for, use it rarely and carefully.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DBSelectQuery {

	/**
	 * The raw SELECT clause.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the Select SQL.
	 */
	String value() default "";

}
