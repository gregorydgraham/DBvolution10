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
package nz.co.gregs.dbvolution.generation;

/**
 * A Helper class to capture naming conventions of databases.
 *
 * <p>
 * While databases have a mechanism to identify Foreign Keys, few actually use
 * it.
 *
 * <p>
 * However there is often a naming convention that makes it obvious that a
 * column is a FK.
 *
 * <p>
 * Extend the methods of this class to help DBvolution automatically recognize
 * the FKs within your schema.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class ForeignKeyRecognisor {

	/**
	 * Default implementation, returns FALSE.
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the column is a foreign key reference, FALSE otherwise
	 */
	public boolean isForeignKeyColumn(String tableName, String columnName) {
		return false;
	}

	/**
	 * Default implementation, returns NULL.
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The database name of the referenced column derived from the
	 * referencing table and column, or NULL
	 */
	public String getReferencedColumn(String tableName, String columnName) {
		return null;
	}

	/**
	 * Default implementation, returns NULL.
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The database name of the referenced table based on the referencing
	 * table and column or NULL.
	 */
	public String getReferencedTable(String tableName, String columnName) {
		return null;
	}
}
