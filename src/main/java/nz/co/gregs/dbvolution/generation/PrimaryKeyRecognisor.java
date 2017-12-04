/*
 * Copyright Error: on line 4, column 29 in Templates/Licenses/license-apache20.txt
 Expecting a date here, found: 15/06/2013 Gregory Graham.
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
 * While databases have a mechanism to identify Primary Keys, some don't use it.
 *
 * <p>
 * However there is often a naming convention that makes it obvious that a
 * column is a PK.
 *
 * <p>
 * Extend the methods of this class to help DBvolution automatically recognize
 * the PKs within your schema.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class PrimaryKeyRecognisor {

	/**
	 * Default implementation, returns FALSE
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the column is a PRimary Key, otherwise FALSE.
	 */
	public boolean isPrimaryKeyColumn(String tableName, String columnName) {
		return false;
	}
}
