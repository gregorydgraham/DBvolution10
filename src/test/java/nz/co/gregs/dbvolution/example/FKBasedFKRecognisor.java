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
package nz.co.gregs.dbvolution.example;

import java.util.regex.Pattern;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;
import nz.co.gregs.dbvolution.generation.ForeignKeyRecognisor;

/**
 * An Example Class To Demonstrate Implementing A ForeignKeyRecognisor.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class FKBasedFKRecognisor extends ForeignKeyRecognisor {

	Pattern fkStartPattern = Pattern.compile("^[fF][kK]_");

	/**
	 * Indicates that the column is a foreign key if the column name starts with
	 * "fk_".
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the column is a foreign key column, FALSE otherwise
	 */
	@Override
	public boolean isForeignKeyColumn(String tableName, String columnName) {
		return columnName.toLowerCase().startsWith("fk_");
	}

	/**
	 * Converts the foreign key to the referenced column name.
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The name of the referenced column
	 */
	@Override
	public String getReferencedColumn(String tableName, String columnName) {
		if (isForeignKeyColumn(tableName, columnName)) {
			String strippedOfFK = "";

			strippedOfFK = fkStartPattern.matcher(columnName).replaceAll("uid_").replaceAll("^(uid_[a-zA-Z0-9]+)(_[0-9]*)*$", "$1");

			return strippedOfFK;
		} else {
			return null;
		}
	}

	/**
	 * Converts the column name into the name of the referenced table.
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the name of the referenced table
	 */
	@Override
	public String getReferencedTable(String tableName, String columnName) {
		if (isForeignKeyColumn(tableName, columnName)) {
			String strippedOfFK = fkStartPattern.matcher(columnName).replaceAll("");
			if (strippedOfFK.matches("^[0-9_]+$")) {
				return "T_" + strippedOfFK.replaceAll("^([a-zA-Z0-9]+)(_[0-9]*)*$", "$1");
			} else {
				return DBTableClassGenerator.toClassCase(strippedOfFK.replaceAll("_[0-9]+$", ""));
			}
		} else {
			return null;
		}
	}
}
