/*
 * Copyright 2013 gregorygraham.
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

import java.util.Set;

/**
 * Used to infer foreign keys from the names of columns where
 * there are no foreign key constraints defined for the column.
 * @author gregorygraham
 */
public class ForeignKeyRecognisor {

	/**
	 * Indicates whether the column is a foreign key column,
	 * based on the name of the column itself.
	 * 
	 * <p> Only called for columns that don't have explicit
	 * foreign key constraints.
	 * @param tableName the table containing the column
	 * @param columnName name of the candidate foreign key column
	 * @return
	 */
    public boolean isForeignKeyColumn(String tableName, String columnName) {
        return false;
    }

    /**
     * Gets the name of the table referenced by the foreign key column.
     * The table name is inferred based on the foreign key column name.
     * 
     * <p> Only called for columns that are indicated
     * as foreign keys by {@link #isForeignKeyColumn(String, String)}.
     * @param tableName the table containing the foreign key column
     * @param columnName the foreign key column name
     * @return
     */
    public String getReferencedTable(String tableName, String columnName) {
        return null;
    }

    /**
     * Gets the name of the table referenced by the foreign key column.
     * The table name is inferred based on the foreign key column name.
     * 
     * <p> The default implementation just calls {@link #getReferencedTable(String, String)},
     * but sub-classes can override this implementation to perform smarter
     * mapping where needed.
     * 
     * <p> Only called for columns that are indicated
     * as foreign keys by {@link #isForeignKeyColumn(String, String)}.
     * @param tableName the table containing the foreign key column
     * @param columnName the foreign key column name
     * @param tables names of all tables in the schema
     * @return
     */
    public String getReferencedTable(String tableName, String columnName, Set<String> tables) {
        return getReferencedTable(tableName, columnName);
    }
    
    /**
     * Gets the name of the referenced column in the referenced table,
     * referenced by the foreign key column.
     * A {@code null} value indicates an assumption that the referenced
     * column is the primary key column of the referenced table.
     * 
     * <p> Only called for columns that are indicated
     * as foreign keys by {@link #isForeignKeyColumn(String, String)}.
     * @param tableName the table containing the foreign key column
     * @param columnName the foreign key column name
     * @return the referenced column name or null to reference the primary key column
     */
    public String getReferencedColumn(String tableName, String columnName) {
        return null;
    }

    /**
     * Gets the name of the referenced column in the referenced table,
     * referenced by the foreign key column.
     * A {@code null} value indicates an assumption that the referenced
     * column is the primary key column of the referenced table.
     * 
     * <p> The default implementation just calls {@link #getReferencedColumn(String, String)},
     * but sub-classes can override this implementation to perform smarter
     * mapping where needed.
     * 
     * <p> Only called for columns that are indicated
     * as foreign keys by {@link #isForeignKeyColumn(String, String)}.
     * @param tableName the table containing the foreign key column
     * @param columnName the foreign key column name
     * @param foreignColumns the set of all columns on the referenced table
     * @return the referenced column name or null to reference the primary key column
     */
    public String getReferencedColumn(String tableName, String columnName, Set<String> foreignColumns) {
        return getReferencedColumn(tableName, columnName);
    }
}