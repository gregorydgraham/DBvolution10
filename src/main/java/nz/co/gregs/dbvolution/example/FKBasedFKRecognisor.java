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
package nz.co.gregs.dbvolution.example;

import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;
import nz.co.gregs.dbvolution.generation.ForeignKeyRecognisor;

/**
 *
 * @author gregorygraham
 */
public class FKBasedFKRecognisor extends ForeignKeyRecognisor {

    /**
     *
     * @param tableName
     * @param columnName
     * @return
     */
    @Override
    public boolean isForeignKeyColumn(String tableName, String columnName) {
        return columnName.toLowerCase().startsWith("fk_");
    }

    /**
     *
     * @param tableName
     * @param columnName
     * @return
     */
    @Override
    public String getReferencedColumn(String tableName, String columnName) {
        if (isForeignKeyColumn(tableName, columnName)) {
            return columnName.replaceAll("^fk_", "uid_");
        } else {
            return null;
        }
    }

    /**
     *
     * @param tableName
     * @param columnName
     * @return
     */
    @Override
    public String getReferencedTable(String tableName, String columnName) {
        if (isForeignKeyColumn(tableName, columnName)) {
            return DBTableClassGenerator.toClassCase(columnName.replaceAll("^fk_", ""));
        } else {
            return null;
        }
    }
}
