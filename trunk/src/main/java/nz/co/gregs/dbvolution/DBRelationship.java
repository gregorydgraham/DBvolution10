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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import java.io.Serializable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.operators.DBEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregorygraham
 */
public class DBRelationship implements Serializable{
    public static final long serialVersionUID = 1L;

    private DBRow firstTable;
    private DBRow secondTable;
    private QueryableDatatype firstColumn;
    private QueryableDatatype secondColumn;
    private DBOperator operation;

    public DBRelationship(DBRow thisTable, QueryableDatatype thisTableField, DBRow otherTable, QueryableDatatype otherTableField) {

        this(thisTable, thisTableField, otherTable, otherTableField, new DBEqualsOperator(thisTableField));
    }

    public DBRelationship(DBRow thisTable, QueryableDatatype thisTableField, DBRow otherTable, QueryableDatatype otherTableField, DBOperator operator) {

        firstTable = thisTable;
        firstColumn = thisTableField;
        secondTable = otherTable;
        secondColumn = otherTableField;
        this.operation = operator;
    }

    public String generateSQL(DBDatabase database) {
        final DBDefinition definition = database.getDefinition();
        return getOperation().generateRelationship(database,
                definition.formatTableAndColumnName(firstTable.getTableName(), firstTable.getDBColumnName(getFirstColumn())),
                definition.formatTableAndColumnName(secondTable.getTableName(), secondTable.getDBColumnName(getSecondColumn())));
    }

    /**
     * @return the firstTable
     */
    public DBRow getFirstTable() {
        return firstTable;
    }

    /**
     * @return the secondTable
     */
    public DBRow getSecondTable() {
        return secondTable;
    }

    /**
     * @return the firstColumn
     */
    public QueryableDatatype getFirstColumn() {
        return firstColumn;
    }

    /**
     * @return the secondColumn
     */
    public QueryableDatatype getSecondColumn() {
        return secondColumn;
    }

    /**
     * @return the operation
     */
    public DBOperator getOperation() {
        return operation;
    }
}
