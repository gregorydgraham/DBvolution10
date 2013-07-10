/*
 * Copyright 2013 gregory.graham.
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
package nz.co.gregs.dbvolution.operators;

import java.lang.reflect.Field;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.QueryableDatatype;

/**
 *
 * @author gregory.graham
 */
public class DBExistsOperator extends DBOperator {

    DBTableRow tableRow;
    private final String referencedColumnName;

    public DBExistsOperator(DBTableRow tableRow, QueryableDatatype qdtOfTheRow) {
        this.tableRow = tableRow;
        Field qdtField = tableRow.getFieldOf(qdtOfTheRow);
        if (qdtField == null) {
            throw new RuntimeException("QueryableDatatype Not Found: the specified " + qdtOfTheRow.getClass().getSimpleName() + " is not part of the specified row, please use only columns from the actual row");
        }
        this.referencedColumnName = tableRow.getDBColumnName(qdtField);
    }
    
    

    @Override
    public String generateWhereLine(DBDatabase database, String columnName) {
        DBTable<DBTableRow> table = new DBTable<DBTableRow>(database, tableRow);
        String subSelect;
        try {
            subSelect = table.getSelectStatementForWhereClause() + table.getWhereClauseWithExampleAndRawSQL(tableRow, " and " + columnName + " = " + referencedColumnName);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        }

        return database.beginAndLine() + (invertOperator?" not ":"")+" exists (" + subSelect + ") ";
    }
}
