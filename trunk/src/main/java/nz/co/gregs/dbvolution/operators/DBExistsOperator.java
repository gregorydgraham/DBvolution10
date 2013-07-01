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

import java.beans.IntrospectionException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    public DBExistsOperator(DBTableRow tableRow, QueryableDatatype qdtOfTheRow) throws IllegalArgumentException, IllegalAccessException {
        this.tableRow = tableRow;
        Field qdtField = tableRow.getFieldOf(qdtOfTheRow);
        if (qdtField == null) {
            throw new RuntimeException("QueryableDatatype Not Found: the specified " + qdtOfTheRow.getClass().getSimpleName() + " is not part of the specified row, please use only columns from the actual row");
        }
        this.referencedColumnName = tableRow.getDBColumnName(qdtField);
    }
    
    

    @Override
    public String generateWhereLine(DBDatabase database, String columnName) {
        DBTable<DBTableRow> table = new DBTable<DBTableRow>(tableRow, database);
        String subSelect;
        try {
            subSelect = table.getSelectStatementForWhereClause() + table.getWhereClauseWithExampleAndRawSQL(tableRow, " and " + columnName + " = " + referencedColumnName);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        } catch (SQLException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        } catch (IntrospectionException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        }

        return database.beginWhereLine() + (invertOperator?" not ":"")+" exists (" + subSelect + ") ";
    }
}
