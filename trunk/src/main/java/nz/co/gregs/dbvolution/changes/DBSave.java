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
package nz.co.gregs.dbvolution.changes;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;

/**
 *
 * @author gregorygraham
 */
public class DBSave extends DBDataChange {
    
    private DBChangeList blobUpdates;
    private transient StringBuilder allColumns;
    private transient StringBuilder allValues;
    
    public <R extends DBRow> DBSave(R row) {
        super(row);
    }
    
    @Override
    public boolean canBeBatched() {
        return blobUpdates.isEmpty();
    }
    
    @Override
    public ArrayList<String> getSQLStatements(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        processAllFieldsForInsert(db);
        final DBRow row = getRow();
        ArrayList<String> strs = new ArrayList<String>();
        strs.add(defn.beginInsertLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginInsertColumnList()
                + allColumns
                + defn.endInsertColumnList()
                + allValues
                + defn.endInsertLine());
        return strs;
    }
    
    @Override
    public void execute(DBDatabase db, DBStatement statement) throws SQLException {
        for (String sql : getSQLStatements(db)) {
            statement.execute(sql);
        }
        for (DBDataChange update : blobUpdates) {
            update.execute(db, statement);
        }
    }
    
    private void processAllFieldsForInsert(DBDatabase database) {
        allColumns = new StringBuilder();
        allValues = new StringBuilder();
        blobUpdates = new DBChangeList();
        DBDefinition defn = database.getDefinition();
        List<PropertyWrapper> props = getRow().getPropertyWrappers();
        String columnSeparator = "";
        String valuesSeparator = " VALUES ( ";
        for (PropertyWrapper prop : props) {
            // BLOBS are not inserted normally so create updates for them
            if (prop.isColumn()) {
                if (prop.isInstanceOf(DBLargeObject.class)) {
                    QueryableDatatype qdt = prop.getQueryableDatatype();
                    if (qdt instanceof DBLargeObject) {
                        DBLargeObject blob = (DBLargeObject) qdt;
                        DBUpdateBLOB updateBLOB = new DBUpdateBLOB(getRow(), prop.columnName(), blob);
                        blobUpdates.add(updateBLOB);
                    }
                } else {
                    final QueryableDatatype qdt = prop.getQueryableDatatype();
                    // nice normal columns
                    // Add a column
                    allColumns
                            .append(columnSeparator)
                            .append(" ")
                            .append(defn.formatColumnName(prop.columnName()));
                    columnSeparator = ",";
                    // add the value
                    allValues.append(valuesSeparator).append(qdt.toSQLString(database));
                    valuesSeparator = ",";
                }
            }
        }
        allValues.append(")");
    }
//    public String getValuesClause(DBDatabase db) {
////        this.setDatabase(db);
//        StringBuilder string = new StringBuilder();
//        Class<? extends DBRow> thisClass = getRow().getClass();
//        List<PropertyWrapper> props = getRow().getPropertyWrappers();
//
//        String separator = " VALUES ( ";
//        for (PropertyWrapper prop : props) {
//            if (prop.isColumn()
//                    && !DBLargeObject.class.isAssignableFrom(prop.type())) {
//                final QueryableDatatype qdt = prop.getQueryableDatatype();
//                string.append(separator).append(qdt.toSQLString(db));
//                separator = ",";
//            }
//        }
//        return string.append(")").toString();
//    }
}
