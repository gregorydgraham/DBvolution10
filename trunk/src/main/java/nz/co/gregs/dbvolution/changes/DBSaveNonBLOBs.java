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
public class DBSaveNonBLOBs extends DBDataChange {

    public <R extends DBRow> DBSaveNonBLOBs(R row) {
        super(row);
    }

//    public <R extends DBRow> DBSaveNonBLOBs(R row, String sql) {
//        super(row, sql);
//    }
    @Override
    public boolean canBeBatched() {
        return true;
    }

    @Override
    public String getSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        final DBRow row = getRow();
        return defn.beginInsertLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginInsertColumnList()
                + getAllColumnsToInsert(db)
                + defn.endInsertColumnList()
                + getValuesClause(db)
                + defn.endInsertLine();
    }

    @Override
    public void execute(DBDatabase db, DBStatement statement) throws SQLException {
        statement.execute(getSQLStatement(db));
    }

    private String getAllColumnsToInsert(DBDatabase database) {
        StringBuilder allFields = new StringBuilder();
        DBDefinition defn = database.getDefinition();
        List<PropertyWrapper> props = getRow().getPropertyWrappers();
        String separator = "";
        for (PropertyWrapper prop : props) {
            // BLOBS are not inserted.so exclude them
            if (prop.isColumn() && !prop.isInstanceOf(DBLargeObject.class)) {
                allFields
                        .append(separator)
                        .append(" ")
                        .append(defn.formatColumnName(prop.columnName()));
                separator = ",";
            }
        }
        return allFields.toString();
    }
    
        public String getValuesClause(DBDatabase db) {
//        this.setDatabase(db);
        StringBuilder string = new StringBuilder();
        Class<? extends DBRow> thisClass = getRow().getClass();
        List<PropertyWrapper> props = getRow().getPropertyWrappers();

        String separator = " VALUES ( ";
        for (PropertyWrapper prop : props) {
            if (prop.isColumn()
                    && !DBLargeObject.class.isAssignableFrom(prop.type())) {
                final QueryableDatatype qdt = prop.getQueryableDatatype();
                string.append(separator).append(qdt.toSQLString(db));
                separator = ",";
            }
        }
        return string.append(")").toString();
    }


}
