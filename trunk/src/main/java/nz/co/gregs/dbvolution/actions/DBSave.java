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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 *
 * @author gregorygraham
 */
public class DBSave extends DBAction {

    public <R extends DBRow> DBSave(R row) {
        super(row);
    }

    public <R extends DBRow> DBSave(R row, String sql) {
        super(row, sql);
    }

    @Override
    public boolean canBeBatched() {
        return true;
    }

    @Override
    public String getSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        final DBRow row = getRow();
        if (sql == null || sql.isEmpty()) {
            return defn.beginInsertLine()
                    + defn.formatTableName(row.getTableName())
                    + defn.beginInsertColumnList()
                    + db.getDBTable(row).getAllFieldsForInsert()
                    + defn.endInsertColumnList()
                    + row.getValuesClause(db)
                    + defn.endInsertLine();

        } else {
            return super.getSQLStatement(db);
        }
    }

    @Override
    public void execute(DBDatabase db, DBStatement statement) throws SQLException {
        statement.execute(sql);
    }
}
