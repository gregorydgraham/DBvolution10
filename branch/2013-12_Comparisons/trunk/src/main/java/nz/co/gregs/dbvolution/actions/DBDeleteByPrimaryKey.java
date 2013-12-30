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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

public class DBDeleteByPrimaryKey extends DBDelete {

    private List<DBRow> savedRows = new ArrayList<DBRow>();

    public <R extends DBRow> DBDeleteByPrimaryKey(R row) {
        super(row);
    }

    DBDeleteByPrimaryKey() {
        super();
    }

    @Override
    public DBActionList execute(DBDatabase db, DBRow row) throws SQLException {
        final DBDeleteByPrimaryKey newDeleteAction = new DBDeleteByPrimaryKey(row);
        DBActionList actions = new DBActionList(newDeleteAction);
        DBRow example = DBRow.getDBRow(row.getClass());
        example.getPrimaryKey().setValue(row.getPrimaryKey());
        List<DBRow> rowsToBeDeleted = db.get(example);
        for (DBRow deletingRow : rowsToBeDeleted) {
            newDeleteAction.savedRows.add(DBRow.copyDBRow(deletingRow));
        }
        DBStatement statement = db.getDBStatement();
        for (String str : getSQLStatements(db, row)) {
            statement.execute(str);
        }
        return actions;
    }

    @Override
    public ArrayList<String> getSQLStatements(DBDatabase db, DBRow row) {
        DBDefinition defn = db.getDefinition();

        ArrayList<String> strs = new ArrayList<String>();
        strs.add(defn.beginDeleteLine()
                + defn.formatTableName(row)
                + defn.beginWhereClause()
                + defn.formatColumnName(row.getPrimaryKeyColumnName())
                + defn.getEqualsComparator()
                + row.getPrimaryKey().toSQLString(db)
                + defn.endDeleteLine());
        return strs;
    }

    @Override
    public DBActionList getRevertDBActionList() {
        DBActionList reverts = new DBActionList();
        for (DBRow row : savedRows) {
            reverts.add(new DBInsert(row));
        }
        return reverts;
    }

    @Override
    protected DBActionList getActions(DBRow row) {
        return new DBActionList(new DBDeleteByPrimaryKey(row));
    }

}
