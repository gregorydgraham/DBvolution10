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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

public class DBDeleteByExample extends DBDelete {

    private List<DBRow> savedRows = new ArrayList<DBRow>();

    public <R extends DBRow> DBDeleteByExample(R row) {
        super(row);
    }

    private <R extends DBRow> DBDeleteByExample(DBDatabase db, R row) throws SQLException {
        super(row);
        List<R> gotRows = db.get(row);
        for (R gotRow : gotRows) {
            savedRows.add(DBRow.copyDBRow(gotRow));
        }
    }

    DBDeleteByExample() {
        super();
    }

    @Override
    public DBActionList execute(DBDatabase db, DBRow row) throws SQLException {
        final DBDeleteByExample deleteAction = new DBDeleteByExample(row);
        DBActionList actions = new DBActionList(deleteAction);
        List<DBRow> rowsToBeDeleted = db.get(row);
        for (DBRow deletingRow : rowsToBeDeleted) {
            deleteAction.savedRows.add(DBRow.copyDBRow(deletingRow));
        }
        DBStatement statement = db.getDBStatement();
        for (String str : getSQLStatements(db, row)) {
            statement.execute(str);
        }
        return actions;
    }

    @Override
    public List<String> getSQLStatements(DBDatabase db, DBRow row) {
        DBDefinition defn = db.getDefinition();
        String whereClause = "";
        for (String clause : row.getWhereClausesWithoutAliases(db)) {
            whereClause += defn.beginAndLine() + clause;
        }

        ArrayList<String> strs = new ArrayList<String>();
        strs.add(defn.beginDeleteLine()
                + defn.formatTableName(row)
                + defn.beginWhereClause()
                + defn.getWhereClauseBeginningCondition()
                + whereClause
                + defn.endDeleteLine());
        return strs;
    }

    @Override
    public DBActionList getRevertDBActionList() {
        DBActionList reverts = new DBActionList();
        for (DBRow savedRow : savedRows) {
            reverts.add(new DBInsert(savedRow));
        }
        return reverts;
    }

    @Override
    protected DBActionList getActions(DBRow row) {
        return new DBActionList(new DBDeleteByExample(row));
    }

    @Override
    protected DBActionList getActions(DBDatabase db, DBRow row) throws SQLException{
        return new DBActionList(new DBDeleteByExample(db, row));
    }
}
