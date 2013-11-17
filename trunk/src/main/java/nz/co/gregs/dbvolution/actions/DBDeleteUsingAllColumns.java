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
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;

public class DBDeleteUsingAllColumns extends DBDelete {

    private List<DBRow> savedRows = new ArrayList<DBRow>();

    public <R extends DBRow> DBDeleteUsingAllColumns(R row) {
        super(row);
    }

    DBDeleteUsingAllColumns() {
        super();
    }
    
    
    @Override
    public DBActionList execute(DBDatabase db, DBRow row) throws SQLException {
        DBActionList actions = new DBActionList(new DBDeleteUsingAllColumns(row));
        DBStatement statement = db.getDBStatement();
        List<DBRow> rowsToBeDeleted = db.get(row);
        for (DBRow deletingRow : rowsToBeDeleted) {
            savedRows.add(DBRow.copyDBRow(deletingRow));
        }
        for (String str : getSQLStatements(db, row)) {
            statement.execute(str);
        }
        return actions;
    }


    @Override
    public ArrayList<String> getSQLStatements(DBDatabase db, DBRow row) {
        DBDefinition defn = db.getDefinition();

        String sql = defn.beginDeleteLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginWhereClause()
                + defn.getTrueOperation();
        for (PropertyWrapper prop : row.getPropertyWrappers()) {
            QueryableDatatype qdt = prop.getQueryableDatatype();
            sql = sql
                    + defn.beginAndLine()
                    + prop.columnName()
                    + defn.getEqualsComparator()
                    + (qdt.hasChanged() ? qdt.getPreviousSQLValue(db) : qdt.toSQLString(db));
        }
        sql = sql + defn.endDeleteLine();
        ArrayList<String> strs = new ArrayList<String>();
        strs.add(sql);
        return strs;
    }
    
        @Override
    public DBActionList getRevertDBActionList() {
        DBActionList reverts = new DBActionList();
        for (DBRow row : savedRows) {
            reverts.add(new DBSave(row));
        }
        return reverts;
    }

}
