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
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;

public abstract class DBDelete extends DBAction {

    private static DBDeleteByExample example = new DBDeleteByExample();
    private static DBDeleteByPrimaryKey pk = new DBDeleteByPrimaryKey();
    private static DBDeleteUsingAllColumns allCols = new DBDeleteUsingAllColumns();

    public DBDelete() {
        super();
    }

    public <R extends DBRow> DBDelete(R row) {
        super(row);
    }

    /**
     * Deletes the specified row or example from the database and returns the
     * actions performed.
     *
     * @param database
     * @param row
     * @return the actions executed as a DBActionList
     * @throws SQLException
     */
    public static DBActionList delete(DBDatabase database, DBRow row) throws SQLException {
        DBActionList delete = getDeletes(row);
        return delete.execute(database);
//        if (row.getDefined()) {
//            if (row.getPrimaryKey() == null) {
//                return allCols.execute(database, row);
//            } else {
//                return pk.execute(database, row);
//            }
//        } else {
//            return example.execute(database, row);
//        }
    }

    /**
     * Creates a DBActionList of delete actions for the rows.
     *
     * <p>
     * The actions created can be applied on a particular database using
     * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
     *
     * <p>
     * This method cannot produce DBInsert statements for the revert action list
     * until the actions have been executed. If you need the revert script to
     * include insert statements use the {@link #getDeletes(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBRow[])
     * } method.
     *
     * @param rows
     * @return a DBActionList of deletes.
     * @throws SQLException
     */
    public static DBActionList getDeletes(DBRow... rows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (DBRow row : rows) {
            if (row.getDefined()) {
                if (row.getPrimaryKey() == null) {
                    actions.addAll(allCols.getActions(row));
                } else {
                    actions.addAll(pk.getActions(row));
                }
            } else {
                actions.addAll(example.getActions(row));
            }
        }
        return actions;
    }

    /**
     * Creates a DBActionList of delete actions for the rows.
     *
     * <p>
     * The actions created can be applied on a particular database using
     * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
     *
     * <p>
     * The DBDatabase instance will be used to create DBInsert actions for the revert
     * action list.
     *
     *
     * @param rows
     * @return a DBActionList of deletes.
     * @throws SQLException
     */
    public static DBActionList getDeletes(DBDatabase db, DBRow... rows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (DBRow row : rows) {
            if (row.getDefined()) {
                if (row.getPrimaryKey() == null) {
                    actions.addAll(allCols.getActions(db, row));
                } else {
                    actions.addAll(pk.getActions(db, row));
                }
            } else {
                actions.addAll(example.getActions(db, row));
            }
        }
        return actions;
    }
    
    protected abstract DBActionList getActions(DBDatabase db, DBRow row) throws SQLException;
}
