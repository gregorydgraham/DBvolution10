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
import nz.co.gregs.dbvolution.exceptions.AccidentalUpdateOfUndefinedRowException;

public abstract class DBUpdate extends DBAction {

    static final DBUpdateLargeObjects blobUpdate = new DBUpdateLargeObjects();
    static final DBUpdateSimpleTypes simpleUpdate = new DBUpdateSimpleTypes();
    static final DBUpdateSimpleTypesUsingAllColumns notsosimpleUpdate = new DBUpdateSimpleTypesUsingAllColumns();

    public DBUpdate() {
        super();
    }

    public <R extends DBRow> DBUpdate(R row) {
        super(row);
    }

    /**
     * Executes required update actions for the row and returns a DBACtionList
     * of those actions.
     *
     * @param db
     * @param row
     * @return a DBActionList of updates that have been executed.
     * @throws SQLException
     */
    public static DBActionList update(DBDatabase db, DBRow row) throws SQLException {
        DBActionList updates = getUpdates(row);
        for (DBAction act : updates) {
            act.execute(db, row);
        }
        return updates;
    }

    /**
     * Creates a DBActionList of update actions for the rows.
     *
     * <p>
     * The actions created can be applied on a particular database using
     * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
     *
     * <p>
     * Synonym for {@link #getUpdates(nz.co.gregs.dbvolution.DBRow...) }
     *
     * @param rows
     * @return a DBActionList of updates.
     * @throws SQLException
     */
    public static DBActionList update(DBRow... rows) throws SQLException {
        return getUpdates(rows);
    }

    /**
     * Creates a DBActionList of update actions for the rows.
     *
     * <p>
     * The actions created can be applied on a particular database using
     * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
     *
     * @param rows
     * @return a DBActionList of updates.
     * @throws SQLException
     */
    public static DBActionList getUpdates(DBRow... rows) throws SQLException {
        DBActionList updates = new DBActionList();
        for (DBRow row : rows) {
            if (row.getDefined()) {
                if (row.hasChangedSimpleTypes()) {
                    if (row.getPrimaryKey() == null) {
                        updates.add(new DBUpdateSimpleTypesUsingAllColumns(row));
                    } else {
                        updates.add(new DBUpdateSimpleTypes(row));
                    }
                }
                if (row.hasChangedLargeObjects()) {
                    updates.add(new DBUpdateLargeObjects(row));
                }
            } else {
                throw new AccidentalUpdateOfUndefinedRowException(row);
            }
        }
        return updates;
    }
}
