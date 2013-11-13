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

    public static DBActionList update(DBDatabase db, DBRow row) throws SQLException {
        DBActionList updates = new DBActionList();
        if (row.hasChangedSimpleTypes()) {
            if (row.getPrimaryKey() == null) {
                updates.addAll(notsosimpleUpdate.execute(db, row));
            } else {
                updates.addAll(simpleUpdate.execute(db, row));
            }
        }
        if (row.hasChangedLargeObjects()) {
            updates.addAll(blobUpdate.execute(db, row));
        }
        return updates;
    }
}
