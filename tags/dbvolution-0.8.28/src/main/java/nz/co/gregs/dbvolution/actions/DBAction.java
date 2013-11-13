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
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;

/**
 *
 * @author gregorygraham
 */
public abstract class DBAction {

    private final DBRow row;

    DBAction() {
        super();
        row = null;
    }

    public <R extends DBRow> DBAction(R row) {
        super();
        this.row = DBRow.copyDBRow(row);
    }

    public final List<String> getSQLStatements(DBDatabase db) {
        return getSQLStatements(db, row);
    }

    protected abstract List<String> getSQLStatements(DBDatabase db, DBRow row);

    /**
     *
     * This method performs the DB action and returns a list of all actions
     * perform in the process.
     *
     * The supplied row will be changed by the action in an appropriate way,
     * however the Actions will contain an unchanged and unchangeable copy of
     * the row for internal use.
     *
     * @param db
     * @param row
     * @return
     * @throws SQLException
     */
    protected abstract DBActionList execute(DBDatabase db, DBRow row) throws SQLException;
}
