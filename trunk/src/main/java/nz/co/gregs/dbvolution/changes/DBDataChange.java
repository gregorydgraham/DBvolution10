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

/**
 *
 * @author gregorygraham
 */
public  abstract class DBDataChange {

    private final DBRow row;
    
    public <R extends DBRow> DBDataChange(R row) {
        super();
        this.row = DBRow.copyDBRow(row);
    }
    
    public abstract List<String> getSQLStatements(DBDatabase db);

    public abstract boolean canBeBatched();

    public abstract void execute(DBDatabase db, DBStatement statement) throws SQLException ;

    /**
     * @return the row
     */
    public DBRow getRow() {
        return row;
    }
    
}
