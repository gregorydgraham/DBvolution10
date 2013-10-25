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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

public class DBSaveBLOB extends DBAction {

//    private DBRow row = null;
    private DBLargeObject blob = null;

    public DBSaveBLOB(DBRow row, DBLargeObject blob) {
        super(row);
        this.blob = blob;
    }

    @Override
    public boolean canBeBatched() {
        return false;
    }

    @Override
    public void execute(DBDatabase db, Statement statement) throws SQLException {
        DBDefinition defn = db.getDefinition();
        DBRow row = getRow();
        String sqlString = defn.beginUpdateLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginSetClause()
                + defn.formatColumnName(row.getDBColumnName(blob))
                + defn.getEqualsComparator()
                + defn.getPreparedVariableSymbol()
                + defn.beginWhereClause()
                + defn.formatColumnName(row.getPrimaryKeyName())
                + defn.getEqualsComparator()
                + row.getPrimaryKey().toSQLString(db)
                + defn.endSQLStatement();
        db.printSQLIfRequested(sqlString);
        PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
        prep.setBinaryStream(1, blob.getInputStream(), blob.getSize());
        prep.execute();
    }

    @Override
    public String getSQLRepresentation() {
        return "// SAVE BINARY DATA";
    }
}
