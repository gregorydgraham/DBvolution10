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
import nz.co.gregs.dbvolution.DBLargeObject;
import nz.co.gregs.dbvolution.DBRow;


public class DBSaveBLOB extends DBAction {
    private DBRow row = null;
    private DBLargeObject blob = null;

    public DBSaveBLOB(DBRow row, DBLargeObject blob) {
        super();
        this.row = row;
        this.blob = blob;
    }

    @Override
    public boolean canBeBatched() {
        return false;
    }

    @Override
    public void execute(Statement statement) throws SQLException{
        String sqlString = "UPDATE "+row.getTableName()+" SET "+row.getDBColumnName(blob)+" = ? WHERE "+row.getPrimaryKeyName() +" = "+row.getPrimaryKey().getSQLValue()+";";
        PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
        prep.setBinaryStream(1, blob.getInputStream());
        prep.execute();
    }

    @Override
    public String getSQLRepresentation() {
        return "// SAVE BINARY DATA";
    }
    
    
}
