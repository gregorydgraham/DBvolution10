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
import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;

public class DBUpdateLargeObjects extends DBUpdate {

    private DBUpdateLargeObjects(DBRow row){
        super(row);
    }

    DBUpdateLargeObjects() {
        super();
    }

    @Override
    public DBActionList execute(DBDatabase db, DBRow row) throws SQLException {
        DBDefinition defn = db.getDefinition();
        DBStatement statement = db.getDBStatement();
        DBActionList actions = new DBActionList();
        for (PropertyWrapper prop : getInterestingLargeObjects(row)) {
            final String col = prop.columnName();
            final DBLargeObject largeObject = (DBLargeObject) prop.getQueryableDatatype();

            String sqlString = defn.beginUpdateLine()
                    + defn.formatTableName(row.getTableName())
                    + defn.beginSetClause()
                    + defn.formatColumnName(col)
                    + defn.getEqualsComparator()
                    + defn.getPreparedVariableSymbol()
                    + defn.beginWhereClause()
                    + defn.formatColumnName(row.getPrimaryKeyColumnName())
                    + defn.getEqualsComparator()
                    + row.getPrimaryKey().toSQLString(db)
                    + defn.endSQLStatement();
            db.printSQLIfRequested(sqlString);
            PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
            prep.setBinaryStream(1, largeObject.getInputStream(), largeObject.getSize());
            prep.execute();

            DBUpdateLargeObjects update = new DBUpdateLargeObjects(row);
            actions.add(update);
            
            largeObject.setUnchanged();
        }
        return actions;
    }

    @Override
    protected List<String> getSQLStatements(DBDatabase db, DBRow row) {
        List<String> strs = new ArrayList<String>();
        strs.add(db.getDefinition().startMultilineComment() + " SAVE BINARY DATA" + db.getDefinition().endMultilineComment());
        return strs;
    }

    protected List<PropertyWrapper> getInterestingLargeObjects(DBRow row) {
        return row.getChangedLargeObjects();
    }

    @Override
    public DBActionList getRevertDBActionList() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
