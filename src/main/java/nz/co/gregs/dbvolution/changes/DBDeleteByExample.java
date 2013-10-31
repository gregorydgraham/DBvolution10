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

package nz.co.gregs.dbvolution.changes;

import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;


public class DBDeleteByExample extends DBDelete {

    public <R extends DBRow> DBDeleteByExample(R row) {
        super(row);
    }
    
    @Override
    public List<String> getSQLStatements(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        DBRow row = getRow();
        ArrayList<String> strs = new ArrayList<String>();
        strs.add(defn.beginDeleteLine()
                                      + defn.formatTableName(row.getTableName())
                                      + defn.beginWhereClause()
                                      + defn.getTrueOperation()
                                      + row.getWhereClause(db)
                                      + defn.endDeleteLine());
        return strs;
    }
}
