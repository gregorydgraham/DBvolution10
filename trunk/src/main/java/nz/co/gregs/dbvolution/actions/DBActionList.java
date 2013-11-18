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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public class DBActionList extends ArrayList<DBAction> {

    private static final long serialVersionUID = 1L;

    public DBActionList(DBAction... actions) {
        super();
        this.addAll(Arrays.asList(actions));
    }

    public synchronized List<String> getSQL(DBDatabase db) {
        List<String> sqlList = new ArrayList<String>();
        for (DBAction act : this) {
            sqlList.addAll(act.getSQLStatements(db));
        }
        return sqlList;
    }

    public synchronized void execute(DBDatabase database) throws SQLException {
        for (DBAction action : this) {
            action.execute(database);
        }
    }

    public DBActionList getRevertActionList() {
        DBAction[] toArray = this.toArray(new DBAction[]{});
        DBActionList reverts = new DBActionList();
        for (int i = toArray.length-1 ;  i >= 0 ; i--) {
            reverts.addAll(toArray[i].getRevertDBActionList());
        }
        return reverts;
    }
}
