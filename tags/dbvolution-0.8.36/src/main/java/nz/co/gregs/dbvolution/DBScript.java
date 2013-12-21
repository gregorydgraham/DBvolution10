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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.transactions.DBTransaction;
import java.util.List;
import nz.co.gregs.dbvolution.actions.DBActionList;

/**
 * A convenient method of implement a database script in DBvolution
 *
 * @author gregory.graham
 */
public abstract class DBScript {

    /**
     *
     * Create all the database interaction is this method.
     * 
     * Call test() or implement() to safely run the script within a transaction.
     * 
     * Use the List<String> to return a revert script.
     * 
     * @param db
     * @return List<String>
     * @throws Exception
     */
    public abstract DBActionList script(DBDatabase db) throws Exception;

    /**
     * Run the script in a committed transaction.
     * 
     * Implement() wraps the script() method in a transaction and commits it.
     * 
     * Any exceptions will cause the script to abort and rollback safely.
     * 
     * When the script executes without exceptions the changes will be committed and made permanent.
     *
     * @param db
     * @return
     * @throws Exception
     */
    public final DBActionList implement(DBDatabase db) throws Exception {
        DBTransaction<DBActionList> trans = getDBTransaction();
        DBActionList revertScript = db.doTransaction(trans);
        return revertScript;
    }

    /**
     * Run the script in a read-only transaction.
     * 
     * Test() wraps the script() method in a transaction but rolls it back.
     * 
     * Any changes will be safely rolled back.
     *
     * @param db
     * @return
     * @throws Exception
     */
    public final DBActionList test(DBDatabase db) throws Exception {
        DBTransaction<DBActionList> trans = getDBTransaction();
        DBActionList revertScript = db.doReadOnlyTransaction(trans);
        return revertScript;
    }

    public final DBTransaction<DBActionList> getDBTransaction() {
        return new DBTransaction<DBActionList>() {
            @Override
            public DBActionList doTransaction(DBDatabase dbd) throws Exception {
                DBActionList revertScript = script(dbd);
                return revertScript;
            }
        };
    }

    /**
     * Convenience method to print out the List<String> as an SQL script.
     *
     * @param statementList
     */
    public static void printStatementList(List<String> statementList) {
        System.out.println("--BEGIN SCRIPT--");
        System.out.println("begin;");
        System.out.println("");
        for (String sql : statementList) {
            System.out.println("" + sql);
        }
        System.out.println("");
        System.out.println("rollback;");
        System.out.println("--END SCRIPT--");
    }

}
