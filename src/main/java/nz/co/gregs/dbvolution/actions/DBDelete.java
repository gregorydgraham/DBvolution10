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

public abstract class DBDelete extends DBAction {

    private static DBDeleteByExample example = new DBDeleteByExample();
    private static DBDeleteByPrimaryKey pk = new DBDeleteByPrimaryKey();
    private static DBDeleteUsingAllColumns allCols = new DBDeleteUsingAllColumns();
    
    public DBDelete() {
        super();
    }
    
    public <R extends DBRow> DBDelete(R row) {
        super(row);
    }

    public static DBActionList delete(DBDatabase database, DBRow row) throws SQLException{
        
        if (row.getDefined()){
            if (row.getPrimaryKey()==null){
                return allCols.execute(database, row);
            }else{
                return pk.execute(database, row);
            }
        }else{
            return example.execute(database, row);
        }
    }    
}
