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

import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;


public class DBDeleteUsingAllColumns extends DBDelete {

    public <R extends DBRow> DBDeleteUsingAllColumns(R row) {
        super(row);
    }
    
    @Override
    public String getSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        DBRow row = getRow();
        String sql
                = defn.beginDeleteLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginWhereClause()
                + defn.getTrueOperation();
        for (PropertyWrapper prop : row.getPropertyWrappers()) {
            QueryableDatatype qdt = prop.getQueryableDatatype();
            sql = sql
                    + defn.beginAndLine()
                    + prop.columnName()
                    + defn.getEqualsComparator()
                    + (qdt.hasChanged() ? qdt.getPreviousSQLValue(db) : qdt.toSQLString(db));
        }
        sql = sql + defn.endDeleteLine();
        return sql;
    }
}
