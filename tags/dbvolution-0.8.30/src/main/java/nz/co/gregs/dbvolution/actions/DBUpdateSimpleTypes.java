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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;

/**
 *
 * @author gregory.graham
 */
public class DBUpdateSimpleTypes extends DBUpdate {
//    private List<QueryableDatatype> changedQDTs = new ArrayList<QueryableDatatype>();

    DBUpdateSimpleTypes(DBRow row) {
        super(row);
    }

    DBUpdateSimpleTypes() {
        super();
    }

    @Override
    public DBActionList execute(DBDatabase db, DBRow row) throws SQLException {
        DBStatement statement = db.getDBStatement();
        DBActionList actions = new DBActionList(new DBUpdateSimpleTypes(row));
        for (String sql : getSQLStatements(db, row)) {
            statement.execute(sql);
            row.setSimpleTypesToUnchanged();
        }
        return actions;
    }

    @Override
    public List<String> getSQLStatements(DBDatabase db, DBRow row) {
        List<String> sqls = new ArrayList<String>();
        DBDefinition defn = db.getDefinition();

        String sql = defn.beginUpdateLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginSetClause()
                + getSetClause(db, row)
                + defn.beginWhereClause()
                + getWhereClause(db, row)
                + defn.endDeleteLine();
        sqls.add(sql);
        return sqls;
    }

    protected String getSetClause(DBDatabase db, DBRow row) {
        DBDefinition defn = db.getDefinition();
        StringBuilder sql = new StringBuilder();
        List<PropertyWrapper> fields = row.getPropertyWrappers();

        String separator = defn.getStartingSetSubClauseSeparator();
        for (PropertyWrapper field : fields) {
            if (field.isColumn()) {
                final QueryableDatatype qdt = field.getQueryableDatatype();
                if (qdt.hasChanged()) {
                    String columnName = field.columnName();
                    sql.append(separator)
                            .append(defn.formatColumnName(columnName))
                            .append(defn.getEqualsComparator())
                            .append(qdt
                            .toSQLString(db));
                    separator = defn.getSubsequentSetSubClauseSeparator();
                }
            }
        }
        return sql.toString();
    }

    @Override
    public DBActionList getRevertDBActionList() {
        DBActionList dbActionList = new DBActionList();
        dbActionList.add(new DBUpdateToPreviousValues(this.row));
        return dbActionList;
    }

    String getWhereClause(DBDatabase db, DBRow row) {
        DBDefinition defn = db.getDefinition();
        QueryableDatatype primaryKey = row.getPrimaryKey();
        String pkOriginalValue = (primaryKey.hasChanged() ? primaryKey.getPreviousSQLValue(db) : primaryKey.toSQLString(db));
        return defn.formatColumnName(row.getPrimaryKeyColumnName())
                + defn.getEqualsComparator()
                + pkOriginalValue;
    }

    @Override
    protected DBActionList getActions(DBRow row){
       return new DBActionList(new DBUpdateSimpleTypes(row));
    }
}
