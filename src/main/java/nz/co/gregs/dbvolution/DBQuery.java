/*
 * Copyright Error: on line 4, column 29 in Templates/Licenses/license-apache20.txt
 Expecting a date here, found: 6/06/2013 gregorygraham.
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

import java.beans.IntrospectionException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTableForeignKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public class DBQuery {

    DBDatabase database;
    private List<DBTableRow> queryTables;

    public DBQuery(DBDatabase database) {
        this.queryTables = new ArrayList<DBTableRow>();
        this.database = database;
    }

    /**
     *
     * Add a table to the query
     *
     * @param table
     */
    public void add(DBTableRow table) {
        queryTables.add(table);
    }

    public String generateSQLString() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException {
        StringBuilder selectClause = new StringBuilder().append("select ");
        StringBuilder fromClause = new StringBuilder().append(" from ");
        StringBuilder whereClause = new StringBuilder().append(" where 1=1 ");
        ArrayList<DBTableRow> otherTables = new ArrayList<DBTableRow>();

        String separator = "";
        String colSep = "";
        String tableName;

        for (DBTableRow tab : queryTables) {
            otherTables.clear();
            otherTables.addAll(queryTables);
            otherTables.remove(tab);
            tableName = tab.getTableName();
            //DBTable<DBTableRow> actualTable = new DBTable<DBTableRow>(tab, database);

            List<String> columnNames = tab.getColumnNames();
            for (String columnName : columnNames) {
                String formattedColumnName = database.formatTableAndColumnForDBTableForeignKey(tableName, columnName);
                selectClause.append(colSep).append(formattedColumnName);
                colSep = ", ";
            }
            fromClause.append(separator).append(tableName);

            for (DBTableRow otherTab : otherTables) {
                Map<DBTableForeignKey, DBTableColumn> fks = otherTab.getForeignKeys();
                for (DBTableForeignKey fk : fks.keySet()) {
                    tab.setDatabase(database);
                    String formattedPK = database.formatTableAndColumnForDBTableForeignKey(tableName, tab.getPrimaryKeyName());
                    if (formattedPK.equalsIgnoreCase(fk.value())) {
                        String fkColumnName = fks.get(fk).value();
                        String formattedFK = database.formatTableAndColumnForDBTableForeignKey(otherTab.getTableName(), fkColumnName);
                        whereClause
                                .append(" and ")
                                .append(formattedPK)
                                .append(" = ")
                                .append(formattedFK);
                    }
                }
            }

            separator = ", ";
            otherTables.addAll(queryTables);
        }

        return selectClause.append(fromClause).append(whereClause).append(";").toString();
    }

    public List<Map<Class, DBTableRow>> getResults() throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException {
        ArrayList<Map<Class, DBTableRow>> resultList = new ArrayList<Map<Class, DBTableRow>>();
        Map<Class, DBTableRow> rowClassMap;

        Statement dbStatement = database.getDBStatement();
        ResultSet resultSet = dbStatement.executeQuery(this.generateSQLString());
        while (resultSet.next()) {
            rowClassMap = new HashMap();
            for (DBTableRow tab : queryTables) {
                String tableName = tab.getTableName();
                Field[] fields = tab.getClass().getFields();
                for (Field field : fields) {
                    DBTableColumn columnName = field.getAnnotation(DBTableColumn.class);
                    QueryableDatatype qdt = tab.getQueryableValueOfField(field);
                    //EITHER
                    // pick the table+column from the resultset and use the right QDT impl
                    String formattedColumnName = database.formatTableAndColumnForDBTableForeignKey(tableName, columnName.value());
                    String stringOfValue = resultSet.getString(formattedColumnName);
                    qdt.isLiterally(stringOfValue);
                    //OR
                    // crop the result set and send it to the existing DBTableRow functions
                    //throw new RuntimeException("NOT YET IMPLEMENTED");
                }
                rowClassMap.put(tab.getClass(), tab);
            }
            resultList.add(rowClassMap);
        }
        return resultList;
    }
}
