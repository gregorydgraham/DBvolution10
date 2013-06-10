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
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
    private List<DBQueryRow> results;

    public DBQuery(DBDatabase database) {
        this.queryTables = new ArrayList<DBTableRow>();
        this.database = database;
        this.results = null;
    }

    /**
     *
     * Add a table to the query
     *
     * @param table
     */
    public void add(DBTableRow table) {
        queryTables.add(table);
        results = null;
    }

    public String generateSQLString() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        StringBuilder selectClause = new StringBuilder().append("select ");
        StringBuilder fromClause = new StringBuilder().append(" from ");
        StringBuilder whereClause = new StringBuilder().append(" where 1=1 ");
        ArrayList<DBTableRow> otherTables = new ArrayList<DBTableRow>();
        String lineSep = System.getProperty("line.separator");

        String separator = "";
        String colSep = "";
        String tableName;

        for (DBTableRow tabRow : queryTables) {
            otherTables.clear();
            otherTables.addAll(queryTables);
            otherTables.remove(tabRow);
            tableName = tabRow.getTableName();
            //DBTable<DBTableRow> actualTable = new DBTable<DBTableRow>(tab, database);

            List<String> columnNames = tabRow.getColumnNames();
            for (String columnName : columnNames) {
                String formattedColumnName = database.formatTableAndColumnName(tableName, columnName);
                selectClause.append(colSep).append(formattedColumnName);
                colSep = ", "+lineSep;
            }
            fromClause.append(separator).append(tableName);
            tabRow.setDatabase(database);
            whereClause.append(tabRow.getWhereClause());

            for (DBTableRow otherTab : otherTables) {
                Map<DBTableForeignKey, DBTableColumn> fks = otherTab.getForeignKeys();
                for (DBTableForeignKey fk : fks.keySet()) {
                    tabRow.setDatabase(database);
                    String formattedPK = database.formatTableAndColumnName(tableName, tabRow.getPrimaryKeyName());
                    if (formattedPK.equalsIgnoreCase(fk.value())) {
                        String fkColumnName = fks.get(fk).value();
                        String formattedFK = database.formatTableAndColumnName(otherTab.getTableName(), fkColumnName);
                        whereClause
                                .append(lineSep)
                                .append("and ")
                                .append(formattedPK)
                                .append(" = ")
                                .append(formattedFK);
                    }
                }
            }

            separator = ", "+lineSep;
            otherTables.addAll(queryTables);
        }

        return selectClause.append(fromClause).append(whereClause).append(";").toString();
    }

    public List<DBQueryRow> getAllRows() throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        results = new ArrayList<DBQueryRow>();
        DBQueryRow queryRow;

        Statement dbStatement = database.getDBStatement();
        ResultSet resultSet = dbStatement.executeQuery(this.generateSQLString());
        while (resultSet.next()) {
            queryRow = new DBQueryRow();
            for (DBTableRow tableRow : queryTables) {
                //String tableName = tableRow.getTableName();
                DBTableRow newInstance = tableRow.getClass().getConstructor().newInstance();
                newInstance.setDatabase(database);
                Map<String, QueryableDatatype> columnsAndQueryableDatatypes = newInstance.getColumnsAndQueryableDatatypes();
                //Field[] fields = tableRow.getClass().getFields();
                for (String columnName : columnsAndQueryableDatatypes.keySet()) {
                    QueryableDatatype qdt = columnsAndQueryableDatatypes.get(columnName);
                    String fullColumnName = database.formatColumnNameForResultSet(tableRow.getTableName(),columnName);
                    String stringOfValue = resultSet.getString(fullColumnName);
                    qdt.isLiterally(stringOfValue);
                }
                queryRow.put(newInstance.getClass(), newInstance);
            }
            results.add(queryRow);
        }
        return results;
    }

    /**
     * Convenience method to print all the rows in the current collection
     * Equivalent to: printAll(System.out);
     *
     */
    public void printAll() throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        printAll(System.out);
    }

    /**
     * Fast way to print the results
     *
     * myTable.printAllRows(System.err);
     *
     * @param ps
     */
    public void printAll(PrintStream ps) throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        if (results == null) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBTableRow tab : this.queryTables) {
                DBTableRow rowPart = row.get(tab.getClass());
                String rowPartStr = rowPart.toString();
                ps.print(rowPartStr);
            }
            ps.println();
        }
    }
}
