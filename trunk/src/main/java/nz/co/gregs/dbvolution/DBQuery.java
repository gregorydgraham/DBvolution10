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

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public class DBQuery {

    DBDatabase database;
    private List<DBRow> queryTables;
    private List<DBQueryRow> results;
    private Map<Class, Map<String, DBRow>> existingInstances = new HashMap<Class, Map<String, DBRow>>();

    public DBQuery(DBDatabase database) {
        this.queryTables = new ArrayList<DBRow>();
        this.database = database;
        this.results = null;
    }

    public DBQuery(DBDatabase database, DBRow... examples) {
        this(database);
        for (DBRow example : examples) {
            this.add(example);
        }
    }

    /**
     *
     * Add a table to the query
     *
     * @param table
     */
    public void add(DBRow table) {
        queryTables.add(table);
        results = null;
    }

    public String generateSQLString() throws SQLException {
        StringBuilder selectClause = new StringBuilder().append("select ");
        StringBuilder fromClause = new StringBuilder().append(" from ");
        StringBuilder whereClause = new StringBuilder().append(database.beginWhereClause()).append(database.getTrueOperation());
        ArrayList<DBRow> otherTables = new ArrayList<DBRow>();
        String lineSep = System.getProperty("line.separator");

        String separator = "";
        String colSep = "";
        String tableName;

        for (DBRow tabRow : queryTables) {
            otherTables.clear();
            otherTables.addAll(queryTables);
            otherTables.remove(tabRow);
            tableName = tabRow.getTableName();
            //DBTable<DBTableRow> actualTable = new DBTable<DBTableRow>(tab, database);

            List<String> columnNames = tabRow.getColumnNames();
            for (String columnName : columnNames) {
                String formattedColumnName = database.formatTableAndColumnName(tableName, columnName);
                selectClause.append(colSep).append(formattedColumnName);
                colSep = ", " + lineSep;
            }
            fromClause.append(separator).append(tableName);
            tabRow.setDatabase(database);
            String tabRowCriteria = tabRow.getWhereClause();
            if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
                whereClause.append(lineSep).append(tabRowCriteria);
            }

            for (DBRow otherTab : otherTables) {
                Map<DBForeignKey, DBColumn> fks = otherTab.getForeignKeys();
                for (DBForeignKey fk : fks.keySet()) {
                    tabRow.setDatabase(database);
                    String formattedPK = database.formatTableAndColumnName(tableName, tabRow.getPrimaryKeyName());
                    Class<? extends DBRow> pkClass = fk.value();
                    DBRow fkReferencesTable = DBRow.getInstance(pkClass);
                    String fkReferencesColumn = database.formatTableAndColumnName(fkReferencesTable.getTableName(), fkReferencesTable.getPrimaryKeyName());
                    if (formattedPK.equalsIgnoreCase(fkReferencesColumn)) {
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

            separator = ", " + lineSep;
            otherTables.addAll(queryTables);
        }
        final String sqlString = selectClause.append(lineSep).append(fromClause).append(lineSep).append(whereClause).append(";").toString();
        if (database.isPrintSQLBeforeExecuting()) {
            System.out.println(sqlString);
        }

        return sqlString;
    }

    public List<DBQueryRow> getAllRows() throws SQLException{
        results = new ArrayList<DBQueryRow>();
        DBQueryRow<Class, DBRow> queryRow;

        Statement dbStatement = database.getDBStatement();
        ResultSet resultSet = dbStatement.executeQuery(this.generateSQLString());
        while (resultSet.next()) {
            queryRow = new DBQueryRow<Class, DBRow>();
            for (DBRow tableRow : queryTables) {
                DBRow newInstance = DBRow.getInstance(tableRow.getClass());//.getClass().getConstructor().newInstance();
                newInstance.setDatabase(database);
                Map<String, QueryableDatatype> columnsAndQueryableDatatypes = newInstance.getColumnsAndQueryableDatatypes();
                for (String columnName : columnsAndQueryableDatatypes.keySet()) {
                    QueryableDatatype qdt = columnsAndQueryableDatatypes.get(columnName);
                    String fullColumnName = database.formatColumnNameForResultSet(tableRow.getTableName(), columnName);
                    String stringOfValue = resultSet.getString(fullColumnName);
                    qdt.isLiterally(stringOfValue);
                }
                Map<String, DBRow> existingInstancesOfThisTableRow = existingInstances.get(tableRow.getClass());
                if (existingInstancesOfThisTableRow == null) {
                    existingInstancesOfThisTableRow = new HashMap<String, DBRow>();
                    existingInstances.put(tableRow.getClass(), existingInstancesOfThisTableRow);
                }
                DBRow existingInstance = existingInstancesOfThisTableRow.get(newInstance.getPrimaryKeySQLStringValue());
                if (existingInstance == null) {
                    existingInstance = newInstance;
                    existingInstancesOfThisTableRow.put(existingInstance.getPrimaryKeySQLStringValue(), existingInstance);
                }
                queryRow.put(existingInstance.getClass(), existingInstance);
            }
            results.add(queryRow);
        }
        return results;
    }

    public ArrayList<DBRow> getAllInstancesOf(DBRow exemplar) throws SQLException {
        HashSet<DBRow> objList = new HashSet<DBRow>();
        if (results.isEmpty()) {
            getAllRows();
        }
        for (DBQueryRow row : results) {
            objList.add(row.get(exemplar));
        }

        DBRow[] arrayOfInstances = objList.toArray(new DBRow[]{});
        ArrayList<DBRow> arrayList = new ArrayList<DBRow>();
        arrayList.addAll(Arrays.asList(arrayOfInstances));
        return arrayList;
    }

    /**
     * Convenience method to print all the rows in the current collection
     * Equivalent to: printAll(System.out);
     *
     */
    public void printAllRows() throws SQLException {
        printAllRows(System.out);
    }

    /**
     * Fast way to print the results
     *
     * myTable.printRows(System.err);
     *
     * @param ps
     */
    public void printAllRows(PrintStream ps) throws SQLException{
        if (results == null) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBRow tab : this.queryTables) {
                DBRow rowPart = row.get(tab);
                String rowPartStr = rowPart.toString();
                ps.print(rowPartStr);
            }
            ps.println();
        }
    }

    /**
     * Fast way to print the results
     *
     * myTable.printAllPrimaryKeys(System.err);
     *
     * @param ps
     */
    public void printAllPrimaryKeys(PrintStream ps) throws SQLException{
        if (results == null) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBRow tab : this.queryTables) {
                DBRow rowPart = row.get(tab);
                String rowPartStr = rowPart.getPrimaryKeySQLStringValue();
                ps.print(" " + rowPart.getPrimaryKeyName() + ": " + rowPartStr);
            }
            ps.println();
        }
    }
}
