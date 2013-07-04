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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
    private Map<Class, Map<String, DBTableRow>> existingInstances = new HashMap<Class, Map<String, DBTableRow>>();

    public DBQuery(DBDatabase database) {
        this.queryTables = new ArrayList<DBTableRow>();
        this.database = database;
        this.results = null;
    }

    public DBQuery(DBDatabase database, DBTableRow... examples) {
        this(database);
        for (DBTableRow example : examples) {
            this.add(example);
        }
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
                colSep = ", " + lineSep;
            }
            fromClause.append(separator).append(tableName);
            tabRow.setDatabase(database);
            String tabRowCriteria = tabRow.getWhereClause();
            if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
                whereClause.append(lineSep).append(tabRowCriteria);
            }

            for (DBTableRow otherTab : otherTables) {
                Map<DBTableForeignKey, DBTableColumn> fks = otherTab.getForeignKeys();
                for (DBTableForeignKey fk : fks.keySet()) {
                    tabRow.setDatabase(database);
                    String formattedPK = database.formatTableAndColumnName(tableName, tabRow.getPrimaryKeyName());
                    Class pkClass = fk.value();
                    DBTableRow fkReferencesTable = (DBTableRow) pkClass.getConstructor().newInstance();
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

        return selectClause.append(lineSep).append(fromClause).append(lineSep).append(whereClause).append(";").toString();
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
                    String fullColumnName = database.formatColumnNameForResultSet(tableRow.getTableName(), columnName);
                    String stringOfValue = resultSet.getString(fullColumnName);
                    qdt.isLiterally(stringOfValue);
                }
                Map<String, DBTableRow> existingInstancesOfThisTableRow = existingInstances.get(tableRow.getClass());
                if (existingInstancesOfThisTableRow == null) {
                    existingInstancesOfThisTableRow = new HashMap<String, DBTableRow>();
                    existingInstances.put(tableRow.getClass(), existingInstancesOfThisTableRow);
                }
                DBTableRow existingInstance = existingInstancesOfThisTableRow.get(newInstance.getPrimaryKeyValue());
                if (existingInstance == null) {
                    existingInstance = newInstance;
                    existingInstancesOfThisTableRow.put(existingInstance.getPrimaryKeyValue(), existingInstance);
                }
                queryRow.put(existingInstance.getClass(), existingInstance);
            }
            results.add(queryRow);
        }
        return results;
    }

    public ArrayList<DBTableRow> getAllInstancesOf(DBTableRow exemplar) throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        HashSet<DBTableRow> objList = new HashSet<DBTableRow>();
        if (results.isEmpty()) {
            getAllRows();
        }
        for (DBQueryRow row : results) {
            objList.add(row.get(exemplar));
        }
        
        DBTableRow[] arrayOfInstances = objList.toArray(new DBTableRow[]{});
        ArrayList<DBTableRow> arrayList = new ArrayList<DBTableRow>();
        arrayList.addAll(Arrays.asList(arrayOfInstances));
        return arrayList;
    }

    /**
     * Convenience method to print all the rows in the current collection
     * Equivalent to: printAll(System.out);
     *
     */
    public void printRows() throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        printRows(System.out);
    }

    /**
     * Fast way to print the results
     *
     * myTable.printRows(System.err);
     *
     * @param ps
     */
    public void printRows(PrintStream ps) throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        if (results == null) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBTableRow tab : this.queryTables) {
                DBTableRow rowPart = row.get(tab);
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
    public void printAllPrimaryKeys(PrintStream ps) throws SQLException, IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        if (results == null) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBTableRow tab : this.queryTables) {
                DBTableRow rowPart = row.get(tab);
                String rowPartStr = rowPart.getPrimaryKeyValue();
                ps.print(" " + rowPart.getPrimaryKeyName() + ": " + rowPartStr);
            }
            ps.println();
        }
    }
}
