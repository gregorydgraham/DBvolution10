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

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 *
 * @author gregorygraham
 */
public class DBQuery {

    DBDatabase database;
    private List<DBRow> queryTables;
    private List<Class<? extends DBRow>> optionalQueryTables;
    private List<DBRow> allQueryTables;
    private List<DBQueryRow> results;
    private Map<Class<?>, Map<String, DBRow>> existingInstances = new HashMap<Class<?>, Map<String, DBRow>>();
    private Long rowLimit;
    private DBRow[] sortBase;
    private QueryableDatatype[] sortOrder;
    private String resultSQL;
    private boolean cartesianJoinAllowed = false;
    private final int INNER_JOIN = 0;
    private final int LEFT_JOIN = 1;
    private final int RIGHT_JOIN = 2;
    private final int FULL_OUTER_JOIN = 4;
    private boolean useANSISyntax = true;

    private DBQuery(DBDatabase database) {
        this.queryTables = new ArrayList<DBRow>();
        this.optionalQueryTables = new ArrayList<Class<? extends DBRow>>();
        this.allQueryTables = new ArrayList<DBRow>();
        this.database = database;
        this.results = null;
        this.resultSQL = null;
    }

//    private DBQuery(DBDatabase database, DBRow... examples) {
//        this(database);
//        for (DBRow example : examples) {
//            this.add(example);
//        }
//    }
    public static DBQuery getInstance(DBDatabase database, DBRow... examples) {
        DBQuery dbQuery = new DBQuery(database);
        for (DBRow example : examples) {
            dbQuery.add(example);
        }
        return dbQuery;
    }

    /**
     *
     * Add a table to the query
     *
     * @param table
     */
    public DBQuery add(DBRow table) {
        queryTables.add(table);
        allQueryTables.add(table);
        results = null;
        resultSQL = null;
        return this;
    }

    /**
     *
     * Remove a table to the query
     *
     * @param table
     */
    public DBQuery remove(DBRow table) {
        queryTables.remove(table);
        allQueryTables.remove(table);
        results = null;
        resultSQL = null;
        return this;
    }

    /**
     *
     * @param table
     * @return
     */
    public DBQuery addOptional(DBRow table) {
        optionalQueryTables.add(table.getClass());
        allQueryTables.add(table);
        results = null;
        resultSQL = null;
        return this;
    }

    /**
     *
     * @param table
     * @return
     */
    public DBQuery removeOptionalTable(DBRow table) {
        optionalQueryTables.remove(table.getClass());
        allQueryTables.remove(table);
        results = null;
        resultSQL = null;
        return this;
    }

    public String getSQLForQuery() throws SQLException {
        return getSQLForQuery(null);
    }

    public String getANSIJoinClause(DBRow newTable, List<DBRow> previousTables, Set<DBRow> connectedTables) {
        newTable.setDatabase(database);
        List<String> joinClauses = new ArrayList<String>();
        DBDefinition defn = database.getDefinition();
        boolean isLeftOuterJoin = false;
        boolean isFullOuterJoin = false;
        if (queryTables.isEmpty() && optionalQueryTables.size() == allQueryTables.size()) {
            isFullOuterJoin = true;
        } else if (optionalQueryTables.contains(newTable.getClass())) {
            isLeftOuterJoin = true;
        }
        for (DBRow otherTable : previousTables) {
            otherTable.setDatabase(database);
            String join = otherTable.getRelationshipsAsSQL(newTable);
            if (join != null && !join.isEmpty()) {
                joinClauses.add(join);
                connectedTables.add(newTable);
                connectedTables.add(otherTable);
            }
        }
        String sqlToReturn;
        if (previousTables.isEmpty()) {
            sqlToReturn = " " + newTable.getTableName() + " ";
        } else {
            if (isFullOuterJoin) {
                sqlToReturn = defn.beginFullOuterJoin();
            } else if (isLeftOuterJoin) {
                sqlToReturn = defn.beginLeftOuterJoin();
            } else {
                sqlToReturn = defn.beginInnerJoin();
            }
            sqlToReturn += newTable.getTableName();
            if (joinClauses.isEmpty()) {
                sqlToReturn += defn.beginOnClause() + defn.getTrueOperation() + defn.endOnClause();
            } else {
                sqlToReturn += defn.beginOnClause();
                String separator = "";
                for (String join : joinClauses) {
                    sqlToReturn += separator + join;
                    separator = defn.beginAndLine();
                }
                sqlToReturn += defn.endOnClause();
            }
        }
        return sqlToReturn;
    }

    private String getSQLForQuery(String providedSelectClause) throws SQLException {
        DBDefinition defn = database.getDefinition();
        Set<DBRow> connectedTables = new HashSet<DBRow>();
        StringBuilder selectClause = new StringBuilder().append(defn.beginSelectStatement());
        StringBuilder fromClause = new StringBuilder().append(defn.beginFromClause());
        List<DBRow> joinedTables = new ArrayList<DBRow>();
        StringBuilder whereClause = new StringBuilder().append(defn.beginWhereClause()).append(defn.getTrueOperation());
        ArrayList<DBRow> otherTables = new ArrayList<DBRow>();
        String lineSep = System.getProperty("line.separator");

        if (rowLimit != null) {
            selectClause.append(defn.getLimitRowsSubClauseDuringSelectClause(rowLimit));
        }

        String separator = "";
        String colSep = defn.getStartingSelectSubClauseSeparator();
        String tableName;

        for (DBRow tabRow : allQueryTables) {
            otherTables.clear();
            otherTables.addAll(allQueryTables);
            otherTables.remove(tabRow);
            tableName = tabRow.getTableName();

            if (providedSelectClause == null) {
                List<String> columnNames = tabRow.getColumnNames();
                for (String columnName : columnNames) {
                    String formattedColumnName = defn.formatTableAndColumnNameForSelectClause(tableName, columnName);
                    selectClause.append(colSep).append(formattedColumnName);
                    colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;
                }
            } else {
                selectClause = new StringBuilder(providedSelectClause);
            }
            if (!useANSISyntax) {
                fromClause.append(separator).append(tableName);
            } else {
                fromClause.append(getANSIJoinClause(tabRow, joinedTables, connectedTables));
                joinedTables.add(tabRow);
            }
//            tabRow.setDatabase(database);
            String tabRowCriteria = tabRow.getWhereClause(database);
            if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
                whereClause.append(lineSep).append(tabRowCriteria);
            }

            if (!useANSISyntax) {
                for (DBRelationship rel : tabRow.getAdHocRelationships()) {
                    whereClause.append(defn.beginAndLine()).append(rel.generateSQL(database));
                    connectedTables.add(rel.getFirstTable());
                    connectedTables.add(rel.getSecondTable());
                }

                for (DBRow otherTab : otherTables) {
                    Map<DBForeignKey, DBColumn> fks = otherTab.getForeignKeys();
                    for (DBForeignKey fk : fks.keySet()) {
                        final String tabRowPK = tabRow.getPrimaryKeyName();
                        if (tabRowPK != null) {
                            String formattedPK = defn.formatTableAndColumnName(tableName, tabRowPK);
                            Class<? extends DBRow> pkClass = fk.value();
                            DBRow fkReferencesTable = DBRow.getDBRow(pkClass);
                            String fkReferencesColumn = defn.formatTableAndColumnName(fkReferencesTable.getTableName(), fkReferencesTable.getPrimaryKeyName());
                            if (formattedPK.equalsIgnoreCase(fkReferencesColumn)) {
                                String fkColumnName = fks.get(fk).value();
                                String formattedFK = defn.formatTableAndColumnName(otherTab.getTableName(), fkColumnName);
                                whereClause
                                        .append(lineSep)
                                        .append(defn.beginAndLine())
                                        .append(formattedPK)
                                        .append(defn.getEqualsComparator())
                                        .append(formattedFK);
                                connectedTables.add(otherTab);
                                connectedTables.add(tabRow);
                            }
                        }
                    }
                }
            }

            separator = ", " + lineSep;
            otherTables.addAll(allQueryTables);
        }
        if (allQueryTables.size() > 1 && connectedTables.size() < allQueryTables.size() && !cartesianJoinAllowed) {
            throw new AccidentalCartesianJoinException();
        }
        final String sqlString
                = selectClause.append(lineSep)
                .append(fromClause).append(lineSep)
                .append(whereClause).append(lineSep)
                .append(getOrderByClause()).append(lineSep)
                .append(defn.getLimitRowsSubClauseAfterWhereClause(rowLimit))
                .append(defn.endSQLStatement())
                .toString();
        if (database.isPrintSQLBeforeExecuting()) {
            System.out.println(sqlString);
        }

        return sqlString;
    }

    public String getSQLForCount() throws SQLException {
        DBDefinition defn = database.getDefinition();
        return getSQLForQuery(defn.beginSelectStatement() + defn.countStarClause());
    }

    public List<DBQueryRow> getAllRows() throws SQLException {
        results = new ArrayList<DBQueryRow>();
        resultSQL = this.getSQLForQuery();
        DBQueryRow queryRow;

        Statement dbStatement = database.getDBStatement();
        ResultSet resultSet = dbStatement.executeQuery(resultSQL);
        while (resultSet.next()) {
            queryRow = new DBQueryRow();
            for (DBRow tableRow : allQueryTables) {
                DBRow newInstance = DBRow.getDBRow(tableRow.getClass());
                newInstance.setDatabase(database);
                DBDefinition defn = database.getDefinition();
                Map<String, QueryableDatatype> columnsAndQueryableDatatypes = newInstance.getColumnsAndQueryableDatatypes();
                for (String columnName : columnsAndQueryableDatatypes.keySet()) {
                    QueryableDatatype qdt = columnsAndQueryableDatatypes.get(columnName);
                    String fullColumnName = defn.formatColumnNameForResultSet(tableRow.getTableName(), columnName);
                    qdt.setFromResultSet(resultSet, fullColumnName);
                }
                Map<String, DBRow> existingInstancesOfThisTableRow = existingInstances.get(tableRow.getClass());
                if (existingInstancesOfThisTableRow == null) {
                    existingInstancesOfThisTableRow = new HashMap<String, DBRow>();
                    existingInstances.put(tableRow.getClass(), existingInstancesOfThisTableRow);
                }
                DBRow existingInstance = newInstance;
                final QueryableDatatype primaryKey = newInstance.getPrimaryKey();
                if (primaryKey != null) {
                    existingInstance = existingInstancesOfThisTableRow.get(primaryKey.getSQLValue());
                    if (existingInstance == null) {
                        existingInstance = newInstance;
                        existingInstancesOfThisTableRow.put(primaryKey.getSQLValue(), existingInstance);
                    }
                }
                queryRow.put(existingInstance.getClass(), existingInstance);
            }
            results.add(queryRow);
        }
        return results;
    }

    /**
     *
     * Expects there to be exactly one(1) object of the exemplar type.
     *
     * An UnexpectedNumberOfRowsException is thrown if there is zero or more
     * than one row.
     *
     * @param <R>
     * @param exemplar
     * @return
     * @throws SQLException
     * @throws UnexpectedNumberOfRowsException
     */
    public <R extends DBRow> R getOnlyInstanceOf(R exemplar) throws SQLException, UnexpectedNumberOfRowsException {
        List<R> allInstancesFound = getAllInstancesOf(exemplar, 1);
        return allInstancesFound.get(0);
    }

    /**
     *
     * @param <R>: A Java Object that extends DBRow
     * @param exemplar: The DBRow class that you would like returned.
     * @param expected: The expected number of rows, an exception will be thrown
     * if this expectation is not met.
     * @return
     * @throws SQLException
     * @throws UnexpectedNumberOfRowsException
     */
    public <R extends DBRow> List<R> getAllInstancesOf(R exemplar, int expected) throws SQLException, UnexpectedNumberOfRowsException {
        List<R> allInstancesFound = getAllInstancesOf(exemplar);
        final int actual = allInstancesFound.size();
        if (actual > expected) {
            throw new UnexpectedNumberOfRowsException(expected, actual, "Too Many Results: expected " + expected + ", actually got " + actual);
        } else if (actual < expected) {
            throw new UnexpectedNumberOfRowsException(expected, actual, "Too Few Results: expected " + expected + ", actually got " + actual);
        } else {
            return allInstancesFound;
        }
    }

    private boolean needsResults() {
        try {
            return results == null
                    || results.isEmpty()
                    || resultSQL == null
                    || !resultSQL.equals(getSQLForQuery());
        } catch (SQLException ex) {
            return true;
        }
    }

    public <R extends DBRow> List<R> getAllInstancesOf(R exemplar) throws SQLException {
        List<R> arrayList = new ArrayList<R>();
        if (this.needsResults()) {
            getAllRows();
        }
        if (!results.isEmpty()) {
            for (DBQueryRow row : results) {
                final R found = row.get(exemplar);
                if (found != null) { // in case there are no items of the exemplar
                    if (!arrayList.contains(found)) {
                        arrayList.add(found);
                    }
                }
            }
        }
        return arrayList;
    }

    /**
     * Convenience method to print all the rows in the current collection
     * Equivalent to: printAll(System.out);
     *
     */
    public void print() throws SQLException {
        print(System.out);
    }

    /**
     * Fast way to print the results
     *
     * myTable.printRows(System.err);
     *
     * @param ps
     */
    public void print(PrintStream ps) throws SQLException {
        if (needsResults()) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBRow tab : this.allQueryTables) {
                DBRow rowPart = row.get(tab);
                if (rowPart != null) {
                    String rowPartStr = rowPart.toString();
                    ps.print(rowPartStr);
                }
            }
            ps.println();
        }
    }

    /**
     * Fast way to print the results
     *
     * myTable.printRows(System.err);
     *
     * @param printStream
     */
    public void printAllDataColumns(PrintStream printStream) throws SQLException {
        if (needsResults()) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBRow tab : this.allQueryTables) {
                DBRow rowPart = row.get(tab);
                if (rowPart != null) {
                    String rowPartStr = rowPart.toString();
                    printStream.print(rowPartStr);
                }
            }

            printStream.println();
        }
    }

    /**
     * Fast way to print the results
     *
     * myTable.printAllPrimaryKeys(System.err);
     *
     * @param ps
     */
    public void printAllPrimaryKeys(PrintStream ps) throws SQLException {
        if (needsResults()) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBRow tab : this.allQueryTables) {
                DBRow rowPart = row.get(tab);
                if (rowPart != null) {
                    String rowPartStr = rowPart.getPrimaryKey().getSQLValue();
                    ps.print(" " + rowPart.getPrimaryKeyName() + ": " + rowPartStr);
                }
            }

            ps.println();
        }
    }

    public DBQuery clear() {
        this.queryTables.clear();
        this.optionalQueryTables.clear();
        this.allQueryTables.clear();
        results = null;
        return this;
    }

    public Long count() throws SQLException {
        if (results != null) {
            return new Long(results.size());
        } else {
            Long result = 0L;

            Statement dbStatement = database.getDBStatement();
            ResultSet resultSet = dbStatement.executeQuery(this.getSQLForCount());
            while (resultSet.next()) {
                result = resultSet.getLong(1);
            }
            return result;
        }
    }

    public boolean willCreateBlankQuery() {
        boolean willCreateBlankQuery = true;
        for (DBRow table : allQueryTables) {
            willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(this.database);
        }
        return willCreateBlankQuery;
    }

    public void setRowLimit(int i) {
        rowLimit = new Long(i);
        results = null;

    }

    public void clearRowLimit() {
        rowLimit = null;
        results = null;
    }

    public void setSortOrder(DBRow[] baseRows, QueryableDatatype... baseRowColumns) {
        sortBase = baseRows;
        sortOrder = baseRowColumns;
        results = null;
    }

    public void clearSortOrder() {
        sortOrder = null;
    }

    private String getOrderByClause() {
        DBDefinition defn = database.getDefinition();
        if (sortOrder != null) {
            StringBuilder orderByClause = new StringBuilder(defn.beginOrderByClause());
            String sortSeparator = defn.getStartingOrderByClauseSeparator();
            for (QueryableDatatype qdt : sortOrder) {
                final String dbColumnName = DBRow.getTableAndColumnName(sortBase, qdt);
                if (dbColumnName != null) {
                    orderByClause.append(sortSeparator).append(dbColumnName).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
                    sortSeparator = defn.getSubsequentOrderByClauseSeparator();
                }
            }
            orderByClause.append(defn.endOrderByClause());
            return orderByClause.toString();
        }
        return "";
    }

    public void setCartesianJoinsAllowed(boolean allow) {
        this.cartesianJoinAllowed = allow;
    }

    public List<DBQueryRow> getAllRows(Long expectedRows) throws UnexpectedNumberOfRowsException, SQLException {
        List<DBQueryRow> allRows = getAllRows();
        if (allRows.size() != expectedRows) {
            throw new UnexpectedNumberOfRowsException(expectedRows.intValue(), allRows.size());
        } else {
            return allRows;
        }
    }

    /**
     * @return the useANSISyntax
     */
    public boolean isUseANSISyntax() {
        return useANSISyntax;
    }

    /**
     * @param useANSISyntax the useANSISyntax to set
     */
    public void setUseANSISyntax(boolean useANSISyntax) {
        this.useANSISyntax = useANSISyntax;
    }
}
