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
package nz.co.gregs.dbvolution;

import java.io.PrintStream;
import java.sql.*;
import java.util.*;

import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DBDataComparison;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.operators.DBOperator;
import nz.co.gregs.dbvolution.query.QueryGraph;

/**
 *
 * @author gregorygraham
 */
public class DBQuery {

    DBDatabase database;
    private final List<DBRow> queryTables;
    private final List<Class<? extends DBRow>> optionalQueryTables;
    private final List<DBRow> allQueryTables;
    private List<DBQueryRow> results;
    private final Map<Class<?>, Map<String, DBRow>> existingInstances = new HashMap<Class<?>, Map<String, DBRow>>();
    private Long rowLimit;
    private List<PropertyWrapper> sortOrder = null;
    private String resultSQL;
    private boolean useANSISyntax = true;
    private boolean cartesianJoinAllowed = false;
    private boolean blankQueryAllowed = false;
    private final List<DBDataComparison> comparisons = new ArrayList<DBDataComparison>();
    private QueryOptions options = new QueryOptions();

    private DBQuery(DBDatabase database) {
        this.queryTables = new ArrayList<DBRow>();
        this.optionalQueryTables = new ArrayList<Class<? extends DBRow>>();
        this.allQueryTables = new ArrayList<DBRow>();
        this.database = database;
        this.results = null;
        this.resultSQL = null;
    }

    public static DBQuery getInstance(DBDatabase database, DBRow... examples) {
        DBQuery dbQuery = new DBQuery(database);
        for (DBRow example : examples) {
            dbQuery.add(example);
        }
        return dbQuery;
    }

    /**
     *
     * Add a table to the query.
     *
     * <p>This method adds the DBRow to the list of required (INNER) tables.
     *
     * <p>Criteria (permitted and excluded values) from this instance will be
     * automatically included in the query and an instance of this DBRow class
     * will be created for each DBQueryRow returned.
     *
     * @param tables a list of DBRow objects that defines required tables and
     * criteria
     * @return this DBQuery instance
     */
    public DBQuery add(DBRow... tables) {
        for (DBRow table : tables) {
            queryTables.add(table);
            allQueryTables.add(table);
            results = null;
            resultSQL = null;
        }
        return this;
    }

    /**
     * Add an optional table to this query
     *
     * This method adds an optional (OUTER) table to the query. The query will
     * return an instance of this DBRow for each row found, though it may be a
     * null instance as there was no matching row in the database.
     *
     * Criteria (permitted and excluded values) specified in the supplied
     * instance will be added to the query.
     *
     * @param tables a list of DBRow objects that defines optional tables and
     * criteria
     *
     * @return this DBQuery instance
     */
    public DBQuery addOptional(DBRow... tables) {
        for (DBRow table : tables) {
            optionalQueryTables.add(table.getClass());
            allQueryTables.add(table);
            results = null;
            resultSQL = null;
        }
        return this;
    }

    /**
     *
     * Remove optional or required tables from the query
     *
     * @param tables
     * @return this DBQuery instance
     */
    public DBQuery remove(DBRow... tables) {
        for (DBRow table : tables) {
            queryTables.remove(table);
            optionalQueryTables.remove(table.getClass());
            allQueryTables.remove(table);
            results = null;
            resultSQL = null;
        }
        return this;
    }

    public String getSQLForQuery() throws SQLException {
        return getSQLForQuery(null);
    }

    public String getANSIJoinClause(DBRow newTable, List<DBRow> previousTables, QueryGraph queryGraph) {
        List<String> joinClauses = new ArrayList<String>();
        String lineSep = System.getProperty("line.separator");
        DBDefinition defn = database.getDefinition();
        boolean isLeftOuterJoin = false;
        boolean isFullOuterJoin = false;
        if (queryTables.isEmpty() && optionalQueryTables.size() == allQueryTables.size()) {
            isFullOuterJoin = true;
        } else if (optionalQueryTables.contains(newTable.getClass())) {
            isLeftOuterJoin = true;
        }
        for (DBRow otherTable : previousTables) {
            String join = otherTable.getRelationshipsAsSQL(this.database, newTable, options);
            if (join != null && !join.isEmpty()) {
                joinClauses.add(join);
                queryGraph.add(newTable.getClass(), otherTable.getClass());
            }
        }
        String sqlToReturn;
        if (previousTables.isEmpty()) {
            sqlToReturn = " " + newTable.getTableName() + defn.beginTableAlias() + defn.getTableAlias(newTable) + defn.endTableAlias();
        } else {
            if (isFullOuterJoin) {
                sqlToReturn = lineSep + defn.beginFullOuterJoin();
            } else if (isLeftOuterJoin) {
                sqlToReturn = lineSep + defn.beginLeftOuterJoin();
            } else {
                sqlToReturn = lineSep + defn.beginInnerJoin();
            }
            sqlToReturn += newTable.getTableName() + defn.beginTableAlias() + defn.getTableAlias(newTable) + defn.endTableAlias();
            if (joinClauses.isEmpty()) {
                sqlToReturn += defn.beginOnClause() + defn.getWhereClauseBeginningCondition(options) + defn.endOnClause();
            } else {
                sqlToReturn += defn.beginOnClause();
                String separator = "";
                for (String join : joinClauses) {
                    sqlToReturn += separator + join;
                    separator = defn.beginWhereClauseLine(options);
                }
                sqlToReturn += defn.endOnClause();
            }
        }
        return sqlToReturn;
    }

    private String getSQLForQuery(String providedSelectClause) throws SQLException {

        if (!blankQueryAllowed && willCreateBlankQuery()) {
            throw new AccidentalBlankQueryException();
        }

        DBDefinition defn = database.getDefinition();
        StringBuilder selectClause = new StringBuilder().append(defn.beginSelectStatement());
        StringBuilder fromClause = new StringBuilder().append(defn.beginFromClause());
        List<DBRow> joinedTables = new ArrayList<DBRow>();
        StringBuilder whereClause = new StringBuilder().append(defn.beginWhereClause()).append(defn.getWhereClauseBeginningCondition(options));
        ArrayList<DBRow> otherTables = new ArrayList<DBRow>();
        String lineSep = System.getProperty("line.separator");
        QueryGraph queryGraph = new QueryGraph();

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
            queryGraph.add(tabRow.getClass());

            if (providedSelectClause == null) {
                List<String> columnNames = tabRow.getColumnNames();
                for (String columnName : columnNames) {
                    String formattedColumnName = defn.formatTableAliasAndColumnNameForSelectClause(tabRow, columnName);
                    selectClause.append(colSep).append(formattedColumnName);
                    colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;
                }
            } else {
                selectClause = new StringBuilder(providedSelectClause);
            }
            if (!useANSISyntax) {
                fromClause.append(separator).append(tableName);
            } else {
                fromClause.append(getANSIJoinClause(tabRow, joinedTables, queryGraph));
                joinedTables.add(tabRow);
            }
            List<String> tabRowCriteria = tabRow.getWhereClauses(database, true);
            if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
                for(String clause : tabRowCriteria){
                    whereClause.append(lineSep).append(defn.beginWhereClauseLine(options)).append(clause);
                }
            }

            if (!useANSISyntax) {
                getNonANSIJoin(tabRow, whereClause, defn, queryGraph, joinedTables, tableName, lineSep);
            }

            separator = ", " + lineSep;
            otherTables.addAll(allQueryTables);
        }
        for (DBDataComparison comp : comparisons) {
            whereClause.append(lineSep).append(defn.beginWhereClauseLine(options)).append("(").append(comp.getOperator().generateWhereLine(database, comp.getLeftHandSide().toSQLString(database))).append(")");
        }
        final String sqlString = selectClause.append(lineSep)
                .append(fromClause).append(lineSep)
                .append(whereClause).append(lineSep)
                .append(getOrderByClause()).append(lineSep)
                .append(defn.getLimitRowsSubClauseAfterWhereClause(rowLimit))
                .append(defn.endSQLStatement())
                .toString();
        if (database.isPrintSQLBeforeExecuting()) {
            System.out.println(sqlString);
        }
        if (!cartesianJoinAllowed && allQueryTables.size() > 1 && queryGraph.hasDisconnectedSubgraph()) {
            throw new AccidentalCartesianJoinException();
        }

        return sqlString;
    }

    private void getNonANSIJoin(DBRow tabRow, StringBuilder whereClause, DBDefinition defn, QueryGraph queryGraph, List<DBRow> otherTables, String tableName, String lineSep) {

        for (DBRelationship rel : tabRow.getAdHocRelationships()) {
            whereClause.append(defn.beginWhereClauseLine(options)).append("(").append(rel.generateSQL(database)).append(")");
            queryGraph.add(rel.getFirstTable().getClass(), rel.getSecondTable().getClass());
        }

        final String tabRowPK = tabRow.getPrimaryKeyColumnName();
        if (tabRowPK != null) {
            for (DBRow otherTab : otherTables) {
                List<PropertyWrapper> fks = otherTab.getForeignKeyPropertyWrappers();
                for (PropertyWrapper fk : fks) {
                    String formattedPK = defn.formatTableAliasAndColumnName(tabRow, tabRowPK);
                    Class<? extends DBRow> pkClass = fk.referencedClass();
                    DBRow fkReferencesTable = DBRow.getDBRow(pkClass);
                    String fkReferencesColumn = defn.formatTableAliasAndColumnName(fkReferencesTable, fkReferencesTable.getPrimaryKeyColumnName());
                    if (formattedPK.equalsIgnoreCase(fkReferencesColumn)) {
                        String fkColumnName = fk.columnName();
                        String formattedFK = defn.formatTableAliasAndColumnName(otherTab, fkColumnName);
                        whereClause
                                .append(lineSep)
                                .append(defn.beginWhereClauseLine(options))
                                .append("(")
                                .append(formattedPK)
                                .append(defn.getEqualsComparator())
                                .append(formattedFK)
                                .append(")");
                        queryGraph.add(tabRow.getClass(), otherTab.getClass());
                    }
                }
            }
        }
    }

    public String getSQLForCount() throws SQLException {
        DBDefinition defn = database.getDefinition();
        return getSQLForQuery(defn.beginSelectStatement() + defn.countStarClause());
    }

    public List<DBQueryRow> getAllRows() throws SQLException {
        results = new ArrayList<DBQueryRow>();
        resultSQL = this.getSQLForQuery();
        DBQueryRow queryRow;

        DBStatement dbStatement = database.getDBStatement();
        try {
            ResultSet resultSet = dbStatement.executeQuery(resultSQL);
            try {
                while (resultSet.next()) {
                    queryRow = new DBQueryRow();
                    for (DBRow tableRow : allQueryTables) {
                        DBRow newInstance = DBRow.getDBRow(tableRow.getClass());

                        DBDefinition defn = database.getDefinition();
                        List<PropertyWrapper> newProperties = newInstance.getPropertyWrappers();
                        for (PropertyWrapper newProp : newProperties) {
                            QueryableDatatype qdt = newProp.getQueryableDatatype();
                            String resultSetColumnName = defn.formatColumnNameForDBQueryResultSet(newInstance, newProp.columnName());
                            qdt.setFromResultSet(resultSet, resultSetColumnName);
                        }
                        newInstance.setDefined(true); // Actually came from the database so it is a defined row.
                        Map<String, DBRow> existingInstancesOfThisTableRow = existingInstances.get(tableRow.getClass());
                        if (existingInstancesOfThisTableRow == null) {
                            existingInstancesOfThisTableRow = new HashMap<String, DBRow>();
                            existingInstances.put(newInstance.getClass(), existingInstancesOfThisTableRow);
                        }
                        DBRow existingInstance = newInstance;
                        final PropertyWrapper primaryKey = newInstance.getPrimaryKeyPropertyWrapper();
                        if (primaryKey != null) {
                            final QueryableDatatype qdt = primaryKey.getQueryableDatatype();
                            if (qdt != null) {
                                existingInstance = existingInstancesOfThisTableRow.get(qdt.toSQLString(this.database));
                                if (existingInstance == null) {
                                    existingInstance = newInstance;
                                    existingInstancesOfThisTableRow.put(qdt.toSQLString(this.database), existingInstance);
                                }
                            }
                        }
                        queryRow.put(existingInstance.getClass(), existingInstance);
                    }
                    results.add(queryRow);
                }
            } finally {
                resultSet.close();
            }
        } finally {
            dbStatement.close();
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
     * @param <R> a subclass of DBRow
     * @param exemplar an instance of R
     * @return the ONLY instance found using this query
     * @throws SQLException
     * @throws UnexpectedNumberOfRowsException
     */
    public <R extends DBRow> R getOnlyInstanceOf(R exemplar) throws SQLException, UnexpectedNumberOfRowsException {
        List<R> allInstancesFound = getAllInstancesOf(exemplar, 1);
        return allInstancesFound.get(0);
    }

    /**
     *
     * @param <R>
     * @param exemplar The DBRow class that you would like returned.
     * @param expected The expected number of rows, an exception will be thrown
     * if this expectation is not met.
     * @return a list of all the instances of the exemplar found by this query
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
     * @throws java.sql.SQLException
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
     * @throws java.sql.SQLException
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
     * @throws java.sql.SQLException
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
     * @throws java.sql.SQLException
     */
    public void printAllPrimaryKeys(PrintStream ps) throws SQLException {
        if (needsResults()) {
            this.getAllRows();
        }

        for (DBQueryRow row : this.results) {
            for (DBRow tab : this.allQueryTables) {
                DBRow rowPart = row.get(tab);
                if (rowPart != null) {
                    final QueryableDatatype primaryKey = rowPart.getPrimaryKey();
                    if (primaryKey != null) {
                        String rowPartStr = primaryKey.toSQLString(this.database);
                        ps.print(" " + rowPart.getPrimaryKeyColumnName() + ": " + rowPartStr);
                    }
                }
            }

            ps.println();
        }
    }

    public DBQuery clear() {
        this.queryTables.clear();
        this.optionalQueryTables.clear();
        this.allQueryTables.clear();
        this.comparisons.clear();
        results = null;
        return this;
    }

    public Long count() throws SQLException {
        if (results != null) {
            return new Long(results.size());
        } else {
            Long result = 0L;

            DBStatement dbStatement = database.getDBStatement();
            try {
                ResultSet resultSet = dbStatement.executeQuery(this.getSQLForCount());
                try {
                    while (resultSet.next()) {
                        result = resultSet.getLong(1);
                    }
                } finally {
                    resultSet.close();
                }
            } finally {
                dbStatement.close();
            }
            return result;
        }
    }

    public boolean willCreateBlankQuery() {
        boolean willCreateBlankQuery = true;
        for (DBRow table : allQueryTables) {
            willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(this.database);
        }
        return willCreateBlankQuery && (comparisons.isEmpty());
    }

    public void setRowLimit(int i) {
        rowLimit = new Long(i);
        results = null;

    }

    public void clearRowLimit() {
        rowLimit = null;
        results = null;
    }

    /**
     * Sets the sort order of properties (field and/or method) by the given
     * property object references.
     *
     * <p>
     * For example the following code snippet will sort by just the name column:
     * <pre>
     * Customer customer = ...;
     * query.setSortOrder(customer.column(customer.name));
     * </pre>
     *
     * @param sortColumns
     */
    public void setSortOrder(ColumnProvider... sortColumns) {
        results = null;

        sortOrder = new ArrayList<PropertyWrapper>();
        PropertyWrapper prop;
        for (ColumnProvider col : sortColumns) {
            prop = col.getColumn().getPropertyWrapper();
            if (prop != null) {
                sortOrder.add(prop);
            }
        }
    }

    public void clearSortOrder() {
        sortOrder = null;
    }

    private String getOrderByClause() {
        DBDefinition defn = database.getDefinition();
        if (sortOrder != null) {
            StringBuilder orderByClause = new StringBuilder(defn.beginOrderByClause());
            String sortSeparator = defn.getStartingOrderByClauseSeparator();
            for (PropertyWrapper prop : sortOrder) {
                QueryableDatatype qdt = prop.getQueryableDatatype();
                final String dbColumnName = defn.formatTableAliasAndColumnName(prop.getDBRowInstanceWrapper().adapteeDBRow(), prop.columnName());
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

    public void setBlankQueryAllowed(boolean allow) {
        this.blankQueryAllowed = allow;
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
     * @return the useANSISyntax flag
     */
    public boolean isUseANSISyntax() {
        return useANSISyntax;
    }

    /**
     * @param useANSISyntax the useANSISyntax flag to set
     */
    public void setUseANSISyntax(boolean useANSISyntax) {
        this.useANSISyntax = useANSISyntax;
    }

    public void addAllRelatedTables() throws InstantiationException, IllegalAccessException {
        List<DBRow> tablesToAdd = new ArrayList<DBRow>();
        for (DBRow table : allQueryTables) {
            List<Class<? extends DBRow>> allRelatedTables = table.getAllRelatedTables();
            for (Class<? extends DBRow> relatedTable : allRelatedTables) {
                tablesToAdd.add(relatedTable.newInstance());
            }
        }
        add(tablesToAdd.toArray(new DBRow[]{}));
    }

    public void addAllRelatedTablesAsOptional() throws InstantiationException, IllegalAccessException {
        Set<DBRow> tablesToAdd = new HashSet<DBRow>();
        List<Class<DBRow>> alreadyAddedClasses = new ArrayList<Class<DBRow>>();
        for (DBRow table : allQueryTables) {
            @SuppressWarnings("unchecked")
            Class<DBRow> aClass = (Class<DBRow>) table.getClass();
            alreadyAddedClasses.add(aClass);
        }
        for (DBRow table : allQueryTables) {
            List<Class<? extends DBRow>> allRelatedTables = table.getAllRelatedTables();
            for (Class<? extends DBRow> relatedTable : allRelatedTables) {
                DBRow newInstance = relatedTable.newInstance();
                @SuppressWarnings("unchecked")
                final Class<DBRow> newInstanceClass = (Class<DBRow>) newInstance.getClass();
                if (!alreadyAddedClasses.contains(newInstanceClass)) {
                    tablesToAdd.add(newInstance);
                    alreadyAddedClasses.add(newInstanceClass);
                }
            }
        }
        addOptional(tablesToAdd.toArray(new DBRow[]{}));
    }

    public List<DBQueryRow> getAllRowsContaining(DBRow exemplar) throws SQLException {
        if (this.needsResults()) {
            getAllRows();
        }
        List<DBQueryRow> returnList = new ArrayList<DBQueryRow>();
        for (DBQueryRow row : results) {
            if (row.get(exemplar) == exemplar) {
                returnList.add(row);
            }
        }
        return returnList;
    }

    public void addComparison(DBExpression leftHandSide, DBOperator operatorWithRightHandSideValues) {
        comparisons.add(new DBDataComparison(leftHandSide, operatorWithRightHandSideValues));
        results = null;
    }
    
    /**
     * Set the query to return rows that match any conditions
     * 
     * <p>This means that all permitted*, excluded*, and comparisons are optional 
     * for any rows and rows will be returned if they match any of the conditions.
     * 
     * <p>The conditions will be connected by OR in the SQL.
     */
    public void setToMatchAnyCondition(){
        options.setMatchAny();
        results = null;
    }
    
    
    /**
     * Set the query to only return rows that match all conditions
     * 
     * <p>This is the default state
     * 
     * <p>This means that all permitted*, excluded*, and comparisons are required 
     * for any rows and the conditions will be connected by AND.
     */
    public void setToMatchAllConditions(){
        options.setMatchAll();
        results = null;
    }
}
