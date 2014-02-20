/*
 * Copyright 2014 gregorygraham.
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;

/**
 *
 * @author gregorygraham
 */
public class DBTable<E extends DBRow> {

    private E exemplar = null;
    private final DBDatabase database;
    private DBQuery query = null;
    private QueryOptions options = new QueryOptions();

    protected DBTable(DBDatabase database, E exampleRow) {
        exemplar = exampleRow;
        this.database = database;
        this.query = database.getDBQuery(exampleRow);
    }

    public static <E extends DBRow> DBTable<E> getInstance(DBDatabase database, E example) {
        DBTable<E> dbTable = new DBTable<E>(database, example);
        return dbTable;
    }

    public List<E> getAllRows() throws SQLException {
        query.refreshQuery();
        applyConfigs();
        return query.getAllInstancesOf(exemplar);
    }

    public List<E> toList() throws SQLException {
        return getAllRows();
    }

    public List<E> getRowsByExample(E example) throws SQLException {
        this.exemplar = example;
        this.query = database.getDBQuery(example);
        return getAllRows();
    }

    public E getFirstRow() throws SQLException {
        List<E> allRows = getAllRows();
        return allRows.get(0);
    }

    public E getOnlyRow() throws SQLException, UnexpectedNumberOfRowsException {
        List<E> allRows = getAllRows();
        if (allRows.size() > 1) {
            throw new UnexpectedNumberOfRowsException(1, allRows.size());
        } else {
            return allRows.get(0);
        }
    }

    public E getOnlyRowByExample(E example) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
        return getRowsByExample(example, 1L).get(0);
    }

    public List<E> getRowsByExample(E example, long expectedNumberOfRows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
        List<E> rowsByExample = getRowsByExample(example);
        if (rowsByExample.size() == expectedNumberOfRows) {
            return rowsByExample;
        } else {
            throw new UnexpectedNumberOfRowsException(expectedNumberOfRows, rowsByExample.size());
        }
    }

    private List<E> getRowsByPrimaryKeyObject(Object pkValue) throws SQLException {
        exemplar.clear();
        exemplar.getPrimaryKey().permittedValues(pkValue);
        this.query = database.getDBQuery(exemplar);
        return getAllRows();
    }

    public List<E> getRowsByPrimaryKey(Number pkValue) throws SQLException {
        return getRowsByPrimaryKeyObject(pkValue);
    }

    public List<E> getRowsByPrimaryKey(String pkValue) throws SQLException {
        return getRowsByPrimaryKeyObject(pkValue);
    }

    public List<E> getRowsByPrimaryKey(Date pkValue) throws SQLException {
        return getRowsByPrimaryKeyObject(pkValue);
    }

    public String getSQLForQuery() throws SQLException {
        return query.getSQLForQuery();
    }

    public String getSQLForCount() throws SQLException {
        return query.getSQLForCount();
    }

    public void print() throws SQLException {
        print(System.out);
    }

    public void print(PrintStream stream) throws SQLException {
        List<E> allRows = getAllRows();
        for (E row : allRows) {
            stream.println(row);
        }
    }

    public final DBActionList insert(E... newRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : newRows) {
            actions.addAll(DBInsert.save(database, row));
        }
        query.refreshQuery();
        return actions;
    }

    public DBActionList insert(List<E> newRows) throws SQLException {
        DBActionList changes = new DBActionList();
        for (DBRow row : newRows) {
            changes.addAll(DBInsert.save(database, row));
        }
        query.refreshQuery();
        return changes;
    }

    //@SafeVarargs
    public final DBActionList delete(E... oldRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : oldRows) {
            actions.addAll(DBDelete.delete(database, row));
        }
        query.refreshQuery();
        return actions;
    }

    /**
     * Deletes the rows from the database permanently.
     *
     * @param oldRows
     * @return a {@link DBActionList} of the delete actions.
     * @throws SQLException
     */
    public DBActionList delete(List<E> oldRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : oldRows) {
            actions.addAll(DBDelete.delete(database, row));
        }
        query.refreshQuery();
        return actions;
    }

    public DBActionList update(E oldRow) throws SQLException {
        return DBUpdate.update(database, oldRow);
    }

    public DBActionList update(List<E> oldRows) throws SQLException {
        DBActionList changes = new DBActionList();
        for (E row : oldRows) {
            if (row.hasChangedSimpleTypes()) {
                changes.addAll(DBUpdate.update(database, row));
            }
        }
        query.refreshQuery();
        return changes;
    }

    public List<Long> getPrimaryKeysAsLong() throws SQLException {
        List<E> allRows = getAllRows();
        List<Long> longPKs = new ArrayList<Long>();
        for (E row : allRows) {
            QueryableDatatype primaryKey = row.getPrimaryKey();
            if (DBNumber.class.isAssignableFrom(primaryKey.getClass())) {
                DBNumber num = (DBNumber) primaryKey;
                longPKs.add(num.longValue());
            }
        }
        return longPKs;
    }

    public List<String> getPrimaryKeysAsString() throws SQLException {
        List<E> allRows = getAllRows();
        List<String> stringPKs = new ArrayList<String>();
        for (E row : allRows) {
            stringPKs.add(row.getPrimaryKey().stringValue());
        }
        return stringPKs;
    }

    /**
     * Compares 2 tables, presumably from different criteria or databases prints
     * the differences to System.out
     *
     * Should be updated to return the varying rows somehow
     *
     * @param secondTable : a comparable table
     * @throws java.sql.SQLException
     */
    public void compare(DBTable<E> secondTable) throws SQLException {
        HashMap<String, E> secondMap = new HashMap<String, E>();
        for (E row : secondTable.getAllRows()) {
            secondMap.put(row.getPrimaryKey().toString(), row);
        }
        for (E row : this.getAllRows()) {
            E foundRow = secondMap.get(row.getPrimaryKey().toString());
            if (foundRow == null) {
                System.out.println("NOT FOUND: " + row);
            } else if (!row.toString().equals(foundRow.toString())) {
                System.out.println("DIFFERENT: " + row);
                System.out.println("         : " + foundRow);
            }
        }
    }

    public DBTable<E> setRowLimit(int i) {
        this.options.setRowLimit(new Long(i));
        return this;
    }

    private DBTable<E> applyRowLimit() {
        if (options.getRowLimit() != null) {
            query.setRowLimit(options.getRowLimit());
        } else {
            query.clearRowLimit();
        }
        return this;
    }

    public DBTable<E> clearRowLimit() {
        this.options.setRowLimit(null);
        return this;
    }

    /**
     * Sets the sort order of properties (field and/or method) by the given
     * property object references.
     *
     * <p>
     * For example the following code snippet will sort by just the name column:
     * <pre>
     * Customer customer = ...;
     * customer.setSortOrder(customer, customer.name);
     * </pre>
     *
     * <p>
     * Requires that all {@literal orderColumns} be from the {@code baseRow}
     * instance to work.
     *
     *
     * @param sortColumns
     * @return this
     */
    public DBTable<E> setSortOrder(ColumnProvider... sortColumns) {
        this.options.setSortColumns(sortColumns);
        return this;
    }

    public DBTable<E> clearSortOrder() {
        if (this.options.getSortColumns().length > 0) {
            this.options.setSortColumns(new ColumnProvider[]{});
        }
        return this;
    }

    private void applySortOrder() {
        if (options.getSortColumns().length > 0) {
            this.query.setSortOrder(options.getSortColumns());
        } else {
            query.clearSortOrder();
        }
    }

    public DBTable<E> setBlankQueryAllowed(boolean allow) {
        this.options.setBlankQueryAllowed(allow);
        return this;
    }

    private void applyBlankQueryAllowed() {
        this.query.setBlankQueryAllowed(options.isBlankQueryAllowed());
    }

    private void applyConfigs() {
        applyBlankQueryAllowed();
        applyRowLimit();
        applySortOrder();
        applyMatchAny();
    }

    /**
     * Set the query to return rows that match any conditions
     *
     * <p>
     * This means that all permitted*, excluded*, and comparisons are optional
     * for any rows and rows will be returned if they match any of the
     * conditions.
     *
     * <p>
     * The conditions will be connected by OR in the SQL.
     */
    public void setToMatchAnyCondition() {
        this.options.setMatchAny();
    }

    /**
     * Set the query to only return rows that match all conditions
     *
     * <p>
     * This is the default state
     *
     * <p>
     * This means that all permitted*, excluded*, and comparisons are required
     * for any rows and the conditions will be connected by AND.
     */
    public void setToMatchAllConditions() {
        options.setMatchAll();
    }

    private void applyMatchAny() {
        if (options.isMatchAny()) {
            query.setToMatchAnyCondition();
        } else if (options.isMatchAll()) {
            query.setToMatchAllConditions();
        }
    }
}
