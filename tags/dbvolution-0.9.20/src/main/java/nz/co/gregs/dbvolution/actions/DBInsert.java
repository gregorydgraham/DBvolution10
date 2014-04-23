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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Provides support for the abstract concept of inserting rows.
 *
 * @author gregorygraham
 */
public class DBInsert extends DBAction {

    private transient StringBuilder allColumns;
    private transient StringBuilder allValues;
    public List<Long> generatedKeys = new ArrayList<Long>();

    protected <R extends DBRow> DBInsert(R row) {
        super(row);
    }

    /**
     * Saves the row to the database.
     *
     * Creates the appropriate actions to save the row permanently in the
     * database and executes them.
     * <p>
     * Supports automatic retrieval of the new primary key in limited cases:
     * <ul>
     * <li>If the database supports generated keys, </li>
     * <li> and the primary key has not been set, </li>
     * <li>and there is exactly one generated key</li>
     * <li>then the primary key will be set to the generated key.</li>
     * </ul>
     *
     * @param database
     * @param row
     * @return a DBActionList of the actions performed on the database.
     * @throws SQLException
     */
    public static DBActionList save(DBDatabase database, DBRow row) throws SQLException {
        DBInsert dbInsert = new DBInsert(row);
        final DBActionList executedActions = dbInsert.execute(database);
        if (dbInsert.generatedKeys.size() == 1 && !row.getPrimaryKey().hasBeenSet()) {
            row.getPrimaryKey().setValue(dbInsert.generatedKeys.get(0));
        }
        return executedActions;
    }

    @Override
    public ArrayList<String> getSQLStatements(DBDatabase db) {
        DBRow row = getRow();
        DBDefinition defn = db.getDefinition();
        processAllFieldsForInsert(db, row);

        ArrayList<String> strs = new ArrayList<String>();
        strs.add(defn.beginInsertLine()
                + defn.formatTableName(row)
                + defn.beginInsertColumnList()
                + allColumns
                + defn.endInsertColumnList()
                + allValues
                + defn.endInsertLine());
        return strs;
    }

    @Override
    protected DBActionList execute(DBDatabase db) throws SQLException {
        final DBDefinition defn = db.getDefinition();
        DBStatement statement = db.getDBStatement();
        DBRow row = getRow();

        DBActionList actions = new DBActionList(new DBInsert(row));
        for (String sql : getSQLStatements(db)) {
            if (defn.supportsGeneratedKeys(null)) {
                try {
                    statement.execute(sql, Statement.RETURN_GENERATED_KEYS);

                    ResultSet generatedKeysResultSet = statement.getGeneratedKeys();
                    try {
                        ResultSetMetaData metaData = generatedKeysResultSet.getMetaData();
                        while (generatedKeysResultSet.next()) {
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                this.getGeneratedKeys().add(generatedKeysResultSet.getLong(1));
                                System.out.println("GENERATED KEYS: " + generatedKeysResultSet.getLong(1));
                            }
                        }
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        generatedKeysResultSet.close();
                    }
                } catch (SQLException sqlex) {
                    try {
                        sqlex.printStackTrace();
                        statement.execute(sql);
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            } else {
                try {
                    statement.execute(sql);
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        DBInsertLargeObjects blobSave = new DBInsertLargeObjects(row);
        actions.addAll(blobSave.execute(db));
        row.setDefined();
        return actions;
    }

    private void processAllFieldsForInsert(DBDatabase database, DBRow row) {
        allColumns = new StringBuilder();
        allValues = new StringBuilder();
        DBDefinition defn = database.getDefinition();
        List<PropertyWrapper> props = row.getPropertyWrappers();
        String columnSeparator = "";
        String valuesSeparator = defn.beginValueClause();
        for (PropertyWrapper prop : props) {
            // BLOBS are not inserted normally so don't include them
            if (prop.isColumn()) {
                final QueryableDatatype qdt = prop.getQueryableDatatype();
                if (!(qdt instanceof DBLargeObject) && qdt.hasBeenSet()) {
                    // nice normal columns
                    // Add the column
                    allColumns
                            .append(columnSeparator)
                            .append(" ")
                            .append(defn.formatColumnName(prop.columnName()));
                    columnSeparator = defn.getValuesClauseColumnSeparator();
                    // add the value
                    allValues.append(valuesSeparator).append(qdt.toSQLString(database));
                    valuesSeparator = defn.getValuesClauseValueSeparator();
                }
            }
        }
        allValues.append(defn.endValueClause());
    }

    @Override
    protected DBActionList getRevertDBActionList() {
        DBActionList reverts = new DBActionList();
        if (this.getRow().getPrimaryKey() == null) {
            reverts.add(new DBDeleteUsingAllColumns(getRow()));
        } else {
            reverts.add(new DBDeleteByPrimaryKey(getRow()));
        }
        return reverts;
    }

    @Override
    protected DBActionList getActions() {//DBRow row) {
        return new DBActionList(new DBInsert(getRow()));
    }

    /**
     * Creates a DBActionList of inserts actions for the rows.
     *
     * <p>
     * The actions created can be applied on a particular database using
     * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
     *
     * @param rows
     * @return a DBActionList of inserts.
     * @throws SQLException
     */
    public static DBActionList getInserts(DBRow... rows) throws SQLException {
        DBActionList inserts = new DBActionList();
        for (DBRow row : rows) {
            inserts.add(new DBInsert(row));
        }
        return inserts;
    }

    /**
     * Returns all generated values created during the insert actions.
     *
     * @return the generatedKeys
     */
    public List<Long> getGeneratedKeys() {
        return generatedKeys;
    }
}
