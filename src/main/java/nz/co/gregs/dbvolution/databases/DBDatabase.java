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
package nz.co.gregs.dbvolution.databases;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBTransaction;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;

/**
 *
 * @author gregory.graham
 */
public abstract class DBDatabase {

    private String driverName = "";
    private String jdbcURL = "";
    private String username = "";
    private String password = null;
    private DataSource dataSource = null;
    private boolean printSQLBeforeExecuting;
    private boolean isInATransaction;
    private Statement transactionStatement;
    private final DBDefinition definition;

    /**
     *
     * @param definition
     * @param ds
     */
    public DBDatabase(DBDefinition definition, DataSource ds) {
        this.definition = definition;
        this.dataSource = ds;
    }

    /**
     *
     * @param definition
     * @param driverName
     * @param jdbcURL
     * @param username
     * @param password
     */
    public DBDatabase(DBDefinition definition, String driverName, String jdbcURL, String username, String password) {
        this.definition = definition;
        this.driverName = driverName;
        this.jdbcURL = jdbcURL;
        this.password = password;
        this.username = username;
    }

    /**
     *
     * @return
     */
    public synchronized Statement getDBStatement() {
        Connection connection;
        Statement statement;
        if (isInATransaction) {
            statement = this.transactionStatement;
        } else {
            if (this.dataSource == null) {
                try {
                    // load the driver
                    Class.forName(getDriverName());
                } catch (ClassNotFoundException noDriver) {
                    throw new RuntimeException("No Driver Found: please check the driver name is correct and the appropriate libaries have been supplied: DRIVERNAME=" + getDriverName(), noDriver);
                }
                try {
                    connection = DriverManager.getConnection(getJdbcURL(), getUsername(), getPassword());
                } catch (SQLException noConnection) {
                    throw new RuntimeException("Connection Not Established: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + getJdbcURL() + " USERNAME=" + getUsername(), noConnection);
                }
            } else {
                try {
                    connection = dataSource.getConnection();
                } catch (SQLException noConnection) {
                    throw new RuntimeException("Connection Not Established using the DataSource: please check the datasource - " + dataSource.toString(), noConnection);
                }
            }
            try {
                statement = connection.createStatement();
            } catch (SQLException noConnection) {
                throw new RuntimeException("Unable to create a Statement: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + getJdbcURL() + " USERNAME=" + getUsername(), noConnection);
            }
        }
        return statement;
    }

    /**
     *
     * Convenience method to simplify switching from READONLY to COMMITTED
     * transaction
     *
     * @param <V>
     * @param dbTransaction
     * @param commit
     * @return
     * @throws SQLException
     * @throws Exception
     */
    synchronized public <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, Exception {
        if (commit) {
            return doTransaction(dbTransaction);
        } else {
            return doReadOnlyTransaction(dbTransaction);
        }
    }

    /**
     *
     * Inserts DBRows and lists of DBRows into the correct tables automatically
     *
     * @param objs
     * @throws SQLException
     */
    public void insert(Object... objs) throws SQLException {
        for (Object obj : objs) {
            if (obj instanceof List) {
                List<?> list = (List<?>) obj;
                if (list.size() > 0 && list.get(0) instanceof DBRow) {
                    @SuppressWarnings("unchecked")
                    List<DBRow> rowList = (List<DBRow>) list;
                    for (DBRow row : rowList) {
                        this.getDBTable(row).insert(row);
                    }
                }
            } else if (obj instanceof DBRow) {
                DBRow row = (DBRow) obj;
                this.getDBTable(row).insert(row);
            }
        }
    }

    /**
     *
     * Deletes DBRows and lists of DBRows from the correct tables automatically
     *
     * @param objs
     * @throws SQLException
     */
    public void delete(Object... objs) throws SQLException {
        for (Object obj : objs) {
            if (obj instanceof List) {
                List<?> list = (List<?>) obj;
                if (list.size() > 0 && list.get(0) instanceof DBRow) {
                    @SuppressWarnings("unchecked")
                    List<DBRow> rowList = (List<DBRow>) list;
                    for (DBRow row : rowList) {
                        this.getDBTable(row).delete(row);
                    }
                }
            } else if (obj instanceof DBRow) {
                DBRow row = (DBRow) obj;
                this.getDBTable(row).delete(row);
            }
        }
    }

    /**
     *
     * Updates DBRows and lists of DBRows in the correct tables automatically
     *
     * @param objs
     * @throws SQLException
     */
    public void update(Object... objs) throws SQLException {
        for (Object obj : objs) {
            if (obj instanceof List) {
                List<?> list = (List<?>) obj;
                if (list.size() > 0 && list.get(0) instanceof DBRow) {
                    @SuppressWarnings("unchecked")
                    List<DBRow> rowList = (List<DBRow>) list;
                    for (DBRow row : rowList) {
                        this.getDBTable(row).update(row);
                    }
                }
            } else if (obj instanceof DBRow) {
                DBRow row = (DBRow) obj;
                this.getDBTable(row).update(row);
            }
        }
    }

    /**
     *
     * Automatically selects the correct table and returns the selected rows as
     * a list
     *
     * @param rows
     * @throws SQLException
     */
    public <R extends DBRow> List<R> get(R row) throws SQLException {
        DBTable<R> dbTable = getDBTable(row);
        return dbTable.getRowsByExample(row).toList();
    }

    /**
     *
     * Automatically selects the correct table and returns the selected rows as
     * a list
     *
     * @param <R>
     * @param expectedNumberOfRows
     * @param row
     * @return
     * @throws SQLException
     * @throws UnexpectedNumberOfRowsException
     */
    public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R row) throws SQLException, UnexpectedNumberOfRowsException {
        if (expectedNumberOfRows == null) {
            return get(row);
        } else {
            return getDBTable(row).getRowsByExample(row, expectedNumberOfRows.intValue()).toList();
        }
    }

    /**
     *
     * creates a query and fetches the rows automatically
     *
     * @param rows
     * @throws SQLException
     */
    public List<DBQueryRow> get(DBRow... rows) throws SQLException {
        DBQuery dbQuery = getDBQuery(rows);
        return dbQuery.getAllRows();
    }

    /**
     *
     * creates a query and fetches the rows automatically
     *
     * @param rows
     * @throws SQLException
     */
    public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException {
        if (expectedNumberOfRows == null) {
            return get(rows);
        } else {
            return getDBQuery(rows).getAllRows(expectedNumberOfRows);
        }
    }

    /**
     *
     * @param <V>
     * @param dbTransaction
     * @return
     * @throws SQLException
     * @throws Exception
     */
    synchronized public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
        V returnValues = null;
        Connection connection;
        this.transactionStatement = getDBStatement();
        this.isInATransaction = true;
        connection = transactionStatement.getConnection();
        connection.setAutoCommit(false);
        try {
            returnValues = dbTransaction.doTransaction(this);
            connection.commit();
            System.err.println("Transaction Successful: Commit Performed");
            connection.setAutoCommit(true);
            this.isInATransaction = false;
            transactionStatement = null;
        } catch (Exception ex) {
            connection.rollback();
            System.err.println("Exception Occurred: ROLLBACK Performed");
            connection.setAutoCommit(true);
            this.isInATransaction = false;
            transactionStatement = null;
            throw ex;
        }
        return returnValues;
    }

    /**
     *
     * @param <V>
     * @param dbTransaction
     * @return
     * @throws SQLException
     * @throws Exception
     */
    synchronized public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
        Connection connection;
        V returnValues = null;
        boolean wasReadOnly = false;
        boolean wasAutoCommit = true;

        this.transactionStatement = getDBStatement();
        this.isInATransaction = true;

        connection = transactionStatement.getConnection();
        wasReadOnly = connection.isReadOnly();
        wasAutoCommit = connection.getAutoCommit();

        connection.setReadOnly(true);
        connection.setAutoCommit(false);
        try {
            returnValues = dbTransaction.doTransaction(this);
            connection.rollback();
            System.err.println("Transaction Successful: ROLLBACK Performed");
            connection.setAutoCommit(wasAutoCommit);
            connection.setReadOnly(wasReadOnly);
            this.isInATransaction = false;
            transactionStatement = null;
        } catch (Exception ex) {
            connection.rollback();
            System.err.println("Exception Occurred: ROLLBACK Performed");
            connection.setAutoCommit(wasAutoCommit);
            connection.setReadOnly(wasReadOnly);
            this.isInATransaction = false;
            transactionStatement = null;
            throw ex;
        }
        return returnValues;
    }

    /**
     * @return the driverName
     */
    public String getDriverName() {
        return driverName;
    }

    /**
     * @return the jdbcURL
     */
    public String getJdbcURL() {
        return jdbcURL;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     *
     * @param <R>
     * @param example
     * @return
     */
    public <R extends DBRow> DBTable<R> getDBTable(R example) {
        return DBTable.getInstance(this, example);
    }

    /**
     *
     * @param examples
     * @return
     */
    public DBQuery getDBQuery(DBRow... examples) {
        return DBQuery.getInstance(this, examples);
    }

    public void setPrintSQLBeforeExecuting(boolean b) {
        printSQLBeforeExecuting = b;
    }

    /**
     * @return the printSQLBeforeExecuting
     */
    public boolean isPrintSQLBeforeExecuting() {
        return printSQLBeforeExecuting;
    }

    protected void printSQLIfRequested(String sqlString) {
        printSQLIfRequested(sqlString, System.out);
    }

    protected void printSQLIfRequested(String sqlString, PrintStream out) {
        if (printSQLBeforeExecuting) {
            out.println(sqlString);
        }
    }

    /**
     *
     * @param <TR>
     * @param newTableRow
     * @return
     * @throws SQLException
     */
    public <TR extends DBRow> void createTable(TR newTableRow) throws SQLException {
        StringBuilder sqlScript = new StringBuilder();
        List<Field> pkFields = new ArrayList<Field>();
        String lineSeparator = System.getProperty("line.separator");
        // table name

        sqlScript.append(definition.getCreateTableStart()).append(newTableRow.getTableName()).append(definition.getCreateTableColumnsStart()).append(lineSeparator);

        // columns
        String sep = "";
        String nextSep = definition.getCreateTableColumnsSeparator();
        Field[] fields = newTableRow.getClass().getDeclaredFields();
        for (Field field : fields) {
            DBColumn annotation = field.getAnnotation(DBColumn.class);
            if (annotation != null) {
                QueryableDatatype qdt = newTableRow.getQueryableValueOfField(field);
                String colName = annotation.value();
                if (colName == null || colName.isEmpty()) {
                    colName = field.getName();
                }
                sqlScript.append(sep).append(colName).append(definition.getCreateTableColumnsNameAndTypeSeparator()).append(qdt.getSQLDatatype());
                sep = nextSep + lineSeparator;

                DBPrimaryKey pkAnno = field.getAnnotation(DBPrimaryKey.class);
                if (pkAnno != null) {
                    pkFields.add(field);
                }
            }
        }

        // primary keys
        String pkStart = lineSeparator + definition.getCreateTablePrimaryKeyClauseStart();
        String pkMiddle = definition.getCreateTablePrimaryKeyClauseMiddle();
        String pkEnd = definition.getCreateTablePrimaryKeyClauseEnd() + lineSeparator;
        String pkSep = pkStart;
        for (Field field : pkFields) {
            DBColumn annotation = field.getAnnotation(DBColumn.class);
            String colName = annotation.value();
            if (colName == null || colName.isEmpty()) {
                colName = field.getName();
            }
            sqlScript.append(pkSep).append(colName);
            pkSep = pkMiddle;
        }
        if (!pkSep.equalsIgnoreCase(pkStart)) {
            sqlScript.append(pkEnd);
        }

        //finish
        sqlScript.append(definition.getCreateTableColumnsEnd()).append(lineSeparator);
        String sqlString = sqlScript.toString();
        printSQLIfRequested(sqlString);
        getDBStatement().execute(sqlString);
    }

    public <TR extends DBRow> void dropTable(TR tableRow) throws SQLException {
        StringBuilder sqlScript = new StringBuilder();

        sqlScript.append(definition.getDropTableStart()).append(tableRow.getTableName());
        String sqlString = sqlScript.toString();
        printSQLIfRequested(sqlString);
        getDBStatement().execute(sqlString);
    }

    /**
     *
     * The easy way to drop a table that might not exist.
     *
     * @param <TR>
     * @param tableRow
     */
    @SuppressWarnings("empty-statement")
    public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) {
        try {
            this.dropTable(tableRow);
        } catch (Exception exp) {
            ;
        }
    }

    public DBDefinition getDefinition() {
        return definition;
    }

    public boolean willCreateBlankQuery(DBRow row) {
        return row.willCreateBlankQuery(this);
    }
}
