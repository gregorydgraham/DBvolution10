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
import java.util.Date;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBTransaction;
import nz.co.gregs.dbvolution.QueryableDatatype;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;

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

    public DBDatabase(DataSource ds) {
        this.dataSource = ds;
    }

    public DBDatabase(String driverName, String jdbcURL, String username, String password) {
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
                    this.getDBTable(rowList.get(0)).insert(rowList);
                }
            } else if (obj instanceof DBRow) {
                DBRow row = (DBRow) obj;
                this.getDBTable(row).insert(row);
            }
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
            System.err.println("Transaction Ended: ROLLBACK Performed");
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

    public abstract String getDateFormattedForQuery(Date date);

    public String formatColumnName(String columnName) {
        return columnName;
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

        sqlScript.append(getCreateTableStart()).append(newTableRow.getTableName()).append(getCreateTableColumnsStart()).append(lineSeparator);

        // columns
        String sep = "";
        String nextSep = getCreateTableColumnsSeparator();
        Field[] fields = newTableRow.getClass().getDeclaredFields();
        for (Field field : fields) {
            DBColumn annotation = field.getAnnotation(DBColumn.class);
            if (annotation != null) {
                QueryableDatatype qdt = newTableRow.getQueryableValueOfField(field);
                String colName = annotation.value();
                if (colName == null || colName.isEmpty()) {
                    colName = field.getName();
                }
                sqlScript.append(sep).append(colName).append(getCreateTableColumnsNameAndTypeSeparator()).append(qdt.getSQLDatatype());
                sep = nextSep + lineSeparator;

                DBPrimaryKey pkAnno = field.getAnnotation(DBPrimaryKey.class);
                if (pkAnno != null) {
                    pkFields.add(field);
                }
            }
        }

        // primary keys
        String pkStart = lineSeparator + getCreateTablePrimaryKeyClauseStart();
        String pkMiddle = getCreateTablePrimaryKeyClauseMiddle();
        String pkEnd = getCreateTablePrimaryKeyClauseEnd() + lineSeparator;
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
        sqlScript.append(getCreateTableColumnsEnd()).append(lineSeparator);
        String sqlString = sqlScript.toString();
        printSQLIfRequested(sqlString);
        getDBStatement().execute(sqlString);
    }

    public <TR extends DBRow> void dropTable(TR tableRow) throws SQLException {
        StringBuilder sqlScript = new StringBuilder();

        sqlScript.append(getDropTableStart()).append(tableRow.getTableName());
        String sqlString = sqlScript.toString();
        printSQLIfRequested(sqlString);
        getDBStatement().execute(sqlString);
    }

    public String beginStringValue() {
        return "'";
    }

    public String endStringValue() {
        return "'";
    }

    public String beginNumberValue() {
        return "";
    }

    public String endNumberValue() {
        return "";
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

    /**
     *
     * Formats the table and column name pair correctly for this database
     *
     * This should be used for column names in the select query
     *
     * e.g table, column => TABLE.COLUMN
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public String formatTableAndColumnName(String tableName, String columnName) {
        return formatTableName(tableName) + "." + formatColumnName(columnName);
    }

    public String formatTableName(String tableName) {
        return tableName;
    }

    /**
     *
     * Specifies the column alias used within the JDBC ResultSet to identify the
     * column.
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public String formatColumnNameForResultSet(String tableName, String columnName) {
        String formattedName = formatTableAndColumnName(tableName, columnName).replaceAll("\\.", "__");
        return ("_" + formattedName.hashCode()).replaceAll("-", "_");
    }

    public String formatTableAndColumnNameForSelectClause(String tableName, String columnName) {
        return formatTableAndColumnName(tableName, columnName) + " " + formatColumnNameForResultSet(tableName, columnName);
    }

    public String safeString(String toString) {
        return toString.replaceAll("'", "''");
    }

    /**
     *
     * returns the required SQL to begin a line within the Where Clause
     *
     * usually, but not always " and "
     *
     * @return
     */
    public String beginAndLine() {
        return " and ";
    }

    public String getDropTableStart() {
        return "DROP TABLE ";
    }

    public String getCreateTablePrimaryKeyClauseStart() {
        return ",PRIMARY KEY (";
    }

    private String getCreateTablePrimaryKeyClauseMiddle() {
        return ", ";
    }

    private String getCreateTablePrimaryKeyClauseEnd() {
        return ")";
    }

    private String getCreateTableStart() {
        return "CREATE TABLE ";
    }

    private String getCreateTableColumnsStart() {
        return "(";
    }

    private String getCreateTableColumnsSeparator() {
        return ", ";
    }

    private String getCreateTableColumnsNameAndTypeSeparator() {
        return " ";
    }

    private Object getCreateTableColumnsEnd() {
        return ")";
    }

    public String toLowerCase(String string) {
        return " lower(" + string + ")";
    }

    public String beginInsertLine() {
        return "INSERT INTO ";
    }

    public String endInsertLine() {
        return ";";
    }

    public String beginInsertColumnList() {
        return "(";
    }

    public String endInsertColumnList() {
        return ") ";
    }

    public String beginDeleteLine() {
        return "DELETE FROM ";
    }

    public String endDeleteLine() {
        return ";";
    }

    public String getEqualsComparator() {
        return " = ";
    }

    public String beginWhereClause() {
        return " WHERE ";
    }

    public String beginUpdateLine() {
        return "UPDATE ";
    }

    public String beginSetClause() {
        return " SET ";
    }

    public String getStartingSetSubClauseSeparator() {
        return "";
    }

    public String getSubsequentSetSubClauseSeparator() {
        return ",";
    }

    public String getStartingOrderByClauseSeparator() {
        return "";
    }

    public String getSubsequentOrderByClauseSeparator() {
        return ",";
    }

    public String getFalseOperation() {
        return " 1=0 ";
    }

    public String getTrueOperation() {
        return " 1=1 ";
    }

    public String getNull() {
        return " NULL ";
    }

    public String beginSelectStatement() {
        return " SELECT ";
    }

    public String beginFromClause() {
        return " FROM ";
    }

    public Object endSQLStatement() {
        return ";";
    }

    public String getStartingSelectSubClauseSeparator() {
        return "";
    }

    public String getSubsequentSelectSubClauseSeparator() {
        return ", ";
    }

    public String countStarClause() {
        return " COUNT(*) ";
    }

    /**
     *
     * @param rowLimit
     * @return
     */
    public Object getTopClause(Long rowLimit) {
        return " TOP " + rowLimit + " ";
    }

    public String beginOrderByClause() {
        return " ORDER BY ";
    }

    public String endOrderByClause() {
        return " ";
    }

    public Object getOrderByDirectionClause(Boolean sortOrder) {
        if (sortOrder == null) {
            return "";
        } else if (sortOrder) {
            return " ASC ";
        } else {
            return " DESC ";
        }
    }
}
