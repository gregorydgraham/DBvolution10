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

import java.beans.IntrospectionException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.QueryableDatatype;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;

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
    public Statement getDBStatement() {
        Connection connection;
        Statement statement;
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
        return statement;
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
        out.println(sqlString);
    }

    public abstract String getDateFormattedForQuery(Date date);

    public String formatColumnName(String columnName) {
        return columnName;
    }

    /**
     *
     * @param <TR>
     * @param marque
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     * @throws SQLException
     */
    public <TR extends DBTableRow> void createTable(TR marque) throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException {
        StringBuilder sqlScript = new StringBuilder();
        List<Field> pkFields = new ArrayList<Field>();
        String lineSeparator = System.getProperty("line.separator");
        // table name

        sqlScript.append(getCreateTableStart()).append(marque.getTableName()).append(getCreateTableColumnsStart()).append(lineSeparator);

        // columns
        String sep = "";
        String nextSep = getCreateTableColumnsSeparator();
        Field[] fields = marque.getClass().getDeclaredFields();
        for (Field field : fields) {
            DBTableColumn annotation = field.getAnnotation(DBTableColumn.class);
            if (annotation != null) {
                QueryableDatatype qdt = marque.getQueryableValueOfField(field);
                String colName = annotation.value();
                if (colName == null || colName.isEmpty()) {
                    colName = field.getName();
                }
                sqlScript.append(sep).append(colName).append(getCreateTableColumnsNameAndTypeSeparator()).append(qdt.getSQLDatatype());
                sep = nextSep + lineSeparator;

                DBTablePrimaryKey pkAnno = field.getAnnotation(DBTablePrimaryKey.class);
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
            DBTableColumn annotation = field.getAnnotation(DBTableColumn.class);
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

    public <TR extends DBTableRow> void dropTable(TR tableRow) throws SQLException {
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

    @SuppressWarnings("empty-statement")
    public <TR extends DBTableRow> void dropTableNoExceptions(TR tableRow) {
        try {
            this.dropTable(tableRow);
        } catch (Exception exp) {;
        }
    }

    /**
     *
     * Formats the table and column name pair correctly for this database
     *
     * e.g table, column => TABLE.COLUMN
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public String formatTableAndColumnName(String tableName, String columnName) {
        return tableName + "." + columnName;
    }

    public String formatColumnNameForResultSet(String tableName, String columnName) {
        return formatTableAndColumnName(tableName, columnName);
    }

    public String safeString(String toString) {
        return toString.replaceAll("'", "''");
    }

    public String beginWhereLine() {
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

}
