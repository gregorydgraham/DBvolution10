/*
 * Copyright 2013 Gregory Graham.
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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.transactions.*;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.QueryOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * DBDatabase is the repository of all knowledge about your database.
 *
 * <p>
 * Available on <a href="https://sourceforge.net/projects/dbvolution/">
 * SourceForge</a> complete with <a
 * href="https://sourceforge.net/p/dbvolution/blog/">BLOG</a>
 *
 * <p>
 * All DBvolution projects need a DBDatabase object to provide the database
 * connection, login details, and to generate the correct syntax for the
 * database.
 *
 * <p>
 * It also provides quick methods to get and print database values and perform
 * transactions.
 *
 * <p>
 * There should be a subclass for your database already in
 * {@code nz.co.gregs.dbvolution.databases}
 *
 * <p>
 * Very few programmers will need to construct an actual DBDatabase as the
 * subclasses provide most of the required details for connecting to databases.
 *
 * @author Gregory Graham
 */
public abstract class DBDatabase {

	private static final Log log = LogFactory.getLog(DBDatabase.class);

	private String driverName = "";
	private String jdbcURL = "";
	private String username = "";
	private String password = null;
	private DataSource dataSource = null;
	private boolean printSQLBeforeExecuting;
	private boolean isInATransaction = false;
	private DBTransactionStatement transactionStatement;
	private final DBDefinition definition;
	private String databaseName;
	private boolean batchIfPossible = true;
	private boolean preventAccidentalDroppingOfTables = true;
	private boolean preventAccidentalDroppingDatabase = true;
	private boolean logSQLBeforeExecuting;
	public int connectionsActive = 0;

	/**
	 * Define a new DBDatabase.
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Check the
	 * subclasses in {@code nz.co.gregs.dbvolution} for your particular
	 * database.
	 *
	 * <p>
	 * DBDatabase encapsulates the knowledge of the database, in particular the
	 * syntax of the database in the DBDefinition and the connection details
	 * from a DataSource.
	 *
	 * @param definition - the subclass of DBDefinition that provides the syntax
	 * for your database.
	 * @param ds - a DataSource for the required database.
	 * @see DBDefinition
	 * @see OracleDB
	 * @see MySQLDB
	 * @see MSSQLServerDB
	 * @see H2DB
	 * @see H2MemoryDB
	 * @see InformixDB
	 * @see PostgresDB
	 */
	public DBDatabase(DBDefinition definition, DataSource ds) {
		this.definition = definition;
		this.dataSource = ds;
	}

	/**
	 * Define a new DBDatabase.
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Check the
	 * subclasses in {@code nz.co.gregs.dbvolution} for your particular
	 * database.
	 *
	 * <p>
	 * Create a new DBDatabase by providing the connection details
	 *
	 * @param definition - the subclass of DBDefinition that provides the syntax
	 * for your database.
	 * @param driverName - The name of the JDBC class that is the Driver for
	 * this database.
	 * @param jdbcURL - The JDBC URL to connect to the database.
	 * @param username - The username to login to the database as.
	 * @param password - The users password for the database.
	 * @see DBDefinition
	 * @see OracleDB
	 * @see MySQLDB
	 * @see MSSQLServerDB
	 * @see H2DB
	 * @see H2MemoryDB
	 * @see InformixDB
	 * @see PostgresDB
	 */
	public DBDatabase(DBDefinition definition, String driverName, String jdbcURL, String username, String password) {
		this.definition = definition;
		this.driverName = driverName;
		this.jdbcURL = jdbcURL;
		this.password = password;
		this.username = username;
	}

	private DBTransactionStatement getDBTransactionStatement() throws SQLException {
		final DBStatement dbStatement = getDBStatement();
		if (dbStatement instanceof DBTransactionStatement) {
			return (DBTransactionStatement) dbStatement;
		} else {
			return new DBTransactionStatement(this, dbStatement);
		}
	}

	/**
	 * Retrieve the DBStatement used internally.
	 *
	 * <p>
	 * DBStatement is the internal version of {@code java.sql.Statement}
	 *
	 * <p>
	 * However you will not need a DBStatement to use DBvolution. Your path lies
	 * elsewhere.
	 *
	 * @return the DBStatement to be used: either a new one, or the current
	 * transaction statement.
	 */
	public synchronized DBStatement getDBStatement() {
		Connection connection;
		DBStatement statement;
		if (isInATransaction) {
			statement = this.transactionStatement;
		} else {
			connection = getConnection();
			try {
				statement = new DBStatement(this, connection);
			} catch (SQLException noConnection) {
				try {
					connection.close();
				} catch (SQLException ex) {
					Logger.getLogger(DBDatabase.class.getName()).log(Level.SEVERE, null, ex);
				}
				throw new RuntimeException("Unable to create a Statement: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + getJdbcURL() + " USERNAME=" + getUsername(), noConnection);
			}
		}
		return statement;
	}

	protected synchronized Connection getConnection() throws RuntimeException {
		Connection connection;
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
		connectionsActive++;
		return connection;
	}

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException
	 */
	public final DBActionList insert(DBRow... listOfRowsToInsert) throws SQLException {
		DBActionList changes = new DBActionList();
		for (DBRow row : listOfRowsToInsert) {
			changes.addAll(this.getDBTable(row).insert(row));
		}
		return changes;
	}

	/**
	 *
	 * Inserts DBRows and Lists of DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a List of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException
	 */
	public final DBActionList insert(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException {
		DBActionList changes = new DBActionList();
		if (listOfRowsToInsert.size() > 0) {
			for (DBRow row : listOfRowsToInsert) {
				changes.addAll(this.getDBTable(row).insert(row));
			}
		}
		return changes;
	}

	/**
	 *
	 * Deletes DBRows from the correct tables automatically
	 *
	 * @param rows a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException
	 */
	public final DBActionList delete(DBRow... rows) throws SQLException {
		DBActionList changes = new DBActionList();
		for (DBRow row : rows) {
			changes.addAll(this.getDBTable(row).delete(row));
		}
		return changes;
	}

	/**
	 *
	 * Deletes Lists of DBRows from the correct tables automatically
	 *
	 * @param list a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException
	 */
	public final DBActionList delete(Collection<? extends DBRow> list) throws SQLException {
		DBActionList changes = new DBActionList();
		if (list.size() > 0) {
			for (DBRow row : list) {
				changes.addAll(this.getDBTable(row).delete(row));
			}
		}
		return changes;
	}

	/**
	 *
	 * Updates DBRows and Lists of DBRows in the correct tables automatically.
	 *
	 * Updated rows are marked as updated, and can be used as though they have
	 * been freshly retrieved from the database.
	 *
	 * @param rows a list of DBRows
	 * @return a DBActionList of the actions performed on the database
	 * @throws SQLException
	 */
	public final DBActionList update(DBRow... rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			actions.addAll(this.getDBTable(row).update(row));
		}
		return actions;
	}

	/**
	 *
	 * Updates Lists of DBRows in the correct tables automatically.
	 *
	 * Updated rows are marked as updated, and can be used as though they have
	 * been freshly retrieved from the database.
	 *
	 * @param listOfRowsToUpdate a List of DBRows
	 * @return a DBActionList of the actions performed on the database
	 * @throws SQLException
	 */
	public final DBActionList update(Collection<? extends DBRow> listOfRowsToUpdate) throws SQLException {
		DBActionList actions = new DBActionList();
		if (listOfRowsToUpdate.size() > 0) {
			for (DBRow row : listOfRowsToUpdate) {
				actions.addAll(this.getDBTable(row).update(row));
			}
		}
		return actions;
	}

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See
	 * {@link nz.co.gregs.dbvolution.DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow)}
	 *
	 * @param <R>
	 * @param exampleRow
	 * @return a list of the selected rows
	 * @throws SQLException
	 */
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException {
		DBTable<R> dbTable = getDBTable(exampleRow);
		return dbTable.getAllRows();
	}

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See
	 * {@link nz.co.gregs.dbvolution.DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow)}
	 *
	 * @param <R>
	 * @param exampleRow
	 * @return a list of the selected rows
	 * @throws SQLException
	 */
	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException {
		return get(exampleRow);
	}

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See {@link DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow, long)}
	 *
	 * @param <R>
	 * @param expectedNumberOfRows
	 * @param exampleRow
	 * @return a list of the selected rows
	 * @throws SQLException
	 * @throws UnexpectedNumberOfRowsException
	 */
	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException {
		if (expectedNumberOfRows == null) {
			return get(exampleRow);
		} else {
			return getDBTable(exampleRow).getRowsByExample(exampleRow, expectedNumberOfRows);
		}
	}

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See {@link DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow, long)}
	 *
	 * @param <R>
	 * @param expectedNumberOfRows
	 * @param exampleRow
	 * @return a list of the selected rows
	 * @throws SQLException
	 * @throws UnexpectedNumberOfRowsException
	 */
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException {
		return get(expectedNumberOfRows, exampleRow);
	}

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * @param rows
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
	public List<DBQueryRow> get(DBRow... rows) throws SQLException {
		DBQuery dbQuery = getDBQuery(rows);
		return dbQuery.getAllRows();
	}

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * @param rows
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
	public List<DBQueryRow> getByExamples(DBRow... rows) throws SQLException {
		return get(rows);
	}

	/**
	 *
	 * Convenience method to print the rows from get(DBRow...)
	 *
	 * @param rows
	 */
	public void print(List<?> rows) {
		if (rows != null) {
			for (Object row : rows) {
				System.out.println(row.toString());
			}
		}
	}

	/**
	 *
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * Will throw a {@link UnexpectedNumberOfRowsException} if the number of
	 * rows found is different from the number expected. See {@link DBQuery#getAllRows(long)
	 * } for further details.
	 *
	 * @param expectedNumberOfRows
	 * @param rows
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * @see DBQuery
	 * @see DBQuery#getAllRows(long)
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
	 * Convenience method to simplify switching from READONLY to COMMITTED
	 * transaction
	 *
	 * @param <V>
	 * @param dbTransaction
	 * @param commit
	 * @return the object returned by the transaction
	 * @throws SQLException
	 * @throws Exception
	 * @see DBTransaction
	 * @see
	 * DBDatabase#doTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 * @see
	 * DBDatabase#doReadOnlyTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 */
	synchronized public <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, Exception {
		if (commit) {
			return doTransaction(dbTransaction);
		} else {
			return doReadOnlyTransaction(dbTransaction);
		}
	}

	/**
	 * Performs the transaction on this database.
	 *
	 * <p>
	 * If there is an exception of any kind the transaction is rolled back and
	 * no changes are made.
	 *
	 * <p>
	 * Otherwise the transaction is committed and changes are made permanent
	 *
	 * @param <V>
	 * @param dbTransaction
	 * @return the object returned by the transaction
	 * @throws SQLException
	 * @throws Exception
	 * @see DBTransaction
	 */
	synchronized public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
		V returnValues = null;
		Connection connection;
		this.transactionStatement = getDBTransactionStatement();
		try {
			this.isInATransaction = true;
			connection = transactionStatement.getConnection();
			connection.setAutoCommit(false);
			try {
				returnValues = dbTransaction.doTransaction(this);
				connection.commit();
				log.info("Transaction Successful: Commit Performed");
				connection.setAutoCommit(true);
			} catch (Exception ex) {
				connection.rollback();
				log.warn("Exception Occurred: ROLLBACK Performed! " + ex.getMessage(), ex);
				throw ex;
			} finally {
				connection.setAutoCommit(true);
				connection.close();
			}
		} finally {
			this.transactionStatement.transactionFinished();
			this.isInATransaction = false;
			transactionStatement = null;
		}
		return returnValues;
	}

	/**
	 * Performs the transaction on this database without making changes.
	 *
	 * <p>
	 * If there is an exception of any kind the transaction is rolled back and
	 * no changes are made.
	 *
	 * <p>
	 * If no exception occurs, the transaction is still rolled back and no
	 * changes are made
	 *
	 * @param <V>
	 * @param dbTransaction
	 * @return the object returned by the transaction
	 * @throws SQLException
	 * @throws Exception
	 * @see DBTransaction
	 */
	synchronized public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
		Connection connection;
		V returnValues = null;
		boolean wasAutoCommit = true;

		this.transactionStatement = getDBTransactionStatement();
		try {
			this.isInATransaction = true;

			connection = transactionStatement.getConnection();
			wasAutoCommit = connection.getAutoCommit();

			connection.setAutoCommit(false);
			try {
				returnValues = dbTransaction.doTransaction(this);
				connection.rollback();
				log.info("Transaction Successful: ROLLBACK Performed");
			} catch (Exception ex) {
				connection.rollback();
				log.warn("Exception Occurred: ROLLBACK Performed! " + ex.getMessage(), ex);
				throw ex;
			} finally {
				connection.setAutoCommit(wasAutoCommit);
				if (!connection.isClosed()) {
					connection.close();
				}
			}
		} finally {
			this.transactionStatement.transactionFinished();
			this.isInATransaction = false;
			transactionStatement = null;
		}
		return returnValues;
	}

	/**
	 * Convenience method to implement a DBScript on this database
	 *
	 * equivalent to script.implement(this);
	 *
	 * @param script
	 * @return a DBActionList provided by the script
	 * @throws Exception
	 */
	public DBActionList implement(DBScript script) throws Exception {
		return script.implement(this);
	}

	/**
	 * Convenience method to test a DBScript on this database
	 *
	 * equivalent to script.test(this);
	 *
	 * @param script
	 * @return a DBActionList provided by the script
	 * @throws Exception
	 */
	public DBActionList test(DBScript script) throws Exception {
		return script.test(this);
	}

	/**
	 * Returns the name of the JDBC driver class used by this DBDatabase
	 * instance.
	 *
	 * @return the driverName
	 */
	public String getDriverName() {
		return driverName;
	}

	/**
	 * Returns the JDBC URL used by this instance, if one has been specified.
	 *
	 * @return the jdbcURL
	 */
	public String getJdbcURL() {
		return jdbcURL;
	}

	/**
	 * Returns the username specified for this DBDatabase instance.
	 *
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Returns the password specified
	 *
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Returns a DBTable instance for the DBRow example.
	 *
	 * <p>
	 * See {@link DBTable DBTable} for more details.
	 *
	 * <p>
	 * Please be aware that DBtable doesn't assume the example's criteria are
	 * important. Use
	 * {@link DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow) getRowsByExample}
	 * to use the criteria on the DBRow.
	 *
	 * @param <R>
	 * @param example
	 * @return a DBTable instance for the example provided
	 */
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return DBTable.getInstance(this, example);
	}

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param examples
	 * @return a DBQuery with the examples as required tables
	 */
	public DBQuery getDBQuery(DBRow... examples) {
		return DBQuery.getInstance(this, examples);
	}

	/**
	 * Enables the printing of all SQL to System.out before the SQL is executed.
	 *
	 * @param b TRUE to print SQL before execution, FALSE otherwise.
	 */
	public void setPrintSQLBeforeExecuting(boolean b) {
		printSQLBeforeExecuting = b;
	}

	/**
	 * Indicates whether SQL will be printed before it is executed.
	 *
	 * @return the printSQLBeforeExecuting
	 */
	public boolean isPrintSQLBeforeExecuting() {
		return printSQLBeforeExecuting;
	}

	/**
	 * Called by internal methods that are about to execute SQL so the SQL can
	 * be printed.
	 *
	 * @param sqlString
	 */
	public void printSQLIfRequested(String sqlString) {
		printSQLIfRequested(sqlString, System.out);
	}

	void printSQLIfRequested(String sqlString, PrintStream out) {
		if (printSQLBeforeExecuting) {
			out.println(sqlString);
		}
	}

	/**
	 * Enables the logging of all SQL before the SQL is executed.
	 *
	 * @deprecated because apparently I don't understand logging frameworks
	 * @param b TRUE to log SQL before execution, FALSE otherwise.
	 */
	@Deprecated
	public void setLogSQLBeforeExecuting(boolean b) {
		logSQLBeforeExecuting = b;
	}

	/**
	 * Indicates whether SQL will be logged before it is executed.
	 *
	 * @deprecated because apparently I don't understand logging frameworks
	 * @return TRUE if the SQL will be logged before execution, otherwise FALSE
	 */
	@Deprecated
	public boolean isLogSQLBeforeExecuting() {
		return logSQLBeforeExecuting;
	}

	/**
	 * Called by internal methods that are about to execute SQL so the SQL can
	 * be logged.
	 *
	 * @deprecated because apparently I don't understand logging frameworks
	 * @param sqlString
	 */
	@Deprecated
	public void logSQLIfRequested(String sqlString) {
		logSQLIfRequested(sqlString, log);
	}

	/**
	 * Called by internal methods that are about to execute SQL so the SQL can
	 * be logged.
	 *
	 * @deprecated because apparently I don't understand logging frameworks
	 * @param sqlString
	 */
	@Deprecated
	void logSQLIfRequested(String sqlString, Log log) {
		if (logSQLBeforeExecuting) {
			log.info(sqlString);
		}
	}

	/**
	 * Creates a table on the database based on the DBRow.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates an actual table on
	 * the database using the default data types supplied by the fields of the
	 * DBRow.
	 *
	 * @param newTableRow
	 * @throws SQLException
	 * @throws AutoCommitActionDuringTransactionException
	 */
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		preventDDLDuringTransaction("DBDatabase.createTable()");
		StringBuilder sqlScript = new StringBuilder();
		List<PropertyWrapper> pkFields = new ArrayList<PropertyWrapper>();
		String lineSeparator = System.getProperty("line.separator");
		// table name

		sqlScript.append(definition.getCreateTableStart()).append(definition.formatTableName(newTableRow)).append(definition.getCreateTableColumnsStart()).append(lineSeparator);

		// columns
		String sep = "";
		String nextSep = definition.getCreateTableColumnsSeparator();
		List<PropertyWrapper> fields = newTableRow.getPropertyWrappers();
		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				String colName = field.columnName();
				sqlScript
						.append(sep)
						.append(definition.formatColumnName(colName))
						.append(definition.getCreateTableColumnsNameAndTypeSeparator())
						.append(definition.getSQLTypeOfDBDatatype(field));
				sep = nextSep + lineSeparator;

				if (field.isPrimaryKey()) {
					pkFields.add(field);
				}
			}
		}

		// primary keys
		String pkStart = lineSeparator + definition.getCreateTablePrimaryKeyClauseStart();
		String pkMiddle = definition.getCreateTablePrimaryKeyClauseMiddle();
		String pkEnd = definition.getCreateTablePrimaryKeyClauseEnd() + lineSeparator;
		String pkSep = pkStart;
		for (PropertyWrapper field : pkFields) {
			sqlScript.append(pkSep).append(definition.formatColumnName(field.columnName()));
			pkSep = pkMiddle;
		}
		if (!pkSep.equalsIgnoreCase(pkStart)) {
			sqlScript.append(pkEnd);
		}

		//finish
		sqlScript.append(definition.getCreateTableColumnsEnd()).append(lineSeparator).append(definition.endSQLStatement());
		String sqlString = sqlScript.toString();
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);
		} finally {
			dbStatement.close();
		}
	}

	/**
	 * Drops a table from the database.
	 *
	 * <p>
	 * In General NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Seriously NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Your DBA will murder you.
	 *
	 * @param tableRow
	 * @throws SQLException
	 * @throws AutoCommitActionDuringTransactionException
	 * @throws AccidentalDroppingOfTableException
	 */
	public void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		preventDDLDuringTransaction("DBDatabase.dropTable()");
		if (preventAccidentalDroppingOfTables) {
			throw new AccidentalDroppingOfTableException();
		}
		StringBuilder sqlScript = new StringBuilder();

		sqlScript.append(definition.getDropTableStart()).append(definition.formatTableName(tableRow)).append(definition.endSQLStatement());
		String sqlString = sqlScript.toString();
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);
		} finally {
			dbStatement.close();
		}
	}

	/**
	 * Drops a table from the database.
	 *
	 * <p>
	 * The easy way to drop a table that might not exist. Will still throw a
	 * AutoCommitActionDuringTransactionException if you use it during a
	 * transaction or AccidentalDroppingOfTableException if dropping tables is
	 * being prevented by DBvolution.
	 * <p>
	 * An even worse idea than {@link #dropTable(nz.co.gregs.dbvolution.DBRow)
	 * }
	 * <
	 * p>In General NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Seriously NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Your DBA will murder you.
	 *
	 * @param <TR>
	 * @param tableRow
	 */
	@SuppressWarnings("empty-statement")
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		try {
			this.dropTable(tableRow);
		} catch (SQLException exp) {
			;
		}
	}

	/**
	 * Returns the DBdefinition used by this DBDatabase
	 *
	 * <p>
	 * Every DBDatabase has a DBDefinition that defines the syntax used in that
	 * database.
	 *
	 * <p>
	 * While DBDefinition is important, unless you are implementing support for
	 * a new database you probably don't need this.
	 *
	 * @return the DBDefinition used by this DBDatabase instance
	 */
	public DBDefinition getDefinition() {
		return definition;
	}

	/**
	 * Returns whether or not the example has any specified criteria.
	 *
	 * See
	 * {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.DBDatabase) willCreateBlankQuery}
	 * on DBRow.
	 *
	 * @param row
	 * @return TRUE if the specified row has no specified criteria, FALSE
	 * otherwise
	 */
	protected boolean willCreateBlankQuery(DBRow row) {
		return row.willCreateBlankQuery(this);
	}

	/**
	 * The worst idea EVAH.
	 *
	 * <p>
	 * Do Not Use This.
	 *
	 * @throws Exception
	 * @throws UnsupportedOperationException
	 * @throws AutoCommitActionDuringTransactionException
	 * @throws AccidentalDroppingOfTableException
	 */
	public void dropDatabase() throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		preventDDLDuringTransaction("DBDatabase.dropDatabase()");
		if (preventAccidentalDroppingOfTables) {
			throw new AccidentalDroppingOfTableException();
		}
		if (preventAccidentalDroppingDatabase) {
			throw new AccidentalDroppingOfDatabaseException();
		}

		String dropStr = getDefinition().getDropDatabase(getDatabaseName());

		printSQLIfRequested(dropStr);
		log.info(dropStr);

		this.doTransaction(new DBRawSQLTransaction(dropStr));
	}

	/**
	 * Returns the database name if one was supplied.
	 *
	 * @return the database name
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * Sets the database name.
	 *
	 * @param databaseName
	 */
	protected void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * Returns whether this DBDatabase will attempt to batch multiple SQL
	 * commands.
	 *
	 * <p>
	 * It is possible to execute several SQL statements in one instruction, and
	 * generally DBvolution attempts to do that when handed several actions at
	 * once.
	 * <p>
	 * However sometimes this is inappropriate and this method can help with
	 * those times.
	 *
	 * @return TRUE if this instance will try to batch SQL statements, FALSE
	 * otherwise
	 */
	public boolean batchSQLStatementsWhenPossible() {
		return batchIfPossible;
	}

	/**
	 * Sets whether this DBDatabase will attempt to batch multiple SQL commands.
	 *
	 * <p>
	 * It is possible to execute several SQL statements in one instruction, and
	 * generally DBvolution attempts to do that when handed several actions at
	 * once.
	 * <p>
	 * However sometimes this is inappropriate and this method can help with
	 * those times.
	 *
	 * @param batchSQLStatementsWhenPossible TRUE if this instance will try to
	 * batch SQL statements, FALSE otherwise
	 */
	public void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		batchIfPossible = batchSQLStatementsWhenPossible;
	}

	void preventDDLDuringTransaction(String message) throws AutoCommitActionDuringTransactionException {
		if (isInATransaction) {
			throw new AutoCommitActionDuringTransactionException(message);
		}
	}

	/**
	 *
	 * @param droppingTablesIsAMistake just leave it at TRUE.
	 */
	public void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		this.preventAccidentalDroppingOfTables = droppingTablesIsAMistake;
	}

	/**
	 *
	 * @param justLeaveThisAtTrue
	 */
	public void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		this.preventAccidentalDroppingDatabase = justLeaveThisAtTrue;
	}

	/**
	 * Indicates whether this database supports full outer joins.
	 *
	 * <p>
	 * Some databases don't yet support queries where all the tables are
	 * optional, that is FULL OUTER joins.
	 *
	 * <p>
	 * This method indicates whether or not this instance can perform full outer
	 * joins.
	 *
	 * <p>
	 * Please note: there are plans to implement full outer joins within DBV for
	 * databases without native support, at which point this method will return
	 * TRUE for all databases. Timing for this implementation is not yet
	 * available.
	 *
	 * @return TRUE if this DBDatabase supports full outer joins , FALSE
	 * otherwise.
	 */
	public boolean supportsFullOuterJoin() {
		return supportsFullOuterJoinNatively();
	}

	/**
	 * Indicates whether this database supports full outer joins natively.
	 *
	 * <p>
	 * Some databases don't yet support queries where all the tables are
	 * optional, that is FULL OUTER joins.
	 *
	 * <p>
	 * This method indicates whether or not this instance can perform full outer
	 * joins.
	 *
	 * @return TRUE if the underlying database supports full outer joins
	 * natively, FALSE otherwise.
	 */
	protected boolean supportsFullOuterJoinNatively() {
		return true;
	}

	/**
	 * Get The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows from the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A>
	 * @param report
	 * @param examples
	 * @return A List of instances of the supplied report from the database
	 * @throws SQLException
	 */
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException {
		return DBReport.getRows(this, report, examples);
	}

	/**
	 * Get The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows from the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A>
	 * @param report
	 * @param examples
	 * @return A List of instances of the supplied report from the database
	 * @throws SQLException
	 */
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException {
		return DBReport.getRows(this, report, examples);
	}

	boolean supportsPaging(QueryOptions options) {
		return definition.supportsPaging(options);
	}

	public synchronized void connectionClosed(Connection connection) {
		connectionsActive--;
	}
}
