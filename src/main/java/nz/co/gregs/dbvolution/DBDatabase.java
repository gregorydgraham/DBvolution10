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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
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
public abstract class DBDatabase implements Cloneable {

	private static final Log LOG = LogFactory.getLog(DBDatabase.class);

	private String driverName = "";
	private String jdbcURL = "";
	private String username = "";
	private String password = null;
	private DataSource dataSource = null;
	private boolean printSQLBeforeExecuting = false;
	private boolean isInATransaction = false;
	private DBTransactionStatement transactionStatement;
	private DBDefinition definition = null;
	private String databaseName;
	private boolean batchIfPossible = true;
	private boolean preventAccidentalDroppingOfTables = true;
	private boolean preventAccidentalDroppingDatabase = true;
//	private int connectionsActive = 0;
	private final Object getStatementSynchronizeObject = new Object();
	private final Object getConnectionSynchronizeObject = new Object();
	private Connection transactionConnection;
	private static final transient Map<DBDatabase, List<Connection>> busyConnections = new HashMap<DBDatabase, List<Connection>>();
	private static final transient HashMap<DBDatabase, List<Connection>> freeConnections = new HashMap<DBDatabase, List<Connection>>();
	private Boolean needToAddDatabaseSpecificFeatures = true;

	/**
	 * Clones the DBDatabase.
	 *
	 * @return a clone of the DBDatabase.
	 * @throws CloneNotSupportedException not likely
	 */
	@Override
	protected DBDatabase clone() throws CloneNotSupportedException {
		Object clone = super.clone();
		DBDatabase newInstance = (DBDatabase) clone;
		return newInstance;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 29 * hash + (this.driverName != null ? this.driverName.hashCode() : 0);
		hash = 29 * hash + (this.jdbcURL != null ? this.jdbcURL.hashCode() : 0);
		hash = 29 * hash + (this.username != null ? this.username.hashCode() : 0);
		hash = 29 * hash + (this.password != null ? this.password.hashCode() : 0);
		hash = 29 * hash + (this.dataSource != null ? this.dataSource.hashCode() : 0);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final DBDatabase other = (DBDatabase) obj;
		if ((this.driverName == null) ? (other.driverName != null) : !this.driverName.equals(other.driverName)) {
			return false;
		}
		if ((this.jdbcURL == null) ? (other.jdbcURL != null) : !this.jdbcURL.equals(other.jdbcURL)) {
			return false;
		}
		if ((this.username == null) ? (other.username != null) : !this.username.equals(other.username)) {
			return false;
		}
		if ((this.password == null) ? (other.password != null) : !this.password.equals(other.password)) {
			return false;
		}
		return !(this.dataSource != other.dataSource && (this.dataSource == null || !this.dataSource.equals(other.dataSource)));
	}

	/**
	 *
	 * Provides a convenient constructor for DBDatabases that have configuration
	 * details hardwired or are able to automatically retrieve the details.
	 *
	 * <p>
	 * This constructor creates an empty DBDatabase with only the default
	 * settings, in particular with no driver, URL, username, password, or
	 * {@link DBDefinition}
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Check the
	 * subclasses in {@code nz.co.gregs.dbvolution.databases} for your particular
	 * database.
	 *
	 * <p>
	 * DBDatabase encapsulates the knowledge of the database, in particular the
	 * syntax of the database in the DBDefinition and the connection details from
	 * a DataSource.
	 *
	 * @see DBDefinition
	 * @see Oracle12DB
	 * @see Oracle11XEDB
	 * @see OracleAWS11DB
	 * @see MySQLDB
	 * @see MSSQLServerDB
	 * @see H2DB
	 * @see H2MemoryDB
	 * @see InformixDB
	 * @see MariaDB
	 * @see MariaClusterDB
	 * @see NuoDB
	 */
	protected DBDatabase() {
	}

	/**
	 * Define a new DBDatabase.
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Check the
	 * subclasses in {@code nz.co.gregs.dbvolution} for your particular database.
	 *
	 * <p>
	 * DBDatabase encapsulates the knowledge of the database, in particular the
	 * syntax of the database in the DBDefinition and the connection details from
	 * a DataSource.
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
	 * @see MariaDB
	 * @see MariaClusterDB
	 * @see NuoDB
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
	 * subclasses in {@code nz.co.gregs.dbvolution} for your particular database.
	 *
	 * <p>
	 * Create a new DBDatabase by providing the connection details
	 *
	 * @param definition - the subclass of DBDefinition that provides the syntax
	 * for your database.
	 * @param driverName - The name of the JDBC class that is the Driver for this
	 * database.
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
	 * @throws java.sql.SQLException interacts with the database layer.
	 */
	public DBStatement getDBStatement() throws SQLException {
		DBStatement statement;
		synchronized (getStatementSynchronizeObject) {
			if (isInATransaction) {
				statement = this.transactionStatement;
				if (statement.isClosed()) {
					this.transactionStatement = new DBTransactionStatement(this, getLowLevelStatement());
				}
			} else {
				statement = getLowLevelStatement();
			}
		}
		return statement;
	}

	private DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		Connection connection = getConnection();
		try {
			while (connection.isClosed()) {
				discardConnection(connection);
				connection = getConnection();
			}
			return new DBStatement(this, connection);
		} catch (SQLException cantCreateStatement) {
			discardConnection(connection);
			throw new UnableToCreateDatabaseConnectionException(getJdbcURL(), getUsername(), cantCreateStatement);
		}
	}

	/**
	 * Retrieve the Connection used internally.
	 *
	 * <p>
	 * However you will not need a Connection to use DBvolution. Your path lies
	 * elsewhere.
	 *
	 * @return the Connection to be used.
	 * @throws java.sql.SQLException interacts with the database layer
	 * @throws UnableToCreateDatabaseConnectionException thrown when there is an
	 * issue connecting
	 * @throws UnableToFindJDBCDriver may be thrown if the JDBCDriver is not on
	 * the class path. DBvolution includes several JDBCDrivers already but Oracle
	 * and MS SQLserver, in particular, need to be added to the path if you wish
	 * to work with those databases.
	 */
	public Connection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		if (isInATransaction && !this.transactionConnection.isClosed()) {
			return this.transactionConnection;
		}
		Connection conn = null;
		while (conn == null) {
			if (supportsPooledConnections()) {
				synchronized (freeConnections) {
					if (freeConnections.isEmpty() || getConnectionList(freeConnections).isEmpty()) {
						conn = getRawConnection();
					} else {
						conn = getConnectionList(freeConnections).get(0);
					}
				}
			} else {
				conn = getRawConnection();
			}
			try {
				if (conn.isClosed()) {
					discardConnection(conn);
					conn = null;
				}
			} catch (SQLException ex) {
				Logger.getLogger(DBDatabase.class.getName()).log(Level.FINEST, null, ex);
			}
			if (connectionUsedForPersistentConnection(conn)) {
				conn = null;
			}
		}
		usedConnection(conn);
		return conn;
	}

	private Connection getRawConnection() throws UnableToFindJDBCDriver, UnableToCreateDatabaseConnectionException, SQLException {
		Connection connection;
		synchronized (getConnectionSynchronizeObject) {
			if (this.dataSource == null) {
				try {
					// load the driver
					Class.forName(getDriverName());
				} catch (ClassNotFoundException noDriver) {
					throw new UnableToFindJDBCDriver(getDriverName(), noDriver);
				}
				try {
					connection = getConnectionFromDriverManager();
				} catch (SQLException noConnection) {
					throw new UnableToCreateDatabaseConnectionException(getJdbcURL(), getUsername(), noConnection);
				}
			} else {
				try {
					connection = dataSource.getConnection();
				} catch (SQLException noConnection) {
					throw new UnableToCreateDatabaseConnectionException(dataSource, noConnection);
				}
			}
//			connectionOpened(connection);
		}
		synchronized (this) {
			if (needToAddDatabaseSpecificFeatures) {
				addDatabaseSpecificFeatures(connection.createStatement());
				needToAddDatabaseSpecificFeatures = false;
			}
		}
		return connection;
	}

	/**
	 * Used to hold the database open if required by the database.
	 *
	 */
	protected Connection storedConnection;

	private boolean connectionUsedForPersistentConnection(Connection connection) throws DBRuntimeException, SQLException {
		if (storedConnection == null && persistentConnectionRequired()) {
			this.storedConnection = connection;
			this.storedConnection.createStatement();
			return true;
		}
		return false;
	}

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
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
	 * @throws SQLException database exceptions
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
	 * @throws SQLException database exceptions
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
	 * @throws SQLException database exceptions
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
	 * @throws SQLException database exceptions
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
	 * @throws SQLException database exceptions
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
	 * @param <R> the row affected
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
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
	 * @param <R> the table affected
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
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
	 * @param <R> the table affected
	 * @param expectedNumberOfRows throw an exception and abort if this number is
	 * not matched
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException the exception thrown if the number
	 * of rows is wrong
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
	 * @param <R> the table affected
	 * @param expectedNumberOfRows the number of rows required
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException the exception thrown when the
	 * number of rows is not correct
	 */
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException {
		return get(expectedNumberOfRows, exampleRow);
	}

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * @param rows the examples of the rows required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
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
	 * @param rows the example rows for the tables required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
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
	 * @param rows lists of DBRows, DBReports, or DBQueryRows
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
	 * Will throw a {@link UnexpectedNumberOfRowsException} if the number of rows
	 * found is different from the number expected. See {@link DBQuery#getAllRows(long)
	 * } for further details.
	 *
	 * @param expectedNumberOfRows the number of rows required
	 * @param rows examples of the tables required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException thrown when the retrieved row count
	 * is wrong
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
	 * @param <V> the return type of the transaction, can be anything
	 * @param dbTransaction the transaction to execute
	 * @param commit commit=true or rollback=false.
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws Exception any exception thrown by the transactions code
	 * @see DBTransaction
	 * @see
	 * DBDatabase#doTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 * @see
	 * DBDatabase#doReadOnlyTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 */
	public <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, Exception {
		DBDatabase db;
		synchronized (this) {
			db = this.clone();
		}
		V returnValues = null;
		db.transactionStatement = db.getDBTransactionStatement();
		try {
			db.isInATransaction = true;
			db.transactionConnection = db.transactionStatement.getConnection();
//			boolean wasAutoCommit = db.transactionConnection.getAutoCommit();
			db.transactionConnection.setAutoCommit(false);
			try {
				returnValues = dbTransaction.doTransaction(db);
				if (commit) {
					db.transactionConnection.commit();
					LOG.info("Transaction Successful: Commit Performed");
				} else {
					try {
						db.transactionConnection.rollback();
						LOG.info("Transaction Successful: ROLLBACK Performed");
					} catch (SQLException rollbackFailed) {
						System.out.println("ROLLBACK FAILED");
//						rollbackFailed.printStackTrace();
						System.out.println("CONTINUING REGARDLESS");
						discardConnection(db.transactionConnection);
					}
				}
			} catch (Exception ex) {
				try {
					LOG.warn("Exception Occurred: Attempting ROLLBACK - " + ex.getMessage(), ex);
					db.transactionConnection.rollback();
					LOG.warn("Exception Occurred: ROLLBACK Succeeded!");
				} catch (Exception excp) {
					LOG.warn("Exception Occurred During Rollback: " + ex.getMessage(), excp);
				}
				throw ex;
			}
		} finally {
			db.isInATransaction = false;
			db.transactionStatement.transactionFinished();
			discardConnection(db.transactionConnection);
			db.transactionConnection = null;
			db.transactionStatement = null;
		}
		return returnValues;
	}

	/**
	 * Performs the transaction on this database.
	 *
	 * <p>
	 * If there is an exception of any kind the transaction is rolled back and no
	 * changes are made.
	 *
	 * <p>
	 * Otherwise the transaction is committed and changes are made permanent
	 *
	 * @param <V> the return type of the transaction
	 * @param dbTransaction the transaction to execute
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws Exception any other exception thrown by the transaction
	 * @see DBTransaction
	 */
	public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
		return doTransaction(dbTransaction, true);
	}

	/**
	 * Performs the transaction on this database without making changes.
	 *
	 * <p>
	 * If there is an exception of any kind the transaction is rolled back and no
	 * changes are made.
	 *
	 * <p>
	 * If no exception occurs, the transaction is still rolled back and no changes
	 * are made
	 *
	 * @param <V> the return type of the transaction
	 * @param dbTransaction the transaction to execute
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws Exception any other exception
	 * @see DBTransaction
	 */
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
		return doTransaction(dbTransaction, false);
	}

	/**
	 * Convenience method to implement a DBScript on this database
	 *
	 * equivalent to script.implement(this);
	 *
	 * @param script the script to execute and commit
	 * @return a DBActionList provided by the script
	 * @throws Exception any exception can be thrown by a DBScript
	 */
	public DBActionList implement(DBScript script) throws Exception {
		return script.implement(this);
	}

	/**
	 * Convenience method to test a DBScript on this database
	 *
	 * equivalent to script.test(this);
	 *
	 * @param script the script to executed and rollback
	 * @return a DBActionList provided by the script
	 * @throws Exception DBScripts can throw any exception at any time
	 */
	public DBActionList test(DBScript script) throws Exception {
		return script.test(this);
	}

	/**
	 * Returns the name of the JDBC driver class used by this DBDatabase instance.
	 *
	 * @return the driverName
	 */
	public String getDriverName() {
		return driverName;
	}

	/**
	 * Sets the name of the JDBC driver class used by this DBDatabase instance.
	 *
	 * @param driver the name of the JDBC Drive class for this DBDatabase.
	 */
	protected void setDriverName(String driver) {
		driverName = driver;
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
	 * @param <R> the table affected
	 * @param example the example row to use in the query
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
	 * @param examples the example rows that are required in the query
	 * @return a DBQuery with the examples as required tables
	 */
	public DBQuery getDBQuery(DBRow... examples) {
		return DBQuery.getInstance(this, examples);
	}

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param examples the example rows that are required in the query
	 * @return a DBQuery with the examples as required tables
	 */
	public DBQuery getDBQuery(List<DBRow> examples) {
		DBRow[] toArray = examples.toArray(new DBRow[]{});
		return DBQuery.getInstance(this, toArray);
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
	 * Called by internal methods that are about to execute SQL so the SQL can be
	 * printed.
	 *
	 * @param sqlString the raw SQL to print
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
	 * Creates a table on the database based on the DBRow.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates an actual table on
	 * the database using the default data types supplied by the fields of the
	 * DBRow.
	 *
	 * @param newTableRow the table to create
	 * @throws SQLException database exceptions
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		createTable(newTableRow, false);
	}

	/**
	 * Creates a table on the database based on the DBRow, and creates the
	 * required database foreign key constraints.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates an actual table on
	 * the database using the default data types supplied by the fields of the
	 * DBRow.
	 *
	 * <p>
	 * DBvolution does not require actual foreign keys constraints to exist in the
	 * database but there are some advantages in terms of data integrity and
	 * schema transparency.
	 *
	 * <p>
	 * Unfortunately there are also problems caused by creating foreign key
	 * constraints: insertion order sensitivity for instance.
	 *
	 * <p>
	 * Personally I prefer the foreign keys to exist, however database constraints
	 * have been described as the "ambulance at the bottom of the cliff" so you
	 * might be better off without them.
	 *
	 * @param newTableRow table
	 * @throws SQLException database exceptions
	 *
	 */
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		createTable(newTableRow, true);
	}

	private void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		preventDDLDuringTransaction("DBDatabase.createTable()");
		StringBuilder sqlScript = new StringBuilder();
		List<PropertyWrapper> pkFields = new ArrayList<PropertyWrapper>();
		List<PropertyWrapper> spatial2DFields = new ArrayList<PropertyWrapper>();
		String lineSeparator = System.getProperty("line.separator");
		// table name

		sqlScript.append(definition.getCreateTableStart()).append(definition.formatTableName(newTableRow)).append(definition.getCreateTableColumnsStart()).append(lineSeparator);

		// columns
		String sep = "";
		String nextSep = definition.getCreateTableColumnsSeparator();
		List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
		List<String> fkClauses = new ArrayList<String>();
		for (PropertyWrapper field : fields) {
			if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
				String colName = field.columnName();
				sqlScript
						.append(sep)
						.append(definition.formatColumnName(colName))
						.append(definition.getCreateTableColumnsNameAndTypeSeparator())
						.append(definition.getSQLTypeAndModifiersOfDBDatatype(field));
				sep = nextSep + lineSeparator;

				if (field.isPrimaryKey()) {
					pkFields.add(field);
				}
				if (field.isSpatial2DType()) {
					spatial2DFields.add(field);
				}
				String fkClause = definition.getForeignKeyClauseForCreateTable(field);
				if (!fkClause.isEmpty()) {
					fkClauses.add(fkClause);
				}
			}
		}

		if (includeForeignKeyClauses) {
			for (String fkClause : fkClauses) {
				sqlScript.append(sep).append(fkClause);
				sep = nextSep + lineSeparator;
			}
		}

		// primary keys
		if (definition.prefersTrailingPrimaryKeyDefinition()) {
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
		}

		//finish
		sqlScript.append(definition.getCreateTableColumnsEnd()).append(lineSeparator).append(definition.endSQLStatement());
		String sqlString = sqlScript.toString();
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);

			//Oracle style trigger based auto-increment keys
			if (definition.prefersTriggerBasedIdentities() && pkFields.size() == 1) {
				List<String> triggerBasedIdentitySQL = definition.getTriggerBasedIdentitySQL(this, definition.formatTableName(newTableRow), definition.formatColumnName(pkFields.get(0).columnName()));
				for (String sql : triggerBasedIdentitySQL) {
					dbStatement.execute(sql);
				}
			}

			if (definition.requiresSpatial2DIndexes() && spatial2DFields.size() > 0) {
				List<String> triggerBasedIdentitySQL = definition.getSpatial2DIndexSQL(this, definition.formatTableName(newTableRow), definition.formatColumnName(spatial2DFields.get(0).columnName()));
				for (String sql : triggerBasedIdentitySQL) {
					dbStatement.execute(sql);
				}
			}

		} finally {
			dbStatement.close();
		}
	}

	/**
	 * Adds actual foreign key constraints to the database table represented by
	 * the supplied DBRow.
	 *
	 * <p>
	 * While database theory stipulates that foreign keys should be represented by
	 * a constraint on the table, this is not part of the industry standard.
	 * DBvolution allows for the creation of these constraints through this
	 * method.
	 *
	 * <p>
	 * All databases support adding FK constraints, and they provide useful
	 * checks. However they are the last possible check, represent an inadequate
	 * protection, and can cause considerable difficulties at surprising times. I
	 * recommend against them.
	 *
	 * @param newTableRow the table that needs foreign key constraints
	 * @throws SQLException the database has had an issue.
	 */
	public void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {

		List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
		List<String> fkClauses = new ArrayList<String>();
		for (PropertyWrapper field : fields) {
			if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
				final String alterTableAddForeignKeyStatement = definition.getAlterTableAddForeignKeyStatement(newTableRow, field);
				if (!alterTableAddForeignKeyStatement.isEmpty()) {
					fkClauses.add(alterTableAddForeignKeyStatement);
				}
			}
		}
		if (fkClauses.size() > 0) {
			final DBStatement statement = getDBStatement();
			try {
				for (String fkClause : fkClauses) {
					statement.execute(fkClause);
				}
			} finally {
				statement.close();
			}
		}
	}

	/**
	 * Drops All Foreign Key Constraints From The Supplied Table, does not affect
	 * &#64;DBForeignKey.
	 *
	 * <p>
	 * Generates and executes the required SQL to remove all foreign key
	 * constraints on this table defined within the database.
	 *
	 * <p>
	 * This methods is supplied as an inverse to
	 * {@link #createForeignKeyConstraints(nz.co.gregs.dbvolution.DBRow)}.
	 *
	 * <p>
	 * If a pair of tables have foreign keys constraints to each other it may be
	 * necessary to remove the constraints to successfully insert some rows.
	 * DBvolution cannot to protect you from this situation, however this method
	 * will remove some of the problem.
	 *
	 * @param newTableRow the data models version of the table that needs FKs
	 * removed
	 * @throws SQLException database exceptions
	 */
	public void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {

		List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
		List<String> fkClauses = new ArrayList<String>();
		for (PropertyWrapper field : fields) {
			if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
				final String alterTableDropForeignKeyStatement = definition.getAlterTableDropForeignKeyStatement(newTableRow, field);
				if (!alterTableDropForeignKeyStatement.isEmpty()) {
					fkClauses.add(alterTableDropForeignKeyStatement);
				}
			}
		}
		if (fkClauses.size() > 0) {
			final DBStatement statement = getDBStatement();
			try {
				for (String fkClause : fkClauses) {
					statement.execute(fkClause);
				}
			} finally {
				statement.close();
			}
		}
	}

	/**
	 * Adds Database Indexes To All Fields Of This Table.
	 *
	 * <p>
	 * Use this method to add indexes to all the columns of the table. This is
	 * only necessary once and should really be performed by a DBA.
	 *
	 * <p>
	 * Adding indexes can improve response time for queries, but has consequences
	 * for storage and insertion time. However in a small database the query
	 * improvement will far out weigh the down sides and this is a recommend route
	 * to improvements.
	 *
	 * <p>
	 * As usual, your mileage may vary and consult a DBA if trouble persists.
	 *
	 * @param newTableRow the data model's version of the table that needs indexes
	 * @throws SQLException database exceptions
	 */
	public void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {

		List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
		List<String> indexClauses = new ArrayList<String>();
		for (PropertyWrapper field : fields) {
			final QueryableDatatype<?> qdt = field.getQueryableDatatype();
			if (field.isColumn() && !qdt.hasColumnExpression() && !(qdt instanceof DBLargeObject)) {
				String indexClause = definition.getIndexClauseForCreateTable(field);
				if (!indexClause.isEmpty()) {
					indexClauses.add(indexClause);
				}
			}
		}
		//Create indexes
		if (indexClauses.size() > 0) {
			final DBStatement statement = getDBStatement();
			try {
				for (String indexClause : indexClauses) {
					statement.execute(indexClause);
				}
			} finally {
				statement.close();
			}
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
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param tableRow tableRow
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		preventDDLDuringTransaction("DBDatabase.dropTable()");
		if (preventAccidentalDroppingOfTables) {
			throw new AccidentalDroppingOfTableException();
		}
		StringBuilder sqlScript = new StringBuilder();
		final String dropTableStart = definition.getDropTableStart();
		final String formatTableName = definition.formatTableName(tableRow);
		final Object endSQLStatement = definition.endSQLStatement();

		sqlScript.append(dropTableStart).append(formatTableName).append(endSQLStatement);
		String sqlString = sqlScript.toString();
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);
			dropAnyAssociatedDatabaseObjects(tableRow);
		} finally {
			dbStatement.close();
		}
		preventAccidentalDroppingOfTables = true;
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
	 * An even worse idea than {@link #dropTable(nz.co.gregs.dbvolution.DBRow)}
	 * <p>
	 * In General NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Seriously NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Your DBA will murder you.
	 *
	 * @param <TR> DBRow type
	 * @param tableRow tableRow
	 */
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		try {
			this.dropTable(tableRow);
		} catch (SQLException exp) {
//			exp.printStackTrace();
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
	 * While DBDefinition is important, unless you are implementing support for a
	 * new database you probably don't need this.
	 *
	 * @return the DBDefinition used by this DBDatabase instance
	 */
	public DBDefinition getDefinition() {
		return definition;
	}

	/**
	 * Sets the DBdefinition used by this DBDatabase
	 *
	 * <p>
	 * Every DBDatabase has a DBDefinition that defines the syntax used in that
	 * database.
	 *
	 * <p>
	 * While DBDefinition is important, unless you are implementing support for a
	 * new database you probably don't need this.
	 *
	 * @param defn the DBDefinition to be used by this DBDatabase instance.
	 */
	protected void setDefinition(DBDefinition defn) {
		if (definition == null) {
			definition = defn;
		}
	}

	/**
	 * Returns whether or not the example has any specified criteria.
	 *
	 * See
	 * {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.DBDatabase) willCreateBlankQuery}
	 * on DBRow.
	 *
	 * @param row row
	 * @return TRUE if the specified row has no specified criteria, FALSE
	 * otherwise
	 */
	public boolean willCreateBlankQuery(DBRow row) {
		return row.willCreateBlankQuery(this);
	}

	/**
	 * The worst idea EVAH.
	 *
	 * <p>
	 * Do NOT Use This.
	 *
	 * @param doIt don't do it.
	 * @throws java.lang.Exception java.lang.Exception
	 */
	public void dropDatabase(boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		preventDDLDuringTransaction("DBDatabase.dropDatabase()");
		if (preventAccidentalDroppingOfTables) {
			throw new AccidentalDroppingOfTableException();
		}
		if (preventAccidentalDroppingDatabase) {
			throw new AccidentalDroppingOfDatabaseException();
		}

		String dropStr = getDefinition().getDropDatabase(getDatabaseName());

		printSQLIfRequested(dropStr);
		LOG.info(dropStr);
		if (doIt) {
			this.doTransaction(new DBRawSQLTransaction(dropStr));
		}
		preventAccidentalDroppingOfTables = true;
		preventAccidentalDroppingDatabase = true;
	}

	/**
	 * The worst idea EVAH.
	 *
	 * <p>
	 * Do NOT Use This.
	 *
	 * @param databaseName the database to be permanently and completely
	 * destroyed.
	 * @param doIt don't do it.
	 * @throws java.lang.Exception java.lang.Exception
	 */
	public void dropDatabase(String databaseName, boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		preventDDLDuringTransaction("DBDatabase.dropDatabase()");
		if (preventAccidentalDroppingOfTables) {
			throw new AccidentalDroppingOfTableException();
		}
		if (preventAccidentalDroppingDatabase) {
			throw new AccidentalDroppingOfDatabaseException();
		}

		String dropStr = getDefinition().getDropDatabase(databaseName);

		printSQLIfRequested(dropStr);
		LOG.info(dropStr);
		if (doIt) {
			this.doTransaction(new DBRawSQLTransaction(dropStr));
		}
		preventAccidentalDroppingOfTables = true;
		preventAccidentalDroppingDatabase = true;
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
	 * @param databaseName	databaseName
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
	 * However sometimes this is inappropriate and this method can help with those
	 * times.
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
	 * However sometimes this is inappropriate and this method can help with those
	 * times.
	 *
	 * @param batchSQLStatementsWhenPossible TRUE if this instance will try to
	 * batch SQL statements, FALSE otherwise
	 */
	public void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		batchIfPossible = batchSQLStatementsWhenPossible;
	}

	private void preventDDLDuringTransaction(String message) throws AutoCommitActionDuringTransactionException {
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
	 * @param justLeaveThisAtTrue	justLeaveThisAtTrue
	 */
	public void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		this.preventAccidentalDroppingDatabase = justLeaveThisAtTrue;
	}

	/**
	 * Indicates whether this database supports full outer joins.
	 *
	 * <p>
	 * Some databases don't yet support queries where all the tables are optional,
	 * that is FULL OUTER joins.
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
		return true;
//		return supportsFullOuterJoinNatively();
	}

	/**
	 * Indicates whether this database supports full outer joins natively.
	 *
	 * <p>
	 * Some databases don't yet support queries where all the tables are optional,
	 * that is FULL OUTER joins.
	 *
	 * <p>
	 * This method indicates whether or not this instance can perform full outer
	 * joins.
	 *
	 * @return TRUE if the underlying database supports full outer joins natively,
	 * FALSE otherwise.
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
	 * @param <A> DBReport type
	 * @param report report
	 * @param examples examples
	 * @return A List of instances of the supplied report from the database 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException {
		return DBReport.getRows(this, report, examples);
	}

	/**
	 * Get All The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Provides convenient access to the using DBReport with a blank query.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getAllRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows from the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> the DBReport to be derived from the database data.
	 * @param report the report to be produced
	 * @param examples DBRow subclasses that provide extra criteria
	 * @return A list of the DBreports generated
	 * @throws SQLException database exceptions
	 */
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException {
		return DBReport.getAllRows(this, report, examples);
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
	 * @param <A> DBReport type
	 * @param report report
	 * @param examples examples
	 * @return A List of instances of the supplied report from the database 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException {
		return DBReport.getRows(this, report, examples);
	}

	boolean supportsPaging(QueryOptions options) {
		return definition.supportsPagingNatively(options);
	}

	/**
	 * Indicates to the DBSDatabase that the provided connection has been opened.
	 *
	 * <p>
	 * This is used internally for reference counting.
	 *
	 * @param connection	connection
	 */
//	public void connectionOpened(Connection connection) {
//		synchronized (getConnectionSynchronizeObject) {
//			connectionsActive++;
//		}
//	}
	/**
	 * Indicates to the DBDatabase that the provided connection has been closed.
	 *
	 * <p>
	 * This is used internally for reference counting.
	 *
	 * @param connection	connection
	 */
//	public void connectionClosed(Connection connection) {
//		synchronized (getConnectionSynchronizeObject) {
//			connectionsActive--;
//		}
//	}
	/**
	 * Provided to allow DBDatabase sub-classes to tweak their connections before
	 * use.
	 *
	 * <p>
	 * Used by {@link SQLiteDB} in particular.
	 *
	 * @return The connection configured ready to use. 1 Database exceptions may
	 * be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	protected Connection getConnectionFromDriverManager() throws SQLException {
		return DriverManager.getConnection(getJdbcURL(), getUsername(), getPassword());
	}

	/**
	 * @param jdbcURL the jdbcURL to set
	 */
	protected void setJdbcURL(String jdbcURL) {
		this.jdbcURL = jdbcURL;
	}

	/**
	 * @param username the username to set
	 */
	protected void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @param password the password to set
	 */
	protected void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Called after DROP TABLE to allow the DBDatabase to clean up any extra
	 * objects created with the table.
	 *
	 * @param <R> DBRow type
	 * @param tableRow tableRow
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@SuppressWarnings("empty-statement")
	protected <R extends DBRow> void dropAnyAssociatedDatabaseObjects(R tableRow) throws SQLException {
		;
	}

	/**
	 * Used by DBStatement to release the connection back into the connection
	 * pool.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param connection connection
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public synchronized void unusedConnection(Connection connection) throws SQLException {
		if (supportsPooledConnections()) {
			getConnectionList(busyConnections).remove(connection);
			getConnectionList(freeConnections).add(connection);
		} else {
			discardConnection(connection);
		}
	}

	/**
	 * Used to indicate that the DBDatabase class supports Connection Pooling.
	 *
	 * <p>
	 * The default implementation returns TRUE, and so will probably every
	 * implementation.
	 *
	 * @return TRUE if the DBDatabase supports connection pooling, FALSE
	 * otherwise.
	 */
	protected boolean supportsPooledConnections() {
		return true;
	}

	private synchronized void usedConnection(Connection connection) {
		if (supportsPooledConnections()) {
			getConnectionList(freeConnections).remove(connection);
			getConnectionList(busyConnections).add(connection);
		}
	}

	/**
	 * Removes a connection from the available pool.
	 *
	 * You'll not need to use this unless you're replacing DBvolution's database
	 * connection handling.
	 *
	 * @param connection the JDBC connection to be removed
	 */
	public synchronized void discardConnection(Connection connection) {
		getConnectionList(busyConnections).remove(connection);
		getConnectionList(freeConnections).remove(connection);
		try {
			connection.close();

		} catch (SQLException ex) {
			Logger.getLogger(DBDatabase.class
					.getName()).log(Level.WARNING, null, ex);
		}
//		connectionClosed(connection);
	}

	private synchronized List<Connection> getConnectionList(Map<DBDatabase, List<Connection>> connectionMap) {
		List<Connection> connList = connectionMap.get(this);
		if (connList == null) {
			connList = new ArrayList<Connection>();
			connectionMap.put(this, connList);
		}
		return connList;
	}

	/**
	 * Oracle does not differentiate between NULL and an empty string.
	 *
	 * @return FALSE.
	 */
	public Boolean supportsDifferenceBetweenNullAndEmptyString() {
		return true;
	}

	/**
	 * Indicates that this database supplies sufficient tools to create native
	 * recursive queries.
	 *
	 * <p>
	 * Please note that this may not be actual support for standard "WITH
	 * RECURSIVE".
	 *
	 * <p>
	 * If the database does not support recursive queries natively then DBvolution
	 * will emulate recursive queries. Native queries are faster and easier on the
	 * network and application server, so emulation should be a last resort.
	 *
	 * @return TRUE by default, but some DBDatabases may return FALSE.
	 */
	public boolean supportsRecursiveQueriesNatively() {
		return true;
	}

	/**
	 * Used By Subclasses To Inject Datatypes, Functions, Etc Into the Database.
	 *
	 * @param statement the statement to use when adding features, DO NOT CLOSE
	 * THIS STATEMENT.
	 * @throws SQLException database exceptions may occur
	 * @see PostgresDB
	 * @see H2DB
	 * @see SQLiteDB
	 * @see OracleDB
	 * @see MSSQLServerDB
	 * @see MySQLDB
	 */
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		// by default there are no extras to be added to the database
		;
	}

	/**
	 * Used to add features in a just in time manner.
	 *
	 * <p>
	 * During a statement the database may throw an exception because a feature
	 * has not yet been added. Use this method to parse the exception and install
	 * the required feature.
	 *
	 * <p>
	 * The statement will be automatically run after this method exits.
	 *
	 * @param exp the exception throw by the database that may need fixing
	 * @throws SQLException accessing the database may cause exceptions
	 */
	public void addFeatureToFixException(Exception exp) throws Exception {
		throw exp;
	}

	/**
	 * Indicates whether the database requires a persistent connection to operate
	 * correctly.
	 *
	 * <p>
	 * Some, usually in-memory, databases require a continuous connection to
	 * maintain their data.
	 *
	 * <p>
	 * DBvolution is usually clever with its connections and does not require 
	 * a persistent connection.
	 *
	 * <p>
	 * However if a continuous connection is required to maintain the data,
	 * override this method to return TRUE.
	 *
	 * @return TRUE if the database requires a continuous connection to maintain
	 * data, FALSE otherwise.
	 */
	protected boolean persistentConnectionRequired() {
		return false;
	}

	public <K extends DBRow> DBMigration<K> getDBMigrationMap(K mapper) {
		return new DBMigration<K>(this, mapper);
	}
}
