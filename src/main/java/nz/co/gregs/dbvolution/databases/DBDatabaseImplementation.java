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
package nz.co.gregs.dbvolution.databases;

import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.actions.DBBulkInsert;
import nz.co.gregs.dbvolution.exceptions.UnableToDropDatabaseException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryInsert;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRecursiveQuery;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import nz.co.gregs.dbvolution.databases.connections.DBConnectionSingle;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.metadata.DBDatabaseMetaData;
import nz.co.gregs.dbvolution.databases.settingsbuilders.NamedDatabaseCapableSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.transactions.*;
import nz.co.gregs.dbvolution.reflection.DataModel;
import nz.co.gregs.dbvolution.utility.RegularProcess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import nz.co.gregs.dbvolution.databases.settingsbuilders.VendorSettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.SettingsBuilder;
import nz.co.gregs.dbvolution.expressions.InstantExpression;
import nz.co.gregs.dbvolution.expressions.LocalDateTimeExpression;
import nz.co.gregs.dbvolution.databases.metadata.Options;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.dbvolution.utility.StringCheck;

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
public abstract class DBDatabaseImplementation implements DBDatabase, Serializable, Cloneable, AutoCloseable {

	private static final long serialVersionUID = 1l;
	static final private Log LOG = LogFactory.getLog(DBDatabaseImplementation.class);

	private String driverName = "";
	private boolean printSQLBeforeExecuting = false;
	boolean isInATransaction = false;
	transient DBTransactionStatement transactionStatement;
	private DBDefinition definition = null;
	private boolean batchIfPossible = true;
	private boolean preventAccidentalDroppingOfTables = true;
	private boolean preventAccidentalDroppingDatabase = true;
	private transient final Object getStatementSynchronizeObject = new Object();
	private transient final Object getConnectionSynchronizeObject = new Object();
	transient DBConnection transactionConnection;
	private static final transient Map<String, List<DBConnection>> BUSY_CONNECTIONS = new HashMap<>();
	private static final transient Map<String, List<DBConnection>> FREE_CONNECTIONS = new HashMap<>();
	private Boolean needToAddDatabaseSpecificFeatures = true;
	private final DatabaseConnectionSettings settings = new DatabaseConnectionSettings();
	private boolean terminated = false;
	private transient final List<RegularProcess> REGULAR_PROCESSORS = new ArrayList<>();
	private static final ScheduledExecutorService REGULAR_THREAD_POOL = Executors.newSingleThreadScheduledExecutor();
	private Throwable exception = null;
	private transient ScheduledFuture<?> regularThreadPoolFuture;
	private boolean hasCreatedRequiredTables = false;
	private boolean quietExceptionsPreference = false;
	private boolean preventAccidentalDeletingAllRowFromTable = true;

	{
		Runtime.getRuntime().addShutdownHook(new StopDatabase(this));
	}

	@Override
	public void close() {
		stop();
	}

	@Override
	public String toString() {
		String jdbcURL = getSettings().getUrl();
		String databaseName = getSettings().getDatabaseName();
		String username = getSettings().getUsername();
		if (jdbcURL != null && !jdbcURL.isEmpty()) {
			return this.getClass().getSimpleName() + "{" + (databaseName == null ? "UNNAMED" : databaseName + "=") + jdbcURL + ":" + username + "}";
		} else if (getDataSource() != null) {
			return this.getClass().getSimpleName() + ": " + getDataSource().toString();
		} else {
			return super.toString();
		}
	}

	/**
	 * Clones the DBDatabase.
	 *
	 * @return a clone of the DBDatabase.
	 * @throws CloneNotSupportedException not likely
	 */
	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		Object clone = super.clone();
		DBDatabase newInstance = (DBDatabase) clone;
		return newInstance;
	}

	@Override
	public DBDatabase copy() {
		try {
			return clone();
		} catch (CloneNotSupportedException ex) {
			Logger.getLogger(DBDatabase.class.getName()).log(Level.SEVERE, null, ex);
		}
		try {
			return this.getSettings().createDBDatabase();
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			Logger.getLogger(DBDatabase.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;
	}

	@Override
	public synchronized int hashCode() {
		int hash = 7;
		hash = 29 * hash + (this.getDriverName() != null ? this.getDriverName().hashCode() : 0);
		hash = 29 * hash + (this.getJdbcURL() != null ? this.getJdbcURL().hashCode() : 0);
		hash = 29 * hash + (this.getUsername() != null ? this.getUsername().hashCode() : 0);
		hash = 29 * hash + (this.getPassword() != null ? this.getPassword().hashCode() : 0);
		hash = 29 * hash + (this.getDataSource() != null ? this.getDataSource().hashCode() : 0);
		hash = 29 * hash + (this.getSettings() != null ? this.getSettings().hashCode() : 0);
		return hash;
	}

	@Override
	public synchronized boolean equals(Object obj) {
		if (obj instanceof DBDatabase) {
			DBDatabase other = (DBDatabase) obj;
			return this.getSettings().equals(other.getSettings());
		} else {
			return false;
		}
//		if (obj == null) {
//			return false;
//		}
//		if (getClass() != obj.getClass()) {
//			return false;
//		}
//		final DBDatabase other = (DBDatabase) obj;
//		if ((this.getDriverName() == null) ? (other.getDriverName() != null) : !this.getDriverName().equals(other.getDriverName())) {
//			return false;
//		}
//		if ((this.getJdbcURL() == null) ? (other.getJdbcURL() != null) : !this.getJdbcURL().equals(other.getJdbcURL())) {
//			return false;
//		}
//		if ((this.getUsername() == null) ? (other.getUsername() != null) : !this.getUsername().equals(other.getUsername())) {
//			return false;
//		}
//		if ((this.getPassword() == null) ? (other.getPassword() != null) : !this.getPassword().equals(other.getPassword())) {
//			return false;
//		}
//		if ((this.getDataSource() == null) ? (other.getDataSource() != null) : !this.getDataSource().equals(other.getDataSource())) {
//			return false;
//		}
//		final DatabaseConnectionSettings thisSettings = this.getSettings();
//		final DatabaseConnectionSettings otherSettings = other.getSettings();
//		return !(thisSettings != otherSettings && (thisSettings == null || !thisSettings.equals(otherSettings)));
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
	 * syntax of the database in the DBDefinition and the connection details using
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
	protected DBDatabaseImplementation() {
		SLEEP_BETWEEN_CONNECTION_RETRIES_MILLIS = 10;
		MAX_CONNECTION_RETRIES = 6;
		startRegularProcessor();
	}

	/**
	 * Define a new DBDatabase.
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Check the
	 * subclasses in {@code nz.co.gregs.dbvolution.databases} for your particular
	 * database.
	 *
	 * <p>
	 * DBDatabase encapsulates the knowledge of the database, in particular the
	 * syntax of the database in the DBDefinition and the connection details using
	 * a DataSource.
	 *
	 * @param settings - a SettingsBuilder for the required database.
	 * @throws java.sql.SQLException database errors
	 * @see DBDefinition
	 * @see OracleDB
	 * @see MSSQLServerDB
	 * @see MySQLDB
	 * @see PostgresDB
	 * @see H2DB
	 * @see H2MemoryDB
	 * @see InformixDB
	 * @see MariaDB
	 * @see MariaClusterDB
	 * @see NuoDB
	 */
	protected DBDatabaseImplementation(SettingsBuilder<?, ?> settings) throws SQLException {
		this();
		initDatabase(settings);
	}

	protected final void initDatabase(SettingsBuilder<?, ?> suppliedSettings) throws SQLException {
		this.definition = suppliedSettings.getDefinition();
		initDriver(suppliedSettings);
		settings.copy(suppliedSettings.toSettings());
		if (suppliedSettings instanceof NamedDatabaseCapableSettingsBuilder) {
			this.setDatabaseName(((NamedDatabaseCapableSettingsBuilder) suppliedSettings).getDatabaseName());
		}
		setDBDatabaseClassInSettings(suppliedSettings);
		createRequiredTables();
		checkForTimezoneIssues();
	}

	private void initDriver(SettingsBuilder<?, ?> settings) {
		if (settings instanceof VendorSettingsBuilder) {
			final String newDriverName = ((VendorSettingsBuilder<?, ?>) settings).getDriverName();
			initDriver(newDriverName);
		}
	}

	private void initDriver(final String driverName1) {
		driverName = driverName1;
		try {
			if (driverName != null && !driverName.isEmpty()) {
				Class.forName(driverName);
			}
		} catch (ClassNotFoundException ex) {
			LOG.error("Database driver class not found: " + driverName1, exception);
		}
	}

	@Override
	public DBTransactionStatement getDBTransactionStatement() throws SQLException {
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
	@Override
	public DBStatement getDBStatement() throws SQLException {
		DBStatement statement;
		synchronized (getStatementSynchronizeObject) {
			if (isInATransaction) {
				statement = this.transactionStatement;
				if (statement.isClosed()) {
					this.transactionStatement = new DBTransactionStatement(this, getLowLevelStatement());
				}
				/* TODO: this looks like it can return a closed statement unnecessarily */
			} else {
				statement = getLowLevelStatement();
			}
		}
		return statement;
	}

	protected synchronized DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		if (!terminated) {
			DBConnection connection = getConnection();
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
		return null;
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
	@Override
	public synchronized DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		if (terminated) {
			return null;
		} else {
			if (isInATransaction && !this.transactionConnection.isClosed()) {
				return this.transactionConnection;
			}
			DBConnection conn = null;
			while (conn == null) {
				if (supportsPooledConnections()) {
					if (FREE_CONNECTIONS.isEmpty() || getFreeConnections().isEmpty()) {
						conn = getRawConnection();
					} else {
						conn = getFreeConnections().get(0);
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
	}

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
			value = {"OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", "ODR_OPEN_DATABASE_RESOURCE"},
			justification = "Raw connections are pooled and closed  in discardConnection()")
	private DBConnection getRawConnection() throws UnableToFindJDBCDriver, UnableToCreateDatabaseConnectionException, SQLException {
		if (!terminated) {
			DBConnection connection = null;
			int retries = 0;
			synchronized (getConnectionSynchronizeObject) {
				if (this.getDataSource() == null) {
					try {
						if (getDriverName() != null && !getDriverName().isEmpty()) {
							// load the driver
							Class.forName(getDriverName());
						}
					} catch (ClassNotFoundException noDriver) {
						throw new UnableToFindJDBCDriver(getDriverName(), noDriver);
					}
					startServerIfRequired();
					while (connection == null) {
						try {
							connection = getDatabaseSpecificDBConnection(getConnectionFromDriverManager());
							DatabaseMetaData metaData = connection.getMetaData();
							LOG.debug("DATABASE: " + metaData.getDatabaseProductName() + " - " + metaData.getDatabaseProductVersion());
							LOG.debug("DATABASE: " + metaData.getDriverName() + " - " + metaData.getDriverVersion());
							setDefinitionBasedOnConnectionMetaData(connection.getClientInfo(), metaData);
						} catch (SQLException noConnection) {
							if (retries < MAX_CONNECTION_RETRIES) {
								retries++;
								try {
									getConnectionSynchronizeObject.wait(SLEEP_BETWEEN_CONNECTION_RETRIES_MILLIS);
								} catch (InterruptedException ex) {
									Logger.getLogger(DBDatabase.class.getName()).log(Level.SEVERE, null, ex);
								}
							} else {
								throw noConnection;
							}
						}
					}
				} else {
					try {
						connection = getDatabaseSpecificDBConnection(getDataSource().getConnection());
					} catch (SQLException noConnection) {
						throw new UnableToCreateDatabaseConnectionException(getDataSource(), noConnection);
					}
				}
			}
			synchronized (this) {
				if (needToAddDatabaseSpecificFeatures) {
					try (DBStatement createStatement = connection.createDBStatement()) {
						try {
							addDatabaseSpecificFeatures(createStatement.getInternalStatement());
						} catch (ExceptionDuringDatabaseFeatureSetup exceptionDuringDBCreation) {
							System.out.println("AN EXCEPTION OCCURRED DURING DATABASE SETUP: " + exceptionDuringDBCreation.getMessage());
						}
						needToAddDatabaseSpecificFeatures = false;
					}
				}
			}
			getFreeConnections().add(connection);
			return connection;
		}
		return null;
	}

	public DBConnection getDatabaseSpecificDBConnection(Connection connection) throws SQLException {
		return new DBConnectionSingle(this, connection);
	}

	private final int SLEEP_BETWEEN_CONNECTION_RETRIES_MILLIS;
	private int MAX_CONNECTION_RETRIES = 6;

	/**
	 * Used to hold the database open if required by the database.
	 *
	 */
	protected transient DBConnection storedConnection;

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
			value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
			justification = "Breaking the obligation is required to keep some databases, mostly memory DBs, from disappearing")
	private boolean connectionUsedForPersistentConnection(DBConnection connection) throws DBRuntimeException, SQLException {
		if (getDefinition().persistentConnectionRequired()) {
			if (storedConnection == null) {
				this.storedConnection = connection;
				this.storedConnection.createDBStatement();
			}
			if (storedConnection.equals(connection)) {
				return true;
			}
		}
		return false;
	}

	/**
	 *
	 * Inserts or updates DBRows into the correct tables automatically
	 *
	 * @param row a DBRow
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public DBActionList save(DBRow row) throws SQLException {
		return insertOrUpdate(row);
//		DBActionList changes = new DBActionList();
//		changes.addAll(insertOrUpdate(row));
//		return changes;
	}

	/**
	 *
	 * Inserts or updates DBRows into the correct tables automatically
	 *
	 * @param rows a DBRow
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public DBActionList save(DBRow... rows) throws SQLException {
		return insertOrUpdate(rows);
//		final DBActionList save = save(Arrays.asList(rows));
//		return save;
	}

	/**
	 *
	 * Inserts or updates DBRows into the correct tables automatically
	 *
	 * @param rows a DBRow
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public DBActionList save(Collection<DBRow> rows) throws SQLException {
		return insertOrUpdate(rows);
//		DBActionList actions = new DBActionList();
//		for (DBRow row : rows) {
//			actions.addAll(save(row));
//		}
//		return actions;
	}

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param row a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	@Override
	public DBActionList insert(DBRow row) throws SQLException {
		DBActionList changes = new DBActionList();
		changes.addAll(DBInsert.save(this, row));
		return changes;
	}

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	@Override
	public DBActionList insert(DBRow... listOfRowsToInsert) throws SQLException {
		if (listOfRowsToInsert.length > 0) {
			DBBulkInsert insert = new DBBulkInsert();
			insert.addAll(listOfRowsToInsert);
			return insert.insert(this);
		}
		return new DBActionList();
	}

	/**
	 *
	 * Inserts DBRows and Lists of DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a List of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	@Override
	public DBActionList insert(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException {
		if (listOfRowsToInsert.size() > 0) {
			DBBulkInsert dbBulkInsert = new DBBulkInsert();
			listOfRowsToInsert.forEach(row -> dbBulkInsert.addRow(row));
			return dbBulkInsert.insert(this);
		}
		return new DBActionList();
	}

	/**
	 *
	 * Inserts DBRows and Lists of DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a List of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	@Override
	public DBActionList insertOrUpdate(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException {
		DBActionList changes = new DBActionList();
		if (listOfRowsToInsert.size() > 0) {
			for (DBRow row : listOfRowsToInsert) {
				changes.addAll(insertOrUpdate(row));
			}
		}
		return changes;
	}

	@Override
	public DBActionList insertOrUpdate(DBRow row) throws SQLException {
		DBActionList changes = new DBActionList();
		try {
			changes.addAll(insert(row));
		} catch (SQLException exc1) {
			try {
				changes.addAll(update(row));
			} catch (SQLException exc2) {
				throw exc1;
			}
		}
		return changes;
	}

	@Override
	public DBActionList insertOrUpdate(DBRow... rows) throws SQLException {
		DBActionList changes = new DBActionList();
		for (DBRow row : rows) {
			changes.addAll(insertOrUpdate(row));
		}
		return changes;
	}

	/**
	 *
	 * Deletes DBRows using the correct tables automatically
	 *
	 * @param rows a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	@Override
	public final DBActionList delete(DBRow... rows) throws SQLException {
		DBActionList changes = new DBActionList();
		for (DBRow row : rows) {
			changes.addAll(this.getDBTable(row).delete(row));
		}
		return changes;
	}

	/**
	 *
	 * Deletes Lists of DBRows using the correct tables automatically
	 *
	 * @param list a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	@Override
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
	@Override
	public final DBActionList update(DBRow... rows) throws SQLException {
		return DBUpdate.update(this, rows);
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
	@Override
	public final DBActionList update(Collection<? extends DBRow> listOfRowsToUpdate) throws SQLException {
		return DBUpdate.update(this, listOfRowsToUpdate);
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
	@Override
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBTable<R> dbTable = getDBTable(exampleRow);
		return dbTable.getAllRows();
	}

	/**
	 *
	 * Automatically selects the correct table based on the examples supplied and
	 * returns the number of rows found based on the example.
	 *
	 * <p>
	 * See {@link nz.co.gregs.dbvolution.DBTable#count()}
	 *
	 * @param example the first example
	 * @param examples the examples
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws AccidentalCartesianJoinException Thrown when a query will create a
	 * Cartesian Join and cartesian joins have not been explicitly permitted.
	 */
	public long getCount(DBRow example, DBRow... examples) throws SQLException, AccidentalCartesianJoinException {
		DBQuery query = getDBQuery(example, examples).setBlankQueryAllowed(true);
		return query.count();
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
	@Override
	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
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
	@Override
	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
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
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown if a cluster is unable to service requests.
	 */
	@Override
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return get(expectedNumberOfRows, exampleRow);
	}

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * @param row the first example
	 * @param rows the examples of the rows required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
	@Override
	public List<DBQueryRow> get(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		DBQuery dbQuery = getDBQuery(row, rows);
		return dbQuery.getAllRows();
	}

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given.
	 *
	 * @param row the first table
	 * @param rows the example rows for the tables required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
	@Override
	public List<DBQueryRow> getByExamples(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return get(row, rows);
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
	 * @param row the first example of the tables required
	 * @param rows examples of the tables required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException thrown when the retrieved row count
	 * is wrong
	 * @see DBQuery
	 * @see DBQuery#getAllRows(long)
	 */
	@Override
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow row, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		if (expectedNumberOfRows == null) {
			return get(row, rows);
		} else {
			return getDBQuery(row, rows).getAllRows(expectedNumberOfRows);
		}
	}

	/**
	 *
	 * Convenience method to simplify switching using READONLY to COMMITTED
	 * transaction
	 *
	 * @param <V> the return type of the transaction, can be anything
	 * @param dbTransaction the transaction to execute
	 * @param commit commit=true or rollback=false.
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception using the transaction
	 * @see DBTransaction
	 * @see
	 * DBDatabase#doTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 * @see
	 * DBDatabase#doReadOnlyTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 */
	@Override
	public synchronized <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, ExceptionThrownDuringTransaction {
		DBDatabaseImplementation db;
		try {
			db = (DBDatabaseImplementation) this.clone();
		} catch (CloneNotSupportedException ex) {
			throw new UnsupportedOperationException("Unable to drop database due to incorrecte DBDatabase implementation: correct the implementation of clone()", ex);
		}
		V returnValues = null;
		db.transactionStatement = db.getDBTransactionStatement();
		try {
			db.isInATransaction = true;
			db.transactionConnection = db.transactionStatement.getConnection();
			db.transactionConnection.setAutoCommit(false);
			try {
				returnValues = dbTransaction.doTransaction(db);
				if (commit) {
					db.transactionConnection.commit();
				} else {
					try {
						db.transactionConnection.rollback();
					} catch (SQLException rollbackFailed) {
						discardConnection(db.transactionConnection);
					}
				}
			} catch (SQLException | ExceptionThrownDuringTransaction ex) {
				try {
					db.transactionConnection.rollback();
				} catch (SQLException excp) {
					LOG.warn("Exception Occurred During Rollback: " + ex.getLocalizedMessage());
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

	@Override
	public synchronized <V> IncompleteTransaction<V> doTransactionWithoutCompleting(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction {
		DBDatabaseImplementation db;
		try {
			db = (DBDatabaseImplementation) this.clone();
		} catch (CloneNotSupportedException ex) {
			throw new UnsupportedOperationException("Unable to clone database due to incorrect DBDatabase implementation: correct the implementation of clone()", ex);
		}
		IncompleteTransaction<V> results = null;
		db.transactionStatement = db.getDBTransactionStatement();
		db.isInATransaction = true;
		db.transactionConnection = db.transactionStatement.getConnection();
		db.transactionConnection.setAutoCommit(false);
		try {
			results = new IncompleteTransaction<>(db, dbTransaction.doTransaction(db));
		} catch (ExceptionThrownDuringTransaction ex) {
			try {
				db.transactionConnection.rollback();
			} catch (SQLException excp) {
				LOG.warn("Exception Occurred During Rollback: " + ex.getLocalizedMessage());
			}
			throw ex;
		}
		return results;
	}

	@Override
	public void commitTransaction() throws SQLException {
		try {
			transactionConnection.commit();
		} finally {
			isInATransaction = false;
			transactionStatement.transactionFinished();
			discardConnection(transactionConnection);
			transactionConnection = null;
			transactionStatement = null;
		}
	}

	@Override
	public void rollbackTransaction() throws SQLException {
		try {
			transactionConnection.rollback();
		} finally {
			isInATransaction = false;
			transactionStatement.transactionFinished();
			discardConnection(transactionConnection);
			transactionConnection = null;
			transactionStatement = null;
		}
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
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception using the transaction
	 * @see DBTransaction
	 */
	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction {
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
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception using the transaction
	 * @see DBTransaction
	 */
	@Override
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
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
	@Override
	public DBActionList implement(DBScript script) throws Exception {
		return script.implement(this);
	}

	/**
	 * Returns the name of the JDBC driver class used by this DBDatabase instance.
	 *
	 * @return the driverName
	 */
	@Override
	public synchronized String getDriverName() {
		return driverName;
	}

	/**
	 * Sets the name of the JDBC driver class used by this DBDatabase instance.
	 *
	 * @param driver the name of the JDBC Drive class for this DBDatabase.
	 */
	protected synchronized void setDriverName(String driver) {
		driverName = driver;
	}

	/**
	 * Returns the JDBC URL used by this instance, if one has been specified.
	 *
	 * @return the jdbcURL
	 */
	@Override
	public final synchronized String getJdbcURL() {
		return getUrlFromSettings(getSettings());
	}

	/**
	 * Returns the username specified for this DBDatabase instance.
	 *
	 * @return the username
	 */
	@Override
	final public synchronized String getUsername() {
		return settings.getUsername();
	}

	/**
	 * Returns the password specified.
	 *
	 * @return the password
	 */
	@Override
	final public synchronized String getPassword() {
		return settings.getPassword();
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
	@Override
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return DBTable.getInstance(this, example);
	}

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param example the example rows that are required in the query
	 * @return a DBQuery with the examples as required tables
	 */
	@Override
	public DBQuery getDBQuery(DBRow example) {
		return DBQuery.getInstance(this, example);
	}

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.This is
	 * the easiest way to create DBQueries, and indeed queries.
	 *
	 * @return a DBQuery with the examples as required tables
	 */
	@Override
	public DBQuery getDBQuery() {
		return DBQuery.getInstance(this);
	}

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param example the first example row that is required in the query
	 * @param examples the example rows that are required in the query
	 * @return a DBQuery with the examples as required tables
	 */
	@Override
	public DBQuery getDBQuery(DBRow example, DBRow... examples) {
		final DBQuery query = DBQuery.getInstance(this, example, examples);
		return query;
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
	@Override
	public DBQuery getDBQuery(final Collection<DBRow> examples) {
		switch (examples.size()) {
			case 0:
				return getDBQuery();
			case 1:
				return getDBQuery(examples.toArray(new DBRow[]{})[0]);
			default:
				List<DBRow> list = new ArrayList<>(examples);
				DBRow[] toArray = list.toArray(new DBRow[]{});
				DBRow row = toArray[0];
				list.remove(row);
				toArray = list.toArray(new DBRow[]{});
				return getDBQuery(row, toArray);
		}
	}

	/**
	 * Enables the printing of all SQL to System.out before the SQL is executed.
	 *
	 * @param b TRUE to print SQL before execution, FALSE otherwise.
	 */
	@Override
	public synchronized void setPrintSQLBeforeExecuting(boolean b) {
		printSQLBeforeExecuting = b;
	}

	/**
	 * Indicates whether SQL will be printed before it is executed.
	 *
	 * @return the printSQLBeforeExecuting
	 */
	@Override
	public boolean isPrintSQLBeforeExecuting() {
		return printSQLBeforeExecuting;
	}

	/**
	 * Called by internal methods that are about to execute SQL so the SQL can be
	 * printed.
	 *
	 * @param sqlString the raw SQL to print
	 */
	@Override
	public void printSQLIfRequested(String sqlString) {
		printSQLIfRequested(sqlString, System.out);
		LOG.debug(sqlString);
	}

	@Override
	public synchronized void printSQLIfRequested(String sqlString, PrintStream out) {
		if (printSQLBeforeExecuting) {
			out.println(sqlString);
		}
	}

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param includeForeignKeyClauses should explicit FK references be created in
	 * the database?
	 * @param newTable the table to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	@Override
	public void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		try {
			createTable(newTable, includeForeignKeyClauses);
		} catch (SQLException ex) {
			LOG.info(ex.getLocalizedMessage());
		}
	}

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Foreign key constraints are NOT created.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param newTable the table to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	@Override
	public void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		try {
			createTable(newTable, false);
		} catch (SQLException ex) {
			LOG.info(ex.getLocalizedMessage());
		}
	}

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param includeForeignKeyClauses should explicit FK references be created in
	 * the database?
	 * @param newTables the tables to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	@Override
	public void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		for (DBRow tab : newTables) {
			createTableNoExceptions(includeForeignKeyClauses, tab);
		}
	}

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Foreign key constraints are NOT created.
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param newTables the tables to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	@Override
	public void createTablesNoExceptions(DBRow... newTables) {
		for (DBRow tab : newTables) {
			createTableNoExceptions(false, tab);
		}
	}

	/**
	 * Creates tables on the database based on the DBRows, and creates the
	 * required database foreign key constraints.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRow.
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
	 * @param newTables table
	 *
	 */
	@Override
	public void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		for (DBRow tab : newTables) {
			try {
				createTable(tab, true);
			} catch (SQLException | AutoCommitActionDuringTransactionException ex) {
			}
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
	@Override
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		createTable(newTableRow, false);
	}

	/**
	 * Creates or updates a table on the database based on the DBRow.
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
	@Override
	public void createOrUpdateTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		updateTableToMatchDBRow(newTableRow);
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
	@Override
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		createTable(newTableRow, true);
	}

	@Override
	public DBActionList createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		return DBCreateTable.createTable(this, includeForeignKeyClauses, newTableRow);
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
	 * All databases support FK constraints, and they provide useful checks.
	 * However they are the last possible check, represent an inadequate
	 * protection, and can cause considerable difficulties at surprising times. I
	 * recommend against them.
	 *
	 * <p>
	 * Note: SQLite does not support adding Foreign Keys to existing tables.
	 *
	 * @param newTableRow the table that needs foreign key constraints
	 * @throws SQLException the database has had an issue.
	 */
	/*TODONE: convert to use DBAction to improve cluster implementation */
	@Override
	public synchronized void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		executeDBAction(new DBCreateForeignKeys(newTableRow));
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
	 * DBvolution cannot to protect you using this situation, however this method
	 * will remove some of the problem.
	 *
	 * @param newTableRow the data models version of the table that needs FKs
	 * removed
	 * @throws SQLException database exceptions
	 */
	/*TODONE: convert to use DBAction to improve cluster implementation */
	@Override
	public synchronized void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		executeDBAction(new DBDropForeignKeys(newTableRow));
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
	/*TODONE: convert to use DBAction to improve cluster implementation */
	@Override
	public synchronized void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		DBCreateIndexesOnAllFields.createIndexes(this, newTableRow);
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
	 * @return a list of all actions that have been executed
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public synchronized DBActionList dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		DBActionList changes = new DBActionList();
		preventAccidentalDroppingOfTables();
		DBDropTable drop = new DBDropTable(tableRow);
		changes.add(drop);

		executeDBAction(drop);

		return changes;
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
	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		LOG.debug("DROPPING TABLE NOEXECEPTIONS: " + tableRow.getTableName());
		try {
			this.dropTable(tableRow);
		} catch (SQLException exp) {
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
	 * @throws java.sql.SQLException database errors
	 */
	@Override
	public DBActionList dropTableIfExists(DBRow tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, SQLException {
		DBActionList changes = new DBActionList();
		preventAccidentalDroppingOfTables();
		DBDropTableIfExists drop = new DBDropTableIfExists(tableRow);
		changes.add(drop);

		executeDBAction(drop);

		return changes;
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
	@Override
	public synchronized DBDefinition getDefinition() throws NoAvailableDatabaseException {
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
	protected synchronized final void setDefinition(DBDefinition defn) {
		if (definition == null) {
			definition = defn;
		} else if (defn != null && definition.getClass() != defn.getClass()) {
			definition = defn;
		}
	}

	/**
	 * Returns whether or not the example has any specified criteria.See
	 * {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.databases.definitions.DBDefinition) willCreateBlankQuery}
	 * on DBRow.
	 *
	 *
	 * @param row row
	 * @return TRUE if the specified row has no specified criteria, FALSE
	 * otherwise
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown when a cluster cannot service requests
	 */
	@Override
	public boolean willCreateBlankQuery(DBRow row) throws NoAvailableDatabaseException {
		return row.willCreateBlankQuery(this.getDefinition());
	}

	/**
	 * The worst idea EVAH.
	 *
	 * <p>
	 * Do NOT Use This.
	 *
	 * @param doIt don't do it.
	 * @throws AccidentalDroppingOfDatabaseException See?
	 * @throws UnableToDropDatabaseException Terrible!
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * If You Lucky.
	 */
	@Override
	public synchronized void dropDatabase(boolean doIt) throws AccidentalDroppingOfDatabaseException, UnableToDropDatabaseException, SQLException, AutoCommitActionDuringTransactionException, ExceptionThrownDuringTransaction {
		dropDatabase(getDatabaseName(), true);
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
	 * @throws AccidentalDroppingOfDatabaseException Terrible!
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * If you're lucky...
	 * @throws java.sql.SQLException database errors
	 */
	@Override
	public synchronized void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfDatabaseException, SQLException, ExceptionThrownDuringTransaction {
		preventAccidentalDroppingOfDatabases();
		if (doIt) {
			executeDBAction(new DBDropDatabase(databaseName));
		}
	}

	/**
	 * Returns the database name if one was supplied.
	 *
	 * @return the database name
	 */
	@Override
	final public synchronized String getDatabaseName() {
		return settings.getDatabaseName();
	}

	/**
	 * Sets the database name.
	 *
	 * @param databaseName	databaseName
	 */
	public synchronized void setDatabaseName(String databaseName) {
		getSettings().setDatabaseName(databaseName);
	}

	/**
	 * A label for the database for reference within an application.
	 *
	 * <p>
	 * This label has no effect on the actual database connection.
	 *
	 * @param label a purely arbitrary value
	 */
	final public void setLabel(String label) {
		getSettings().setLabel(label);
	}

	/**
	 * A label for the database for reference within an application.
	 *
	 * <p>
	 * This label has no effect on the actual database connection.
	 *
	 * @return the internal label of this database
	 */
	@Override
	final public String getLabel() {
		final String label = settings.getLabel();
		return label == null || label.isEmpty()
				? "Unlabelled " + this.getClass().getSimpleName()
				: label;
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
	@Override
	public synchronized boolean batchSQLStatementsWhenPossible() {
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
	@Override
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		batchIfPossible = batchSQLStatementsWhenPossible;
	}

	public synchronized void preventAccidentalDDLDuringTransaction(DBAction action) throws AutoCommitActionDuringTransactionException {
		if (isInATransaction && action.getIntent().isDDL()) {
			throw new AutoCommitActionDuringTransactionException(action.getClass().getSimpleName());
		}
	}

	/**
	 *
	 * @param droppingTablesIsAMistake just leave it at TRUE.
	 */
	/*
	* The lack of documentation is by design: you probably shouldn't be using this method.
	
	If you must use it, maybe you're the DBA or something, this only works for one call of dropTable().
	
	It is automatically reset to TRUE after every use to avoid accidental use.
	
	Also note that there is a race condition between the setting of this and your call to dropTable().  If other code
	calls dropTable() somewhere else, it may get there before you do, so just never use this, OK?
	 */
	@Override
	public synchronized void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		this.preventAccidentalDroppingOfTables = droppingTablesIsAMistake;
	}

	protected synchronized boolean getPreventAccidentalDroppingOfTables() {
		return this.preventAccidentalDroppingOfTables;
	}

	/**
	 *
	 * @param justLeaveThisAtTrue	justLeaveThisAtTrue
	 */
	/*
	* The lack of documentation is by design: you shouldn't be using this method.
	
	If you must use it, maybe you're the DBA or something, this only works for one call of dropDatabase().
	
	It is automatically reset to TRUE after every use to avoid accidental use.
	
	Also note that there is a race condition between the setting of this and your call to dropDatabase().  If other code
	calls dropDatabase() somewhere else, it may get there before you do, so just never use this, OK?
	 */
	@Override
	public synchronized void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		this.preventAccidentalDroppingDatabase = justLeaveThisAtTrue;
	}

	public synchronized boolean getPreventAccidentalDroppingOfDatabases() {
		return this.preventAccidentalDroppingDatabase;
	}

//	public synchronized void preventAccidentalDroppingOfDatabases(DBAction action) throws AccidentalDroppingOfDatabaseException {
//		if (preventAccidentalDroppingDatabase && action.getIntent().isDropDatabase()) {
//			throw new AccidentalDroppingOfDatabaseException();
//		} else {
//			preventAccidentalDroppingDatabase = true;
//		}
//	}
	public synchronized void preventAccidentalDroppingOfDatabases() throws AccidentalDroppingOfDatabaseException {
		if (preventAccidentalDroppingDatabase) {
			throw new AccidentalDroppingOfDatabaseException();
		} else {
			preventAccidentalDroppingDatabase = true;
		}
	}

	/**
	 * Get The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows using the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> DBReport type
	 * @param report report
	 * @param examples examples
	 * @return A List of instances of the supplied report using the database 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
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
	 * {@link DBReport#getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows using the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> the DBReport to be derived using the database data.
	 * @param report the report to be produced
	 * @param examples DBRow subclasses that provide extra criteria
	 * @return A list of the DBreports generated
	 * @throws SQLException database exceptions
	 */
	@Override
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return DBReport.getAllRows(this, report, examples);
	}

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
	@Override
	public Connection getConnectionFromDriverManager() throws SQLException {
		if (terminated) {
			return null;
		} else {
			try {
				LOG.debug("CREATING NEW CONNECTION: " + getJdbcURL());
				return DriverManager.getConnection(getJdbcURL(), getUsername(), getPassword());
			} catch (SQLException e) {
				throw new DBRuntimeException("Connection Failed to URL " + getJdbcURL(), e);
			}
		}
	}

	/**
	 * @param jdbcURL the jdbcURL to set
	 */
	final protected synchronized void setJdbcURL(String jdbcURL) {
		if (FREE_CONNECTIONS.isEmpty()) {
			settings.setUrl(jdbcURL);
		}
	}

	/**
	 * @param username the username to set
	 */
	final protected synchronized void setUsername(String username) {
		if (FREE_CONNECTIONS.isEmpty()) {
			getSettings().setUsername(username);
		}
	}

	/**
	 * @param password the password to set
	 */
	final protected synchronized void setPassword(String password) {
		if (FREE_CONNECTIONS.isEmpty()) {
			getSettings().setPassword(password);
		}
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
	@Override
	public synchronized void unusedConnection(DBConnection connection) throws SQLException {
		if (supportsPooledConnections()) {
			getBusyConnections().remove(connection);
			getFreeConnections().add(connection);
		} else {
			discardConnection(connection);
		}
	}

	private List<DBConnection> getBusyConnections() {
		return getConnectionList(BUSY_CONNECTIONS);
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

	private synchronized void usedConnection(DBConnection connection) {
		if (supportsPooledConnections()) {
			getFreeConnections().remove(connection);
			getBusyConnections().add(connection);
		}
	}

	private List<DBConnection> getFreeConnections() {
		return getConnectionList(FREE_CONNECTIONS);
	}

	/**
	 * Removes a connection from the available pool.
	 *
	 * You'll not need to use this unless you're replacing DBvolution's database
	 * connection handling.
	 *
	 * @param connection the JDBC connection to be removed
	 */
	@Override
	public synchronized void discardConnection(DBConnection connection) {
		if (connection != null) {
			getBusyConnections().remove(connection);
			getFreeConnections().remove(connection);
			try {
				connection.close();
			} catch (SQLException ex) {
				Logger.getLogger(DBDatabase.class
						.getName()).log(Level.WARNING, null, ex);
			}
		}
	}

	private synchronized List<DBConnection> getConnectionList(Map<String, List<DBConnection>> connectionMap) {
		final String key = this.getSettings().encode();
		List<DBConnection> connList = connectionMap.get(key);
		if (connList == null) {
			connList = new ArrayList<>();
			connectionMap.put(key, connList);
		}
		return connList;
	}

	/**
	 * Used to add features in a just-in-time manner.
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
	 * @param intent the intention of the query or DDL when the exception was
	 * thrown
	 * @param details the details of the attempted execution
	 * @return the preferred response to the exception
	 * @throws SQLException accessing the database may cause exceptions
	 */
	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent, StatementDetails details) throws Exception {
		throw exp;
	}

	@Override
	public final String getUrlFromSettings(DatabaseConnectionSettings oldSettings) {
		return getURLInterpreter().generateJDBCURL(oldSettings);
	}

	@Override
	public final DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
		return getURLInterpreter().fromJDBCURL(jdbcURL).toSettings();
	}

	/**
	 * Create a DBRecursiveQuery based on the query and foreign key supplied.
	 *
	 * <p>
	 * DBRecursiveQuery uses the query to create the first rows of the recursive
	 * query. This can be any query and contain any tables. However it must
	 * contain the table T and the foreign key must be a recursive foreign key
	 * (FK) to and using table T.
	 *
	 * <p>
	 * After the priming query has been created the FK supplied will be followed
	 * repeatedly. The FK must be contained in one of the tables of the priming
	 * query and it must reference the same table, that is to say it must be a
	 * recursive foreign key.
	 *
	 * <p>
	 * The FK will be repeatedly followed until the root node is reached (an
	 * ascending query) or the leaf nodes have been reached (a descending query).
	 * A root node is defined as a row with a null value in the FK. A leaf node is
	 * a row that has no FKs referencing it.
	 *
	 * <p>
	 * While it is possible to define a root node in other ways only the above
	 * definition is currently supported.
	 *
	 * @param <T> the DBRow produced by this recursive query
	 * @param query the priming query
	 * @param keyToFollow the FK to follow
	 * @param dbRow required to define T
	 * @return a recursive query
	 * @throws ColumnProvidedMustBeAForeignKey Only FKs please
	 * @throws ForeignKeyDoesNotReferenceATableInTheQuery Only FKs actually in the
	 * query please
	 * @throws ForeignKeyIsNotRecursiveException the FK must be in _and_ reference
	 * T
	 */
	@Override
	public <T extends DBRow> DBRecursiveQuery<T> getDBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow, T dbRow) {
		return new DBRecursiveQuery<T>(query, keyToFollow);
	}

	public boolean isDBDatabaseCluster() {
		return (this instanceof DBDatabaseCluster);
	}

	@Override
	public final DataSource getDataSource() {
		return getSettings().getDataSource();
	}

	@Override
	public void setLastException(Throwable except) {
		this.exception = except;
	}

	public Throwable getLastException() {
		return this.exception;
	}

	@Override
	public void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		;
	}

	@Override
	public boolean supportsMicrosecondPrecision() {
		return true;
	}

	@Override
	public boolean supportsNanosecondPrecision() {
		return true;
	}

	@Override
	public boolean supportsDifferenceBetweenNullAndEmptyString() {
		return getDefinition().canProduceNullStrings();
	}

	protected boolean requiredToProduceEmptyStringForNull() {
		return !supportsDifferenceBetweenNullAndEmptyString();
	}

	protected boolean hasCreatedRequiredTables() {
		return hasCreatedRequiredTables;
	}

	protected final void setHasCreatedRequiredTables(boolean b) {
		hasCreatedRequiredTables = b;
	}

	@Override
	public LocalDateTime getCurrentLocalDatetime() throws SQLException {
		DBQuery query = getDBQuery();
		final String key = "THE DATABASE LOCALDATETIME";
		query.addExpressionColumn(key, LocalDateTimeExpression.currentLocalDateTime().asExpressionColumn());
		Object value;
		try {
			value = query.setBlankQueryAllowed(true).getAllRows(1).get(0).getExpressionColumnValue(key).getValue();
			if (value instanceof LocalDateTime) {
				return (LocalDateTime) value;
			}
		} catch (UnexpectedNumberOfRowsException | AccidentalCartesianJoinException | AccidentalBlankQueryException ex) {
			LOG.error("UNABLE TO RETRIEVE SYSTEM LOCALDATETIME ", ex);
		}
		throw new SQLException("UNABLE TO RETRIEVE SYSTEM LOCALDATETIME: " + query.getSQLForQuery());
	}

	@Override
	public Instant getCurrentInstant() throws UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, SQLException {
		DBQuery query = getDBQuery();
		final String key = "THE SYSTEM ZONEDDATETIME";
		query.addExpressionColumn(key, InstantExpression.currentInstant().asExpressionColumn());
		Object value = query.setBlankQueryAllowed(true).getAllRows(1).get(0).getExpressionColumnValue(key).getValue();
		if (value instanceof Instant) {
			return (Instant) value;
		} else {
			throw new SQLException("UNABLE TO RETRIEVE SYSTEM LOCALDATETIME: " + query.getSQLForQuery());
		}
	}

	private void checkForTimezoneIssues() throws SQLException {
	}

	@Override
	public void handleErrorDuringExecutingSQL(DBDatabase suspectDatabase, Throwable sqlException, String sqlString) {
		;
	}

	public boolean supportsPolygonDatatype() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

//	protected synchronized void preventAccidentalDroppingOfTables(DBAction action) throws AccidentalDroppingOfTableException {
//		if (preventAccidentalDroppingOfTables && action.getIntent().isDropTable()) {
//			throw new AccidentalDroppingOfTableException();
//		} else {
//			preventAccidentalDroppingOfTables = true;
//		}
//	}
//
//	protected synchronized void preventAccidentalDeletingAllRowsFromTable(DBAction action) throws AccidentalDroppingOfTableException {
//		if (preventAccidentalDeletingAllRowFromTable && action.getIntent().isDeleteAllRows()) {
//			throw new AccidentalDeletingAllRowsFromTableException();
//		} else {
//			preventAccidentalDeletingAllRowFromTable = true;
//		}
//	}
	protected synchronized void preventAccidentalDroppingOfTables() throws AccidentalDroppingOfTableException {
		if (preventAccidentalDroppingOfTables) {
			throw new AccidentalDroppingOfTableException();
		} else {
			preventAccidentalDroppingOfTables = true;
		}
	}

	protected synchronized void preventAccidentalDeletingAllRowsFromTable() throws AccidentalDroppingOfTableException {
		if (preventAccidentalDeletingAllRowFromTable) {
			throw new AccidentalDeletingAllRowsFromTableException();
		} else {
			preventAccidentalDeletingAllRowFromTable = true;
		}
	}

	@Override
	public void setPreventAccidentalDeletingAllRowsFromTable(boolean b) {
		preventAccidentalDeletingAllRowFromTable = b;
	}

	@Override
	public void deleteAllRowsFromTable(DBRow table) throws SQLException {
		preventAccidentalDeletingAllRowsFromTable();
		executeDBAction(new DBDeleteAll(table));
	}

	@Override
	public boolean supportsGeometryTypesFullyInSchema() {
		return false;
	}

	public static enum ResponseToException {
		REPLACECONNECTION(),
		REQUERY(),
		SKIPQUERY(),
		EMULATE_RECURSIVE_QUERY(),
		NOT_HANDLED;

		ResponseToException() {
		}
	}

	@Override
	public <K extends DBRow> DBQueryInsert<K> getDBQueryInsert(K mapper) {
		return new DBQueryInsert<>(this, mapper);
	}

	@Override
	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
//		preventAccidentalDDLDuringTransaction(action);
//		preventAccidentalDroppingOfDatabases(action);
//		preventAccidentalDroppingOfTables(action);
//		preventAccidentalDeletingAllRowsFromTable(action);
		if (quietExceptionsPreference) {
			try {
				return action.action(this);
			} catch (SQLException acceptableException) {
				return new DBActionList();
			}
		} else {
			return action.action(this);
		}
	}

	@Override
	public void setQuietExceptionsPreference(boolean b) {
		this.quietExceptionsPreference = b;
	}

	@Override
	public boolean getQuietExceptionsPreference() {
		return this.quietExceptionsPreference;
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		query.setDatabaseQuietExceptionsPreference(getQuietExceptionsPreference());
		return query.query(this);
	}

	@Override
	public String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException {
		query.setDatabaseQuietExceptionsPreference(getQuietExceptionsPreference());
		return query.toSQLString(this);
	}

	/**
	 * Checks for the existence of the table on the database.
	 *
	 * @param table the class of the table to check for
	 * @return true if the table exists on the database, for clusters it is only
	 * true if the table exists on all databases in the cluster
	 * @throws SQLException database errors
	 */
	public boolean tableExists(Class<? extends DBRow> table) throws SQLException {
		return tableExists(DBRow.getDBRow(table));
	}

	/**
	 * Checks for the existence of the table on the database.
	 *
	 * @param table the table to check for
	 * @return true if the table exists on the database, for clusters it is only
	 * true if the table exists on all databases in the cluster
	 * @throws SQLException database errors
	 */
	@Override
	@SuppressFBWarnings(
			value = "REC_CATCH_EXCEPTION",
			justification = "Database vendors throw all sorts of silly exceptions")
	public boolean tableExists(DBRow table) throws SQLException {
		boolean tableExists;

		if (getDefinition().supportsTableCheckingViaMetaData()) {
			tableExists = checkTableExistsViaMetaData(table);
		} else {
			tableExists = checkTableExistsViaQuery(table);
		}
		return tableExists;
	}

	protected boolean checkTableExistsViaQuery(DBRow table) throws NoAvailableDatabaseException {
		boolean tableExists;
		String testQuery = getDefinition().getTableExistsSQL(table);
		try (DBStatement dbStatement = getDBStatement()) {
			var dets = new StatementDetails("CHECK FOR TABLE " + table.getTableName(), QueryIntention.CHECK_TABLE_EXISTS, testQuery, dbStatement);
			ResultSet results = dbStatement.executeQuery(dets);
			if (results != null) {
				results.close();
				tableExists = true;
			} else {
				tableExists = false;
			}
		} catch (Exception ex) {
			// An exception means we couldn't find the table for whatever reason
			// so we can safely ignore the exception
			// but just to be clear, lets ensure we return false
			tableExists = false;
			// Theoretically this should only need to catch an SQLException
			// but databases throw all sorts of weird exceptions
		}
		return tableExists;
	}

	private boolean checkTableExistsViaMetaData(DBRow table) throws SQLException {
//		boolean tableExists = false;
		ResultSet rset = getMetaDataForTable(table);
		return checkMetaDataForTable(table, rset);
//		if (rset.next()) {
//			tableExists = true;
//		}
//		return tableExists;
	}

	protected boolean checkMetaDataForTable(DBRow table, ResultSet rset) throws SQLException {
		boolean tableExists = false;
		rset.beforeFirst();
		if (rset.next()) {
			// found the header row, lets check for the data row
			if (rset.next()) {
				tableExists = true;
			}
		}
		return tableExists;
	}

	private ResultSet getMetaDataForTable(DBRow table) throws SQLException {
		try (DBStatement dbStatement = getDBStatement()) {
			DBConnection conn = dbStatement.getConnection();
			ResultSet rset = conn.getMetaData().getTables(null, null, table.getTableName(), null);
			return rset;
		}
	}

	private void createRequiredTables() throws SQLException {
		if (!hasCreatedRequiredTables()) {
			Set<DBRow> tables = DataModel.getRequiredTables();
			for (DBRow table : tables) {
				updateTableToMatchDBRow(table);
			}
			setHasCreatedRequiredTables(true);
		}
	}

	/**
	 * Uses the supplied DBRow to update the existing database table by creating
	 * the table, if necessary, or adding any columns that are missing.
	 *
	 * @param table the database table representation that is correct
	 * @throws java.sql.SQLException database errors
	 */
	@Override
	public void updateTableToMatchDBRow(DBRow table) throws SQLException {
		if (!tableExists(table)) {
			createTable(table);
		} else {
			addMissingColumnsToTable(table);
		}
	}

	private synchronized void addMissingColumnsToTable(DBRow table) throws SQLException {
		executeDBAction(new DBAddMissingColumnsToTable(table));
	}

	/**
	 * Returns the port number usually assign to instances of this database.
	 *
	 * <p>
	 * There is no guarantee that the particular database instance uses this port,
	 * check with your DBA.</p>
	 *
	 * @return the usual database port number
	 */
	@Override
	public abstract Integer getDefaultPort();

	@Override
	public DatabaseConnectionSettings getSettings() {
		return settings;
	}

	protected void setSettings(DatabaseConnectionSettings newSettings) {
		settings.copy(newSettings);
		setDBDatabaseClassInSettings();
	}

	private void setDBDatabaseClassInSettings() {
		if (StringCheck.isEmptyOrNull(settings.getDbdatabaseClass())) {
			settings.setDbdatabaseClass(getBaseDBDatabaseClass().getCanonicalName());
		}
	}

	private void setDBDatabaseClassInSettings(SettingsBuilder<?, ?> suppliedSettings) {
		if (StringCheck.isEmptyOrNull(settings.getDbdatabaseClass())) {
			settings.setDbdatabaseClass(suppliedSettings.generatesURLForDatabase().getCanonicalName());
		}
	}

	protected void startServerIfRequired() {
		;
	}

	@Override
	public boolean isMemoryDatabase() {
		return false;
	}

	@Override
	public final Map<String, String> getExtras() {
		return getSettings().getExtras();
	}

	@Override
	public final String getHost() {
		return getSettings().getHost();
	}

	@Override
	public final String getDatabaseInstance() {
		return getSettings().getInstance();
	}

	@Override
	public final String getPort() {
		return getSettings().getPort();
	}

	@Override
	public final String getSchema() {
		return getSettings().getSchema();
	}

	/**
	 * Closes all threads, connections, and resources used by the database.
	 *
	 * <p>
	 * While it is not usually necessary to close a DBDatabase, this method should
	 * be used during shutdown to release all resources used by the database.
	 *
	 * <p>
	 * In particular the regular processing thread is stopped and the connection
	 * is shutdown and emptied.
	 *
	 * <p>
	 * Please note that this is very different using {@link DBDatabaseCluster#dismantle()
	 * }
	 *
	 */
	@Override
	public synchronized void stop() {
		terminated = true;
		String stopping = "STOPPING: " + this.getLabel();
		LOG.info(stopping);
		LOG.info(stopping + " Regular Processors");
		for (RegularProcess regularProcessor : getRegularProcessors()) {
			LOG.info(stopping + " " + regularProcessor.getSimpleName());
			regularProcessor.stop();
		}
		LOG.info(stopping + " Regular Processor");
		if (regularThreadPoolFuture != null) {
			regularThreadPoolFuture.cancel(true);
			regularThreadPoolFuture = null;
		}

		try {
			if (transactionStatement != null) {
				try {
					transactionStatement.close();
				} catch (SQLException ex) {
				}
			}
			if (transactionConnection != null) {
				try {
					LOG.info(stopping + " transaction connection");
					discardConnection(transactionConnection);
				} catch (Exception ex) {
				}
			}
			final List<DBConnection> freeConnections = getFreeConnections();
			synchronized (freeConnections) {
				final DBConnection[] free = freeConnections.toArray(new DBConnection[]{});
				for (DBConnection connection : free) {
					LOG.info(stopping + " free connection");
					discardConnection(connection);
				}
			}
			final List<DBConnection> busyConnections = getBusyConnections();
			synchronized (busyConnections) {
				final DBConnection[] busy = busyConnections.toArray(new DBConnection[]{});
				for (DBConnection connection : busy) {
					LOG.info(stopping + " busy connection");
					discardConnection(connection);
				}
			}
			try {
				if (storedConnection != null) {
					LOG.info(stopping + " stored connection");
					storedConnection.close();
				}
			} catch (SQLException ex) {
			}
		} catch (Exception ex) {
		}
	}

	@Override
	public synchronized boolean getPrintSQLBeforeExecuting() {
		return printSQLBeforeExecuting;
	}

	@Override
	public synchronized boolean getBatchSQLStatementsWhenPossible() {
		return batchIfPossible;
	}

	/**
	 * Creates a backup of this database in the specified database.
	 *
	 * @param backupDatabase the place to store all the data.
	 * @throws SQLException database errors
	 * @throws UnableToRemoveLastDatabaseFromClusterException Cluster may not
	 * remove their last database
	 */
	public void backupToDBDatabase(DBDatabase backupDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		String randomName = new BigInteger(130, new SecureRandom()).toString(32);
		DBDatabaseCluster cluster = new DBDatabaseCluster(randomName, DBDatabaseCluster.Configuration.autoStart());
		cluster.addDatabase(this);
		cluster.backupToDBDatabase(backupDatabase);
		cluster.dismantle();
	}

	private synchronized void startRegularProcessor() {
		if (regularThreadPoolFuture != null) {
			regularThreadPoolFuture.cancel(true);
		}
		regularThreadPoolFuture = getRegularThreadPool().scheduleWithFixedDelay(new RunRegularProcessors(), 1, 1, TimeUnit.SECONDS);
	}

	public final void addRegularProcess(RegularProcess processor) {
		processor.setDatabase(this);
		getRegularProcessors().add(processor);
	}

	public final void removeRegularProcess(RegularProcess processor) {
		processor.stop();
		getRegularProcessors().remove(processor);
	}

	protected final Class<? extends DBDatabase> getBaseDBDatabaseClass() {
		return getURLInterpreter().generatesURLForDatabase();

	}

	protected class RunRegularProcessors implements Runnable {

		public RunRegularProcessors() {
			super();
		}

		@Override
		public void run() {
			for (RegularProcess process : getRegularProcessors()) {
				if (process.canRun() && process.isDueToRun()) {
					try {
						if (process.preprocess()) {
							process.setLastResult(process.process());
						}
					} catch (Exception ex) {
						process.handleExceptionDuringProcessing(ex);
					} finally {
						process.postprocess();
						process.offsetTime();
					}
				}
			}
		}
	}

	private static class StopDatabase extends Thread {

		DBDatabase db;

		public StopDatabase(DBDatabase db) {
			this.db = db;
		}

		@Override
		public void run() {
			try {
				db.stop();
			} catch (Exception e) {
				LOG.info("Exception while stopping database " + db.getLabel(), e);
			}
		}
	}

	/**
	 * @return the REGULAR_PROCESSORS
	 */
	public final List<RegularProcess> getRegularProcessors() {
		return REGULAR_PROCESSORS;
	}

	/**
	 * @return the REGULAR_THREAD_POOL
	 */
	protected ScheduledExecutorService getRegularThreadPool() {
		return REGULAR_THREAD_POOL;
	}

	@Override
	public DBDatabaseMetaData getDBDatabaseMetaData(Options options) throws SQLException {
		return new DBDatabaseMetaData(options);
	}
}
