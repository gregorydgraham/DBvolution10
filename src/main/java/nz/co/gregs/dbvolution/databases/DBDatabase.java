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
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBMigration;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryInsert;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRecursiveQuery;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.transactions.*;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.reflection.DataModel;
import nz.co.gregs.dbvolution.utility.RegularProcess;
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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public abstract class DBDatabase implements Serializable, Cloneable {
	
	private static final long serialVersionUID = 1l;
	static final Log LOG = LogFactory.getLog(DBDatabase.class);
	
	private String driverName = "";
	private boolean printSQLBeforeExecuting = false;
	boolean isInATransaction = false;
	DBTransactionStatement transactionStatement;
	private DBDefinition definition = null;
	private boolean batchIfPossible = true;
	private boolean preventAccidentalDroppingOfTables = true;
	private boolean preventAccidentalDroppingDatabase = true;
	private final Object getStatementSynchronizeObject = new Object();
	private final Object getConnectionSynchronizeObject = new Object();
	DBConnection transactionConnection;
	private static final transient Map<String, List<DBConnection>> BUSY_CONNECTIONS = new HashMap<>();
	private static final transient HashMap<String, List<DBConnection>> FREE_CONNECTIONS = new HashMap<>();
	private Boolean needToAddDatabaseSpecificFeatures = true;
	private final DatabaseConnectionSettings settings = new DatabaseConnectionSettings();
	private boolean terminated = false;
	private final List<RegularProcess> REGULAR_PROCESSORS = new ArrayList<>();
	private static final ScheduledExecutorService REGULAR_THREAD_POOL = Executors.newSingleThreadScheduledExecutor();
	private Exception exception = null;
	private ScheduledFuture<?> regularThreadPoolFuture;
	
	{
		Runtime.getRuntime().addShutdownHook(new StopDatabase(this));
	}
	
	@Override
	public String toString() {
		String jdbcURL = settings.getUrl();
		String databaseName = settings.getDatabaseName();
		String username = settings.getUsername();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final DBDatabase other = (DBDatabase) obj;
		if ((this.getDriverName() == null) ? (other.getDriverName() != null) : !this.getDriverName().equals(other.getDriverName())) {
			return false;
		}
		if ((this.getJdbcURL() == null) ? (other.getJdbcURL() != null) : !this.getJdbcURL().equals(other.getJdbcURL())) {
			return false;
		}
		if ((this.getUsername() == null) ? (other.getUsername() != null) : !this.getUsername().equals(other.getUsername())) {
			return false;
		}
		if ((this.getPassword() == null) ? (other.getPassword() != null) : !this.getPassword().equals(other.getPassword())) {
			return false;
		}
		if ((this.getDataSource() == null) ? (other.getDataSource() != null) : !this.getDataSource().equals(other.getDataSource())) {
			return false;
		}
		final DatabaseConnectionSettings thisSettings = this.getSettings();
		final DatabaseConnectionSettings otherSettings = other.getSettings();
		return !(thisSettings != otherSettings && (thisSettings == null || !thisSettings.equals(otherSettings)));
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
		SLEEP_BETWEEN_CONNECTION_RETRIES_MILLIS = 10;
		MAX_CONNECTION_RETRIES = 6;
		startRegularProcessor();
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
	 * @param driverName the class name of the database driver
	 * @param ds - a DataSource for the required database.
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
	public DBDatabase(DBDefinition definition, String driverName, DataSource ds) throws SQLException {
		this();
		setDBDatabaseClassInSettings();
		this.definition = definition;
		initDriver(driverName);
		settings.setDataSource(ds);
		setDBDatabaseClassInSettings();
		createRequiredTables();
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
	 * @param driverName the class name of the database driver
	 * @param dcs - a DatabaseConnectionSettings for the required database.
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
	public DBDatabase(DBDefinition definition, String driverName, DatabaseConnectionSettings dcs) throws SQLException {
		this();
		this.definition = definition;
		initDriver(driverName);
		this.settings.copy(dcs);
		setDBDatabaseClassInSettings();
		createRequiredTables();
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
	 * @throws java.sql.SQLException database errors
	 * @see DBDefinition
	 * @see OracleDB
	 * @see MySQLDB
	 * @see MSSQLServerDB
	 * @see H2DB
	 * @see H2MemoryDB
	 * @see InformixDB
	 * @see PostgresDB
	 */
	public DBDatabase(DBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		this();
		this.definition = definition;
		initDriver(driverName);
		setDBDatabaseClassInSettings();
		settings.setUrl(jdbcURL);
		settings.setUsername(username);
		settings.setPassword(password);
		DatabaseConnectionSettings set = this.getSettingsFromJDBCURL(jdbcURL);
		settings.setDatabaseName(set.getDatabaseName());
		settings.setExtras(set.getExtras());
		settings.setHost(set.getHost());
		settings.setInstance(set.getInstance());
		settings.setLabel(set.getLabel());
		settings.setPort(set.getPort());
		settings.setProtocol(set.getProtocol());
		settings.setSchema(set.getSchema());
		settings.setPort(set.getPort());
		createRequiredTables();
	}
	
	private void initDriver(String driverName1) {
		this.driverName = driverName1;
		try {
			Class.forName(this.driverName);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(DBDatabase.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	DBTransactionStatement getDBTransactionStatement() throws SQLException {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the DBStatement to be used: either a new one, or the current
	 * transaction statement.
	 * @throws java.sql.SQLException interacts with the database layer.
	 */
	public final DBStatement getDBStatement() throws SQLException {
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
	
	protected DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
					if (FREE_CONNECTIONS.isEmpty() || getConnectionList(FREE_CONNECTIONS).isEmpty()) {
						conn = getRawConnection();
					} else {
						conn = getConnectionList(FREE_CONNECTIONS).get(0);
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
						// load the driver
						Class.forName(getDriverName());
					} catch (ClassNotFoundException noDriver) {
						throw new UnableToFindJDBCDriver(getDriverName(), noDriver);
					}
					startServerIfRequired();
					while (connection == null) {
						try {
							connection = new DBConnectionSingle(this, getConnectionFromDriverManager());
							LOG.debug("NEW CONNECTION: " + this.getUrlFromSettings(settings));
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
						connection = new DBConnectionSingle(this, getDataSource().getConnection());
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
			return connection;
		}
		return null;
	}
	private final int SLEEP_BETWEEN_CONNECTION_RETRIES_MILLIS;
	private int MAX_CONNECTION_RETRIES = 6;

	/**
	 * Used to hold the database open if required by the database.
	 *
	 */
	protected DBConnection storedConnection;
	
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public final DBActionList save(DBRow row) throws SQLException {
		DBActionList changes = new DBActionList();
		final DBTable<DBRow> table = this.getDBTable(row);
		changes.addAll(table.save(row));
		return changes;
	}

	/**
	 *
	 * Inserts or updates DBRows into the correct tables automatically
	 *
	 * @param rows a DBRow
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public final DBActionList save(DBRow... rows) throws SQLException {
		return save(Arrays.asList(rows));
	}

	/**
	 *
	 * Inserts or updates DBRows into the correct tables automatically
	 *
	 * @param rows a DBRow
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public final DBActionList save(Collection<DBRow> rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			actions.addAll(save(row));
		}
		return actions;
	}

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param row a list of DBRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public final DBActionList insert(DBRow row) throws SQLException {
		DBActionList changes = new DBActionList();
		changes.addAll(this.getDBTable(row).insert(row));
		return changes;
	}

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a list of DBRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public final DBActionList insert(DBRow... listOfRowsToInsert) throws SQLException {
		if (listOfRowsToInsert.length > 0) {
			DBBulkInsert insert = new DBBulkInsert(listOfRowsToInsert[0]);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Inserts DBRows and Lists of DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a List of DBRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public final DBActionList insertOrUpdate(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException {
		DBActionList changes = new DBActionList();
		if (listOfRowsToInsert.size() > 0) {
			for (DBRow row : listOfRowsToInsert) {
				changes.addAll(this.getDBTable(row).insertOrUpdate(row));
			}
		}
		return changes;
	}
	
	protected DBActionList updateAnyway(List<DBRow> rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			actions.addAll(this.getDBTable(row).updateAnyway(row));
		}
		return actions;
	}

	/**
	 *
	 * Deletes DBRows from the correct tables automatically
	 *
	 * @param rows a list of DBRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Deletes DBRows from the correct tables automatically
	 *
	 * @param rows a list of DBRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	public final DBActionList deleteAll(DBRow... rows) throws SQLException {
		DBActionList changes = new DBActionList();
		for (DBRow row : rows) {
			changes.addAll(this.getDBTable(row).deleteAll(row));
		}
		return changes;
	}

	/**
	 *
	 * Deletes Lists of DBRows from the correct tables automatically
	 *
	 * @param list a list of DBRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 */
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBTable<R> dbTable = getDBTable(exampleRow);
		return dbTable.getAllRows();
	}

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the number of rows found based on the example.
	 *
	 * <p>
	 * See {@link nz.co.gregs.dbvolution.DBTable#count()}
	 *
	 * @param <R> the row affected
	 * @param exampleRow the example
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws AccidentalCartesianJoinException Thrown when a query will create a
	 * Cartesian Join and cartesian joins have not been explicitly permitted.
	 */
	public <R extends DBRow> long getCount(R exampleRow) throws SQLException, AccidentalCartesianJoinException {
		DBTable<R> dbTable = getDBTable(exampleRow).setBlankQueryAllowed(true);
		return dbTable.count();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException the exception thrown if the number
	 * of rows is wrong
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException the exception thrown when the
	 * number of rows is not correct
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown if a cluster is unable to service requests.
	 */
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return get(expectedNumberOfRows, exampleRow);
	}

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * @param row the first example
	 * @param rows the examples of the rows required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
	public List<DBQueryRow> getByExamples(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return get(row, rows);
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
				if (row != null) {
					System.out.println(row.toString());
				} else {
					System.out.println("null");
				}
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
	 * @param row the first example of the tables required
	 * @param rows examples of the tables required
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException thrown when the retrieved row count
	 * is wrong
	 * @see DBQuery
	 * @see DBQuery#getAllRows(long)
	 */
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow row, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		if (expectedNumberOfRows == null) {
			return get(row, rows);
		} else {
			return getDBQuery(row, rows).getAllRows(expectedNumberOfRows);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception from the transaction
	 * @see DBTransaction
	 * @see
	 * DBDatabase#doTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 * @see
	 * DBDatabase#doReadOnlyTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 */
	public synchronized <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, ExceptionThrownDuringTransaction {
		DBDatabase db;
		try {
			db = this.clone();
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
			} catch (SQLException ex) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception from the transaction
	 * @see DBTransaction
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception from the transaction
	 * @see DBTransaction
	 */
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return doTransaction(dbTransaction, false);
	}

	/**
	 * Convenience method to implement a DBScript on this database
	 *
	 * equivalent to script.implement(this);
	 *
	 * @param script the script to execute and commit
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList provided by the script
	 * @throws java.sql.SQLException database errors
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception from the transaction
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown when a cluster cannot service requests
	 */
	public DBActionList test(DBScript script) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return script.test(this);
	}

	/**
	 * Returns the name of the JDBC driver class used by this DBDatabase instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the driverName
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the jdbcURL
	 */
	public final synchronized String getJdbcURL() {
		return getUrlFromSettings(getSettings());
	}

	/**
	 * Returns the username specified for this DBDatabase instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the username
	 */
	final public synchronized String getUsername() {
		return settings.getUsername();
	}

	/**
	 * Returns the password specified
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the password
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param example the example rows that are required in the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBQuery with the examples as required tables
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBQuery with the examples as required tables
	 */
	public DBQuery getDBQuery(DBRow example, DBRow... examples) {
		return DBQuery.getInstance(this, example, examples);
	}

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param examples the example rows that are required in the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBQuery with the examples as required tables
	 */
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
	public synchronized void setPrintSQLBeforeExecuting(boolean b) {
		printSQLBeforeExecuting = b;
	}

	/**
	 * Indicates whether SQL will be printed before it is executed.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
		LOG.debug(sqlString);
	}
	
	synchronized void printSQLIfRequested(String sqlString, PrintStream out) {
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
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		createTable(newTableRow, true);
	}
	
	public final synchronized String getSQLForCreateTable(DBRow newTableRow, boolean includeForeignKeyClauses) {
		return getSQLForCreateTable(newTableRow, includeForeignKeyClauses, new ArrayList<PropertyWrapper>(), new ArrayList<PropertyWrapper>());
	}
	
	private synchronized String getSQLForCreateTable(DBRow newTableRow, boolean includeForeignKeyClauses, List<PropertyWrapper> pkFields, List<PropertyWrapper> spatial2DFields) {
		StringBuilder sqlScript = new StringBuilder();
		String lineSeparator = System.getProperty("line.separator");
		// table name

		sqlScript.append(definition.getCreateTableStart()).append(definition.formatTableName(newTableRow)).append(definition.getCreateTableColumnsStart()).append(lineSeparator);

		// columns
		String sep = "";
		String nextSep = definition.getCreateTableColumnsSeparator();
		List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
		List<String> fkClauses = new ArrayList<>();
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
		return sqlScript.toString();
	}
	
	private synchronized void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		
		preventDDLDuringTransaction("DBDatabase.createTable()");
		
		List<PropertyWrapper> pkFields = new ArrayList<>();
		List<PropertyWrapper> spatial2DFields = new ArrayList<>();
		
		String sqlString = getSQLForCreateTable(newTableRow, includeForeignKeyClauses, pkFields, spatial2DFields);
		try (DBStatement dbStatement = getDBStatement()) {
			dbStatement.execute(sqlString, QueryIntention.CREATE_TABLE);

			//Oracle style trigger based auto-increment keys
			if (definition.prefersTriggerBasedIdentities() && pkFields.size() == 1) {
				List<String> triggerBasedIdentitySQL = definition.getTriggerBasedIdentitySQL(this, definition.formatTableName(newTableRow), definition.formatColumnName(pkFields.get(0).columnName()));
				for (String sql : triggerBasedIdentitySQL) {
					try {
						dbStatement.execute(sql, QueryIntention.CREATE_TRIGGER);
					} catch (SQLException sqlex) {
						sqlex.printStackTrace();
//						ORA-04080: trigger 'MARQUE_UID_MARQUEDTG' does not exist
					}
				}
			}
			
			if (definition.requiresSpatial2DIndexes() && spatial2DFields.size() > 0) {
				List<String> triggerBasedIdentitySQL = definition.getSpatial2DIndexSQL(this, definition.formatTableName(newTableRow), definition.formatColumnName(spatial2DFields.get(0).columnName()));
				for (String sql : triggerBasedIdentitySQL) {
					dbStatement.execute(sql, QueryIntention.CREATE_TRIGGER);
				}
			}
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
	public synchronized void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		if (this.definition.supportsAlterTableAddConstraint()) {
			List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
			List<String> fkClauses = new ArrayList<>();
			for (PropertyWrapper field : fields) {
				if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
					final String alterTableAddForeignKeyStatement = definition.getAlterTableAddForeignKeyStatement(newTableRow, field);
					if (!alterTableAddForeignKeyStatement.isEmpty()) {
						fkClauses.add(alterTableAddForeignKeyStatement);
					}
				}
			}
			if (fkClauses.size() > 0) {
				try (DBStatement statement = getDBStatement()) {
					for (String fkClause : fkClauses) {
						statement.execute(fkClause, QueryIntention.CREATE_FOREIGN_KEY);
					}
				}
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
	public synchronized void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		
		List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
		List<String> fkClauses = new ArrayList<>();
		for (PropertyWrapper field : fields) {
			if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
				final String alterTableDropForeignKeyStatement = definition.getAlterTableDropForeignKeyStatement(newTableRow, field);
				if (!alterTableDropForeignKeyStatement.isEmpty()) {
					fkClauses.add(alterTableDropForeignKeyStatement);
				}
			}
		}
		if (fkClauses.size() > 0) {
			try (DBStatement statement = getDBStatement()) {
				for (String fkClause : fkClauses) {
					statement.execute(fkClause, QueryIntention.DROP_FOREIGN_KEY);
				}
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
	public synchronized void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		
		List<PropertyWrapper> fields = newTableRow.getColumnPropertyWrappers();
		List<String> indexClauses = new ArrayList<>();
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
			try (DBStatement statement = getDBStatement()) {
				for (String indexClause : indexClauses) {
					statement.execute(indexClause, QueryIntention.CREATE_INDEX);
				}
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
	public synchronized void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		preventDDLDuringTransaction("DBDatabase.dropTable()");
		if (preventAccidentalDroppingOfTables) {
			throw new AccidentalDroppingOfTableException();
		}
		StringBuilder sqlScript = new StringBuilder();
		final String dropTableStart = definition.getDropTableStart();
		final String formatTableName = definition.formatTableName(tableRow);
		final String endSQLStatement = definition.endSQLStatement();
		
		sqlScript.append(dropTableStart).append(formatTableName).append(endSQLStatement);
		String sqlString = sqlScript.toString();
		try (DBStatement dbStatement = getDBStatement()) {
			dbStatement.execute(sqlString, QueryIntention.DROP_TABLE);
			dropAnyAssociatedDatabaseObjects(dbStatement, tableRow);
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
	public <TR extends DBRow> void dropTableIfExists(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, SQLException {
		if (tableExists(tableRow)) {
			this.dropTable(tableRow);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the DBDefinition used by this DBDatabase instance
	 */
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the specified row has no specified criteria, FALSE
	 * otherwise
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown when a cluster cannot service requests
	 */
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
	 */
	public synchronized void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfDatabaseException, SQLException, ExceptionThrownDuringTransaction {
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
			try {
				this.doTransaction(new DBRawSQLTransaction(dropStr));
			} catch (SQLException | ExceptionThrownDuringTransaction ex) {
				throw new UnableToDropDatabaseException(ex);
			}
		}
		preventAccidentalDroppingOfTables = true;
		preventAccidentalDroppingDatabase = true;
	}

	/**
	 * Returns the database name if one was supplied.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the database name
	 */
	final public synchronized String getDatabaseName() {
		return settings.getDatabaseName();
	}

	/**
	 * Sets the database name.
	 *
	 * @param databaseName	databaseName
	 */
	public synchronized void setDatabaseName(String databaseName) {
		settings.setDatabaseName(databaseName);
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
		settings.setLabel(label);
//		this.label = label;
	}

	/**
	 * A label for the database for reference within an application.
	 *
	 * <p>
	 * This label has no effect on the actual database connection.
	 *
	 * @return the internal label of this database
	 */
	final public String getLabel() {
		return settings.getLabel();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this instance will try to batch SQL statements, FALSE
	 * otherwise
	 */
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
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		batchIfPossible = batchSQLStatementsWhenPossible;
	}
	
	protected synchronized void preventDDLDuringTransaction(String message) throws AutoCommitActionDuringTransactionException {
		if (isInATransaction) {
			throw new AutoCommitActionDuringTransactionException(message);
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
	public synchronized void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		this.preventAccidentalDroppingDatabase = justLeaveThisAtTrue;
	}
	
	public synchronized boolean getPreventAccidentalDroppingOfDatabases() {
		return this.preventAccidentalDroppingDatabase;
	}

	/**
	 * Get The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows from the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> DBReport type
	 * @param report report
	 * @param examples examples
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A List of instances of the supplied report from the database 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
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
	 * Retrieves a list of report rows from the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> the DBReport to be derived from the database data.
	 * @param report the report to be produced
	 * @param examples DBRow subclasses that provide extra criteria
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A list of the DBreports generated
	 * @throws SQLException database exceptions
	 */
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return DBReport.getAllRows(this, report, examples);
	}

	/**
	 * Get The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows from the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> DBReport type
	 * @param report report
	 * @param examples examples
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A List of instances of the supplied report from the database 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return DBReport.getRows(this, report, examples);
	}

	/**
	 * Provided to allow DBDatabase sub-classes to tweak their connections before
	 * use.
	 *
	 * <p>
	 * Used by {@link SQLiteDB} in particular.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return The connection configured ready to use. 1 Database exceptions may
	 * be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	protected Connection getConnectionFromDriverManager() throws SQLException {
		if (terminated) {
			return null;
		} else {
			try {
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
//			this.jdbcURL = jdbcURL;
		}
	}

	/**
	 * @param username the username to set
	 */
	final protected synchronized void setUsername(String username) {
		if (FREE_CONNECTIONS.isEmpty()) {
			settings.setUsername(username);
//			this.username = username;
		}
	}

	/**
	 * @param password the password to set
	 */
	final protected synchronized void setPassword(String password) {
		if (FREE_CONNECTIONS.isEmpty()) {
			settings.setPassword(password);
//			this.password = password;
		}
	}

	/**
	 * Called after DROP TABLE to allow the DBDatabase to clean up any extra
	 * objects created with the table.
	 *
	 * @param <R> DBRow type
	 * @param dbStatement statement for executing the changes, don't close it!
	 * @param tableRow tableRow
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@SuppressWarnings("empty-statement")
	protected <R extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, R tableRow) throws SQLException {
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
	public synchronized void unusedConnection(DBConnection connection) throws SQLException {
		if (supportsPooledConnections()) {
			List<DBConnection> busy = getConnectionList(BUSY_CONNECTIONS);
			busy.remove(connection);
			getConnectionList(FREE_CONNECTIONS).add(connection);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the DBDatabase supports connection pooling, FALSE
	 * otherwise.
	 */
	protected boolean supportsPooledConnections() {
		return true;
	}
	
	private synchronized void usedConnection(DBConnection connection) {
		if (supportsPooledConnections()) {
			getConnectionList(FREE_CONNECTIONS).remove(connection);
			getConnectionList(BUSY_CONNECTIONS).add(connection);
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
	public synchronized void discardConnection(DBConnection connection) {
		if (connection != null) {
			getConnectionList(BUSY_CONNECTIONS).remove(connection);
			getConnectionList(FREE_CONNECTIONS).remove(connection);
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
	 * Used By Subclasses To Inject Datatypes, Functions, Etc Into the Database.
	 *
	 * @param statement the statement to use when adding features, DO NOT CLOSE
	 * THIS STATEMENT.
	 * @throws ExceptionDuringDatabaseFeatureSetup database exceptions may occur
	 * @see PostgresDB
	 * @see H2DB
	 * @see SQLiteDB
	 * @see OracleDB
	 * @see MSSQLServerDB
	 * @see MySQLDB
	 */
	abstract protected void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup;

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
	 * @return the preferred response to the exception
	 * @throws SQLException accessing the database may cause exceptions
	 */
	protected ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		throw exp;
	}

	/**
	 * Create a DBRecursiveQuery based on the query and foreign key supplied.
	 *
	 * <p>
	 * DBRecursiveQuery uses the query to create the first rows of the recursive
	 * query. This can be any query and contain any tables. However it must
	 * contain the table T and the foreign key must be a recursive foreign key
	 * (FK) to and from table T.
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
	public <T extends DBRow> DBRecursiveQuery<T> getDBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow, T dbRow) {
		return new DBRecursiveQuery<T>(query, keyToFollow);
	}
	
	public boolean isDBDatabaseCluster() {
		return (this instanceof DBDatabaseCluster);
	}
	
	protected final DataSource getDataSource() {
		return settings.getDataSource();
	}
	
	public void setLastException(Exception except) {
		this.exception = except;
	}
	
	public Exception getLastException() {
		return this.exception;
	}
	
	protected void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		;
	}
	
	public static enum ResponseToException {
		REPLACECONNECTION(),
		REQUERY(),
		SKIPQUERY();
		
		ResponseToException() {
		}
	}
	
	public <K extends DBRow> DBQueryInsert<K> getDBQueryInsert(K mapper) {
		return new DBQueryInsert<>(this, mapper);
	}

	/**
	 * Creates a DBmigration that will do a conversion from one or more database
	 * tables to another database table.
	 *
	 * <p>
	 * The mapper class should be an extension of a intended DBRow class that has
	 * instances of the DBRows required in the query, constraints, and mappings to
	 * fill the required columns set in the initialization clause. </p>
	 *
	 * <p>
	 * See DBMigrationTest for examples.</p>
	 *
	 * @param <K> the DBRow extension that maps fields of internal DBRows to all
	 * the fields of it's superclass.
	 * @param mapper a class that can be used to map one or more database tables
	 * to a single table.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBQueryInsert for the mapper class
	 */
	public <K extends DBRow> DBMigration<K> getDBMigration(K mapper) {
		return new DBMigration<>(this, mapper);
	}
	
	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		return action.execute(this);
	}
	
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return query.query(this);
	}
	
	public String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException {
		return query.toSQLString(this);
	}
	
	@SuppressFBWarnings(
			value = "REC_CATCH_EXCEPTION",
			justification = "Database vendors throw all sorts of silly exceptions")
	public boolean tableExists(DBRow table) throws SQLException {
		boolean tableExists = false;
		
		if (getDefinition().supportsTableCheckingViaMetaData()) {
			try (DBStatement dbStatement = getDBStatement()) {
				DBConnection conn = dbStatement.getConnection();
				ResultSet rset = conn.getMetaData().getTables(null, null, table.getTableName(), null);
				if (rset.next()) {
					tableExists = true;
				}
			}
		} else {
			String testQuery = getDefinition().getTableExistsSQL(table);
			try (DBStatement dbStatement = getDBStatement()) {
				ResultSet results = dbStatement.executeQuery(
						testQuery,
						"CHECK FOR TABLE " + table.getTableName(),
						QueryIntention.CHECK_TABLE_EXISTS);
				if (results != null) {
					results.close();
				}
				tableExists = true;
			} catch (Exception ex) {
//				ex.printStackTrace();
				// Theoretically this should only need to catch an SQLException 
				// but databases throw allsorts of weird exceptions
			}
		}
		return tableExists;
	}
	
	boolean tableExists(Class<? extends DBRow> tab) throws SQLException {
		return tableExists(DBRow.getDBRow(tab));
	}
	
	private void createRequiredTables() throws SQLException {
		Set<DBRow> tables = DataModel.getRequiredTables();
		for (DBRow table : tables) {
			updateTableToMatchDBRow(table);
		}
	}

	/**
	 * Uses the supplied DBRow to update the existing database table by creating
	 * the table, if necessary, or adding any columns that are missing.
	 *
	 * @param table the database table representation that is correct
	 * @throws java.sql.SQLException database errors
	 */
	public void updateTableToMatchDBRow(DBRow table) throws SQLException {
		if (!tableExists(table)) {
			createTable(table);
		} else {
			addMissingColumnsToTable(table);
		}
	}
	
	private synchronized void addMissingColumnsToTable(DBRow table) throws SQLException {
		
		List<PropertyWrapper> newColumns = new ArrayList<>();
		String testQuery = getDBTable(table)
				.setQueryTimeout(10000)
				.setBlankQueryAllowed(true)
				.setRowLimit(1).getSQLForQuery().replaceAll("(?is)SELECT .* FROM", "SELECT * FROM");
		try (DBStatement dbStatement = getDBStatement()) {
			try (ResultSet resultSet = dbStatement.executeQuery(
					testQuery,
					"CHECK TABLE STRUCTURE FOR " + table.getTableName(),
					QueryIntention.SIMPLE_SELECT_QUERY)) {
				ResultSetMetaData metaData = resultSet.getMetaData();
				List<PropertyWrapper> columnPropertyWrappers = table.getColumnPropertyWrappers();
				for (PropertyWrapper columnPropertyWrapper : columnPropertyWrappers) {
					if (!columnPropertyWrapper.hasColumnExpression()) {
						int columnCount = metaData.getColumnCount();
						boolean foundColumn = false;
						for (int i = 1; i <= columnCount && !foundColumn; i++) {
							String columnName = definition.formatColumnName(metaData.getColumnName(i));
							String formattedPropertyColumnName = definition.formatColumnName(columnPropertyWrapper.columnName());

							/*Postgres returns a lowercase column name in the meta data so use case-insensitive check*/
							if (columnName.equalsIgnoreCase(formattedPropertyColumnName)) {
								foundColumn = true;
							}
						}
						if (!foundColumn) {
							// We collect all the changes and process them later because SQLite doesn't like processing them imediately
							newColumns.add(columnPropertyWrapper);
						}
					}
				}
			}
		} catch (Exception ex) {
			LOG.warn("Error occurred while adding columns to required table", ex);
			// Theoretically this should only need to catch an SQLException 
			// but databases throw allsorts of weird exceptions
		}
		for (PropertyWrapper newColumn : newColumns) {
			alterTableAddColumn(table, newColumn);
		}
	}
	
	private synchronized void alterTableAddColumn(DBRow existingTable, PropertyWrapper columnPropertyWrapper) {
		preventDDLDuringTransaction("DBDatabase.alterTable()");
		
		String sqlString = definition.getAlterTableAddColumnSQL(existingTable, columnPropertyWrapper);
		
		try (DBStatement dbStatement = getDBStatement()) {
			try {
				boolean execute = dbStatement.execute(sqlString, QueryIntention.ADD_COLUMN_TO_TABLE);
			} catch (SQLException ex) {
//				System.err.println("nz.co.gregs.dbvolution.databases.DBDatabase.alterTableAddColumn() " + ex.getLocalizedMessage());
				Logger.getLogger(DBDatabase.class.getName()).log(Level.SEVERE, null, ex);
			}
		} catch (SQLException ex) {
//			System.err.println("nz.co.gregs.dbvolution.databases.DBDatabase.alterTableAddColumn() " + ex.getLocalizedMessage());
			Logger.getLogger(DBDatabase.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	protected abstract String getUrlFromSettings(DatabaseConnectionSettings settings);
	
	protected abstract DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL);
	
	public abstract Integer getDefaultPort();
	
	public DatabaseConnectionSettings getSettings() {
//		if (settings == null) {
//			DatabaseConnectionSettings newSettings = new DatabaseConnectionSettings();
//			newSettings.setLabel(getLabel());
//			newSettings.setDatabaseName(getDatabaseName());
//			newSettings.setDbdatabaseClass(this.getClass().getCanonicalName());
//			newSettings.setExtras(getExtras());
//			newSettings.setHost(getHost());
//			newSettings.setInstance(getDatabaseInstance());
//			newSettings.setPassword(getPassword());
//			newSettings.setPort(getPort());
//			newSettings.setSchema(getSchema());
//			newSettings.setUrl(getJdbcURL());
//			newSettings.setUsername(getUsername());
//			this.settings = newSettings;
//		}
//		setDBDatabaseClassInSettings();
		return settings;
	}
	
	protected void setSettings(DatabaseConnectionSettings newSettings) {
		settings.copy(newSettings);
		setDBDatabaseClassInSettings();
	}
	
	private void setDBDatabaseClassInSettings() {
		settings.setDbdatabaseClass(getBaseDBDatabaseClass().getCanonicalName());
	}
	
	protected void startServerIfRequired() {
		;
	}
	
	public boolean isMemoryDatabase() {
		return false;
	}
	
	protected final Map<String, String> getExtras() {
		return settings.getExtras();
	}
	
	protected final String getHost() {
		return settings.getHost();
	}
	
	protected final String getDatabaseInstance() {
		return settings.getInstance();
	}
	
	protected final String getPort() {
		return settings.getPort();
	}
	
	protected final String getSchema() {
		return settings.getSchema();
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
	 * Please note that this is very different from {@link DBDatabaseCluster#dismantle()
	 * }
	 *
	 */
	public synchronized void stop() {
		terminated = true;
		for (RegularProcess regularProcessor : getRegularProcessors()) {
			regularProcessor.stop();
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
					discardConnection(transactionConnection);
				} catch (Exception ex) {
				}
			}
			final List<DBConnection> freeConnections = getConnectionList(FREE_CONNECTIONS);
			synchronized (freeConnections) {
				final DBConnection[] free = freeConnections.toArray(new DBConnection[]{});
				for (DBConnection connection : free) {
					discardConnection(connection);
				}
			}
			final List<DBConnection> busyConnections = getConnectionList(BUSY_CONNECTIONS);
			synchronized (busyConnections) {
				final DBConnection[] busy = busyConnections.toArray(new DBConnection[]{});
				for (DBConnection connection : busy) {
					discardConnection(connection);
				}
			}
			try {
				if (storedConnection != null) {
					storedConnection.close();
				}
			} catch (SQLException ex) {
			}
		} catch (Exception ex) {
		}
	}
	
	public boolean getPrintSQLBeforeExecuting() {
		return printSQLBeforeExecuting;
	}
	
	public boolean getBatchSQLStatementsWhenPossible() {
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
		DBDatabaseCluster cluster = new DBDatabaseCluster(randomName, DBDatabaseCluster.Configuration.manual());
		cluster.addDatabase(this);
		cluster.backupToDBDatabase(backupDatabase);
		cluster.dismantle();
	}
	
	private synchronized void startRegularProcessor() {
		if (regularThreadPoolFuture != null) {
			regularThreadPoolFuture.cancel(true);
		}
		regularThreadPoolFuture = getRegularThreadPool().scheduleWithFixedDelay(new RunRegularProcessors(), 1, 1, TimeUnit.MINUTES);
	}
	
	public final void addRegularProcess(RegularProcess processor) {
		processor.setDatabase(this);
		getRegularProcessors().add(processor);
	}
	
	public final void removeRegularProcess(RegularProcess processor) {
		processor.setDatabase(null);
		getRegularProcessors().remove(processor);
	}
	
	protected abstract Class<? extends DBDatabase> getBaseDBDatabaseClass();
	
	protected class RunRegularProcessors implements Runnable {
		
		public RunRegularProcessors() {
			super();
		}
		
		@Override
		public void run() {
			for (RegularProcess process : getRegularProcessors()) {
				if (process.canRun() && process.hasExceededTimeLimit()) {
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
	
	private class StopDatabase extends Thread {
		
		DBDatabase db;
		
		public StopDatabase(DBDatabase db) {
			this.db = db;
		}
		
		@Override
		public void run() {
			db.stop();
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
}
