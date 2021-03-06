/*
 * Copyright 2017 gregorygraham.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.databases;

import java.lang.ref.Cleaner;
import nz.co.gregs.dbvolution.utility.ReconnectionProcess;
import java.lang.reflect.InvocationTargetException;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.databases.settingsbuilders.DBDatabaseClusterSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.ClusterDatabaseDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfDatabaseException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.transactions.DBTransaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Creates a database cluster programmatically.
 *
 * <p>
 * Clustering provides several benefits: automatic replication, reduced server
 * load on individual servers, improved server failure tolerance, and, with a
 * little programming, dynamic server replacement.</p>
 *
 * <p>
 * Please note that this class is not required to use database clusters provided
 * by database vendors. Use the normal DBDatabase subclass for those
 * vendors.</p>
 *
 * <p>
 * DBDatabaseCluster collects together several databases and ensures that all
 * actions are performed on all databases. This ensures that all databases stay
 * in synch and allows queries to be distributed to any database and produce the
 * same results. Different databases can be any supported database, for instance
 * the DBvolutionDemo application uses H2 and SQLite.</p>
 *
 * <p>
 * Upon creation, known tables and data are synchronized, the first database in
 * the cluster being used as the template. Added databases are synchronized
 * before being used</p>
 *
 * <p>
 * Automatically generated keys are still supported with a slight change: the
 * key will be generated in the first database and used as a literal value in
 * all other databases.
 *
 * @author gregorygraham
 */
public class DBDatabaseCluster extends DBDatabase {

	static final private Log LOG = LogFactory.getLog(DBDatabaseCluster.class);

	private static final long serialVersionUID = 1l;

	protected final ClusterDetails details;
	private transient final ExecutorService ACTION_THREAD_POOL;
	private final transient DBStatementCluster clusterStatement;

	public DBDatabaseCluster(DBDatabaseClusterSettingsBuilder builder) throws SQLException, InvocationTargetException, IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, NoSuchMethodException, ClassNotFoundException {
		this(builder.getDatabaseName(), new Configuration(builder.getAutoRebuild(), builder.getAutoReconnect()));
	}

	/**
	 * Nope.
	 *
	 * @return ClusterDetails
	 */
	public ClusterDetails getClusterDetails() {
		return details;
	}

	@Override
	public Integer getDefaultPort() {
		throw new UnsupportedOperationException("DBDatabaseCluster does not support getDefaultPort() yet.");
	}

	@Override
	public DBDatabaseClusterSettingsBuilder getURLInterpreter() {
		return new DBDatabaseClusterSettingsBuilder();
	}

	public static enum Status {

		READY,
		UNSYNCHRONISED,
		PAUSED,
		DEAD,
		QUARANTINED,
		UNKNOWN,
		PROCESSING
	}

	public DBDatabaseCluster() {
		this("", new Configuration(true, true));
	}

	public DBDatabaseCluster(String clusterLabel, Configuration config) {
		super();
		clusterStatement = new DBStatementCluster(this);
		details = new ClusterDetails();
		details.setClusterLabel(clusterLabel);
		details.setAutoRebuild(config.isUseAutoRebuild());
		details.setAutoReconnect(config.isUseAutoReconnect());
		setLabel(clusterLabel);
		ACTION_THREAD_POOL = Executors.newCachedThreadPool();
		final ReconnectionProcess reconnectionProcessor = new ReconnectionProcess();
		reconnectionProcessor.setTimeOffset(Calendar.MINUTE, 1);
		addRegularProcess(reconnectionProcessor);
		addCleaner();
	}

	public DBDatabaseCluster(String clusterLabel) {
		this(clusterLabel, new Configuration(true, true));
	}

	public DBDatabaseCluster(String clusterLabel, Configuration config, DBDatabase... databases) throws SQLException {
		this(clusterLabel, config);
		initDatabase(databases);
	}

	public DBDatabaseCluster(String clusterLabel, Configuration config, DatabaseConnectionSettings... settings) throws SQLException, InvocationTargetException, IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, NoSuchMethodException, ClassNotFoundException {
		this(clusterLabel, config);
		setDefinition(new ClusterDatabaseDefinition());
		for (DatabaseConnectionSettings setting : settings) {
			this.addDatabase(setting.createDBDatabase());
		}
	}

	public DBDatabaseCluster(String clusterLabel, DBDatabase... databases) throws SQLException {
		this(clusterLabel);
		initDatabase(databases);
	}

	public DBDatabaseCluster(String clusterLabel, DatabaseConnectionSettings... settings) throws SQLException, InvocationTargetException, IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, NoSuchMethodException, ClassNotFoundException {
		this(clusterLabel);
		setDefinition(new ClusterDatabaseDefinition());
		for (DatabaseConnectionSettings setting : settings) {
			this.addDatabase(setting.createDBDatabase());
		}
	}

	private void initDatabase(DBDatabase[] databases) throws SQLException {
		for (DBDatabase database : databases) {
			final DBDatabase databaseToAdd;
			databaseToAdd = database;
			details.add(databaseToAdd);
		}
		setDefinition(new ClusterDatabaseDefinition());
		synchronizeSecondaryDatabases();
	}

	/**
	 * Creates a new cluster with the configuration supplied.
	 *
	 * <p>
	 * Use this method to ensure that the new cluster will not clash with any
	 * existing clusters.</p>
	 *
	 * @param config
	 * @param databases
	 * @return
	 * @throws SQLException
	 */
	public static DBDatabaseCluster randomCluster(Configuration config, DBDatabase databases) throws SQLException {
		final String dbName = getRandomClusterName();
		return new DBDatabaseCluster(dbName, config, databases);
	}

	/**
	 * Creates a new cluster without auto-rebuild or auto-reconnect.
	 *
	 * <p>
	 * Use this method to ensure that the new cluster will not clash with any
	 * existing clusters.</p>
	 *
	 * @param databases
	 * @return
	 * @throws SQLException
	 */
	public static DBDatabaseCluster randomManualCluster(DBDatabase databases) throws SQLException {
		final String dbName = getRandomClusterName();
		return new DBDatabaseCluster(dbName, Configuration.manual(), databases);
	}

	/**
	 * Creates a new cluster with auto-rebuild and auto-reconnect.
	 *
	 * <p>
	 * Use this method to ensure that the new cluster will not clash with any
	 * existing clusters.</p>
	 *
	 * @param databases
	 * @return
	 * @throws SQLException
	 */
	public static DBDatabaseCluster randomAutomaticCluster(DBDatabase databases) throws SQLException {
		final String dbName = getRandomClusterName();
		return new DBDatabaseCluster(dbName, Configuration.autoRebuildAndReconnect(), databases);
	}

	private static String getRandomClusterName() {
		return "RandomClusterDB-" + UUID.randomUUID();
	}

	/**
	 * Removes all databases from the cluster then adds databases as defined by
	 * the settings.
	 *
	 * <p>
	 * Probably not a good idea to use this method but it allows the cluster to be
	 * set up as a bean, using the default constructor and a collection of
	 * settings.</p>
	 *
	 * @param settings an array of DatabaseConnectionSettings that can be used to
	 * add databases to the cluster
	 * @throws SQLException database errors
	 * @throws InvocationTargetException all database need an accessible default
	 * constructor
	 * @throws IllegalArgumentException all database need an accessible default
	 * constructor
	 * @throws IllegalAccessException all database need an accessible default
	 * constructor
	 * @throws InstantiationException all database need an accessible default
	 * constructor
	 * @throws SecurityException all database need an accessible default
	 * constructor
	 * @throws NoSuchMethodException all database need an accessible default
	 * constructor
	 * @throws ClassNotFoundException all database need an accessible default
	 * constructor
	 */
	public void setConnectionSettings(DatabaseConnectionSettings... settings) throws SQLException, InvocationTargetException, IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, NoSuchMethodException, ClassNotFoundException {
		removeDatabases(getDatabases());
		for (DatabaseConnectionSettings setting : settings) {
			this.addDatabase(setting.createDBDatabase());
		}
	}

	/**
	 * Sets the database name.
	 *
	 * @param databaseName	databaseName
	 */
	@Override
	final public synchronized void setDatabaseName(String databaseName) {
		super.setDatabaseName(databaseName);
		details.setClusterLabel(databaseName);
	}

	private synchronized DBStatement getClusterStatement() {
		return clusterStatement;
	}

	/**
	 * Adds a new database to this cluster.
	 *
	 * <p>
	 * The database will be synchronized and then made available for use.</p>
	 * <p>
	 * This is the non-blocking version of {@link #addDatabaseAndWait(nz.co.gregs.dbvolution.databases.DBDatabase)
	 * }.</p>
	 *
	 * @param database element to be appended to this list
	 * @return TRUE if the database has been added to the cluster.
	 * @throws java.sql.SQLException database errors
	 */
	public final synchronized boolean addDatabase(DBDatabase database) throws SQLException {
		return addDatabaseWithWaiting(database, false);
	}

	/**
	 * Adds a new database to this cluster.
	 *
	 * <p>
	 * The database will be synchronized and then made available for use.</p>
	 *
	 * <p>
	 * This is the blocking version of {@link #addDatabase(nz.co.gregs.dbvolution.databases.DBDatabase)
	 * }</p>
	 *
	 * @param database element to be appended to this list
	 * @return true if the database has been added to the cluster.
	 * @throws java.sql.SQLException database errors
	 */
	public synchronized boolean addDatabaseAndWait(DBDatabase database) throws SQLException {
		return addDatabaseWithWaiting(database, true);
	}

	private boolean addDatabaseWithWaiting(DBDatabase database, boolean wait) throws SQLException {
		boolean add = details.add(database);
		synchronizeAddedDatabases(wait);
		return add;
	}

	/**
	 * Returns all databases within this cluster.
	 *
	 * Please note, that you should probably NOT be using this method, rather just
	 * use the cluster like a normal DBDatabase.
	 *
	 * @return all the databases defined within the cluster
	 */
	public synchronized DBDatabase[] getDatabases() {
		return details.getAllDatabases();
	}

	public Status getDatabaseStatus(DBDatabase db) {
		return details.getStatusOf(db);
	}

	/**
	 * Adds the database to the cluster, synchronizes it, and then removes it.
	 *
	 * @param backupDatabase the database to use as a backup
	 * @throws SQLException database errors
	 * @throws UnableToRemoveLastDatabaseFromClusterException cluster cannot
	 * remove the last remaining database
	 */
	@Override
	public void backupToDBDatabase(DBDatabase backupDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		this.addDatabaseAndWait(backupDatabase);
		removeDatabase(backupDatabase);
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present (optional operation). If this list does not contain the element,
	 * it is unchanged. More formally, removes the element with the lowest index
	 * <code>i</code> such that
	 * <code>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</code>
	 * (if such an element exists). Returns true if this list contained the
	 * specified element (or equivalently, if this list changed as a result of the
	 * call).
	 *
	 * @param databases DBDatabases to be removed from this list, if present
	 * @return true if this list contained the specified element
	 * @throws UnableToRemoveLastDatabaseFromClusterException cluster cannot
	 * remove the last remaining database
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the quarantineDatabase operation
	 * is not supported by this list
	 */
	public synchronized boolean removeDatabases(List<DBDatabase> databases) throws UnableToRemoveLastDatabaseFromClusterException, SQLException {
		return removeDatabases(databases.toArray(new DBDatabase[]{}));
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present (optional operation). If this list does not contain the element,
	 * it is unchanged. More formally, removes the element with the lowest index i
	 * such that
	 * <code>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</code>
	 * (if such an element exists). Returns <code>true</code> if this list
	 * contained the specified element (or equivalently, if this list changed as a
	 * result of the call).
	 *
	 * @param databases DBDatabases to be removed from this list, if present
	 * @return true if this list contained the specified element
	 * @throws UnableToRemoveLastDatabaseFromClusterException cluster cannot
	 * remove the last remaining database
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the quarantineDatabase operation
	 * is not supported by this list
	 */
	public synchronized boolean removeDatabases(DBDatabase... databases) throws UnableToRemoveLastDatabaseFromClusterException, SQLException {
		for (DBDatabase database : databases) {
			removeDatabase(database);
		}
		return true;
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present (optional operation). If this list does not contain the element,
	 * it is unchanged. More formally, removes the element with the lowest index i
	 * such that
	 * <code>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</code>
	 * (if such an element exists). Returns <code>true</code> if this list
	 * contained the specified element (or equivalently, if this list changed as a
	 * result of the call).
	 *
	 * @param database DBDatabase to be removed from this list, if present
	 * @return <code>true</code> if this list contained the specified element
	 * @throws UnableToRemoveLastDatabaseFromClusterException cluster cannot
	 * remove the last remaining database
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the
	 * <code>quarantineDatabase</code> operation is not supported by this list
	 */
	public boolean removeDatabase(DBDatabase database) throws UnableToRemoveLastDatabaseFromClusterException, SQLException {
		return details.removeDatabase(database);
	}

	/**
	 * Places the database in quarantine within the cluster.
	 *
	 * <p>
	 * Auto-reconnecting clusters will try to restore quarantined databases.</p>
	 *
	 * @param database DBDatabase to be removed from this list, if present
	 * @param except the exception that caused the database to be quarantined
	 * @throws UnableToRemoveLastDatabaseFromClusterException cluster cannot
	 * remove the last remaining database
	 * @throws java.sql.SQLException
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the
	 * <code>quarantineDatabase</code> operation is not supported by this list
	 */
	public void quarantineDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		details.quarantineDatabase(database, except);
	}

	/**
	 * Returns a single random database that is ready for queries
	 *
	 * @return a ready database
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException the
	 * cluster is current unable to service requests
	 */
	public DBDatabase getReadyDatabase() throws NoAvailableDatabaseException {
		final DBDatabase ready = details.getReadyDatabase();
		return ready;
	}

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		throw new UnsupportedOperationException("DBDatabaseCluster.addFeatureToFixException(Exception) should not be called");
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		throw new UnsupportedOperationException("DBDatabaseCluster.addDatabaseSpecificFeatures(Statement) should not be called");
	}

	@Override
	public Connection getConnectionFromDriverManager() throws SQLException {
		throw new UnsupportedOperationException("DBDatabaseCluster.getConnectionFromDriverManager() should not be called");
	}

	@Override
	public synchronized void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		super.preventDroppingOfDatabases(justLeaveThisAtTrue);
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.preventDroppingOfDatabases(justLeaveThisAtTrue);
		}
	}

	@Override
	public synchronized void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		super.preventDroppingOfTables(droppingTablesIsAMistake);
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.preventDroppingOfTables(droppingTablesIsAMistake);
		}
	}

	@Override
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		super.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		}
	}

	@Override
	public synchronized boolean batchSQLStatementsWhenPossible() {
		super.batchSQLStatementsWhenPossible();
		boolean result = true;
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			result &= next.batchSQLStatementsWhenPossible();
		}
		return result;
	}

	@Override
	public synchronized void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, SQLException, ExceptionThrownDuringTransaction {
		preventDDLDuringTransaction("DBDatabase.dropDatabase()");
		if (getPreventAccidentalDroppingOfTables()) {
			throw new AccidentalDroppingOfTableException();
		}
		if (getPreventAccidentalDroppingOfDatabases()) {
			throw new AccidentalDroppingOfDatabaseException();
		}
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				try {
					next.dropDatabase(databaseName, doIt);
					finished = true;
				} catch (UnsupportedOperationException | SQLException | AutoCommitActionDuringTransactionException | ExceptionThrownDuringTransaction e) {
					if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
						throw e;
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void dropDatabase(boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, SQLException, UnableToRemoveLastDatabaseFromClusterException, ExceptionThrownDuringTransaction {
		preventDDLDuringTransaction("DBDatabase.dropDatabase()");
		if (getPreventAccidentalDroppingOfTables()) {
			throw new AccidentalDroppingOfTableException();
		}
		if (getPreventAccidentalDroppingOfDatabases()) {
			throw new AccidentalDroppingOfDatabaseException();
		}
		boolean finished = false;
		int tried = 0;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						tried++;
						next.dropDatabase(doIt);
						finished = true;
					} catch (UnsupportedOperationException | SQLException | AutoCommitActionDuringTransactionException | ExceptionThrownDuringTransaction e) {
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							throw e;
						}
					}
				}
			}
		} while (tried < 20 && !finished);
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) throws NoAvailableDatabaseException {
		return getReadyDatabase().willCreateBlankQuery(row);
	}

	@Override
	public synchronized <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, UnableToRemoveLastDatabaseFromClusterException {
		if (getPreventAccidentalDroppingOfTables()) {
			throw new AccidentalDroppingOfTableException();
		}
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.dropTableNoExceptions(tableRow);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException, UnableToRemoveLastDatabaseFromClusterException {
		if (getPreventAccidentalDroppingOfTables()) {
			throw new AccidentalDroppingOfTableException();
		}
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.dropTable(tableRow);
						finished = true;
					} catch (Exception e) {
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							throw e;
						}
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createIndexesOnAllFields(newTableRow);
						finished = true;
					} catch (Exception e) {
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							throw e;
						}
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.removeForeignKeyConstraints(newTableRow);
						finished = true;
					} catch (Exception e) {
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							throw e;
						}
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createForeignKeyConstraints(newTableRow);
						finished = true;
					} catch (Exception e) {
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							throw e;
						}
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createTableWithForeignKeys(newTableRow);
						finished = true;
					} catch (Exception e) {
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							throw e;
						}
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createTable(newTableRow, includeForeignKeyClauses);
						finished = true;
					} catch (Exception e) {
						System.out.println("nz.co.gregs.dbvolution.databases.DBDatabaseCluster.createTable(DBRow, boolean): " + e.getLocalizedMessage());
						e.printStackTrace();
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							System.out.println("nz.co.gregs.dbvolution.databases.DBDatabaseCluster.createTable(DBRow, boolean): " + e.getLocalizedMessage());
							throw e;
						}
					}
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTablesWithForeignKeysNoExceptions(newTables);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTablesNoExceptions(DBRow... newTables) {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTablesNoExceptions(newTables);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTablesNoExceptions(includeForeignKeyClauses, newTables);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTableNoExceptions(newTable);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTableNoExceptions(includeForeignKeyClauses, newTable);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public void updateTableToMatchDBRow(DBRow table) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.updateTableToMatchDBRow(table);
						finished = true;
					} catch (Exception e) {
						if (handleExceptionDuringQuery(e, next).equals(HandlerAdvice.ABORT)) {
							throw e;
						}
					}
				}
			}
		} while (!finished);
	}

	@Override
	public DBActionList test(DBScript script) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		final DBDatabase readyDatabase = getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.test(script);
		}
	}

	@Override
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		final DBDatabase readyDatabase = getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.doReadOnlyTransaction(dbTransaction);
		}
	}

	@Override
	public synchronized <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException {
		V result = null;
		boolean rollbackAll = false;
		List<DBDatabase> transactionDatabases = new ArrayList<>();
		try {
			final DBDatabase[] readyDatabases = details.getReadyDatabases();
			for (DBDatabase database : readyDatabases) {
				synchronized (database) {
					DBDatabase db;
					db = database.clone();
					transactionDatabases.add(db);
					V returnValues = null;
					db.transactionStatement = db.getDBTransactionStatement();
					try {
						db.isInATransaction = true;
						db.transactionConnection = db.transactionStatement.getConnection();
						db.transactionConnection.setAutoCommit(false);
						try {
							returnValues = dbTransaction.doTransaction(db);
							if (!commit) {
								try {
									db.transactionConnection.rollback();
								} catch (SQLException rollbackFailed) {
									discardConnection(db.transactionConnection);
								}
							}
						} catch (ExceptionThrownDuringTransaction ex) {
							try {
								db.transactionConnection.rollback();
							} catch (SQLException excp) {
								LOG.warn("Exception Occurred During Rollback: " + ex.getMessage());
							}
							throw ex;
						}
					} finally {
					}
					result = returnValues;
				}
			}
		} catch (Exception exc) {
			rollbackAll = true;
		} finally {
			for (DBDatabase db : transactionDatabases) {
				synchronized (db) {
					if (commit) {
						if (rollbackAll) {
							db.transactionConnection.rollback();
						} else {
							db.transactionConnection.commit();
						}
					}
					db.isInATransaction = false;
					db.transactionStatement.transactionFinished();
					db.discardConnection(db.transactionConnection);
					db.transactionConnection = null;
					db.transactionStatement = null;
				}
			}
		}
		return result;
	}

//	@Override
//	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.getRows(report, examples);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<>(0);
//	}
//	@Override
//	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.getAllRows(report, examples);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<>(0);
//	}
//	@Override
//	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.get(report, examples);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<>(0);
//	}
//	@Override
//	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow row, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.get(expectedNumberOfRows, row, rows);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException | UnexpectedNumberOfRowsException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<>(0);
//	}
//	@Override
//	public List<DBQueryRow> getByExamples(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.getByExamples(row, rows);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<>(0);
//	}
//	@Override
//	public List<DBQueryRow> get(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.get(row, rows);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<>(0);
//	}
//	@Override
//	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.getByExample(expectedNumberOfRows, exampleRow);
//				} catch (SQLException | AccidentalBlankQueryException | UnexpectedNumberOfRowsException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<R>(0);
//	}
//	@Override
//	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.get(expectedNumberOfRows, exampleRow);
//				} catch (SQLException | AccidentalBlankQueryException | NoAvailableDatabaseException | UnexpectedNumberOfRowsException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<R>(0);
//	}
//	@Override
//	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.getByExample(exampleRow);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					}
//				}
//			}
//		} while (!finished);
//		return new ArrayList<R>(0);
//	}
//	@Override
//	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
//		DBDatabase readyDatabase;
//		boolean finished = false;
//		do {
//			readyDatabase = getReadyDatabase();
//			synchronized (readyDatabase) {
//				try {
//					return readyDatabase.get(exampleRow);
//				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException e) {
//					if (handleExceptionDuringQuery(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
//						throw e;
//					};
//				}
//			}
//		} while (!finished);
//		return new ArrayList<R>(0);
//	}
	@Override
	public DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		throw new UnsupportedOperationException("DBDatabase.getConnection should not be used.");
	}

	@Override
	protected DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return getClusterStatement();
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	private void synchronizeSecondaryDatabases() throws SQLException {
		DBDatabase[] addedDBs;
		addedDBs = details.getUnsynchronizedDatabases();
		for (DBDatabase db : addedDBs) {
			details.synchronizingDatabase(db);

			//Do The Synchronising...
			synchronizeSecondaryDatabase(db);
		}
	}

	@Override
	public synchronized DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		addActionToQueue(action);
		List<ActionTask> tasks = new ArrayList<ActionTask>();
		DBActionList actionsPerformed = new DBActionList();
		try {
			DBDatabase readyDatabase = getReadyDatabase();
			boolean finished = false;
			do {
				try {
					if (action.requiresRunOnIndividualDatabaseBeforeCluster()) {
						// Because of autoincrement PKs we need to execute on one database first
						actionsPerformed = new ActionTask(this, readyDatabase, action).call();
						removeActionFromQueue(readyDatabase, action);
						finished = true;
					} else {
						finished = true;
					}
				} catch (SQLException e) {
					if (handleExceptionDuringAction(e, readyDatabase).equals(HandlerAdvice.ABORT)) {
						throw e;
					}
				}
			} while (!finished && size() > 1);
			final DBDatabase[] readyDatabases = details.getReadyDatabases();
			// Now execute on all the other databases
			for (DBDatabase next : readyDatabases) {
				if (action.runOnDatabaseDuringCluster(readyDatabase, next)) {
					final ActionTask task = new ActionTask(this, next, action);
					tasks.add(task);
					removeActionFromQueue(next, action);
				}
			}
			ACTION_THREAD_POOL.invokeAll(tasks);
		} catch (InterruptedException ex) {
			Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
			throw new DBRuntimeException("Unable To Run Actions", ex);
		}
		if (actionsPerformed.isEmpty()) {
			actionsPerformed = tasks.get(0).getActionList();
		}
		return actionsPerformed;
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, UnableToRemoveLastDatabaseFromClusterException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		try {
			return super.executeDBQuery(query);
		} catch (AccidentalBlankQueryException | AccidentalCartesianJoinException errorWithTheQueryException) {
			throw errorWithTheQueryException;
		} catch (NoAvailableDatabaseException errorWithTheClusterException) {
			throw errorWithTheClusterException;
		} catch (SQLException e) {
			if (handleExceptionDuringQuery(e, query.getWorkingDatabase()).equals(HandlerAdvice.ABORT)) {
				quarantineDatabaseAutomatically(query.getWorkingDatabase(), e);
				throw e;
			}
		}
		return query;
	}

	@Override
	public void handleErrorDuringExecutingSQL(DBDatabase suspectDatabase, Throwable sqlException, String sqlString) {
		quarantineDatabaseAutomatically(suspectDatabase, sqlException);
	}

	private void quarantineDatabaseAutomatically(DBDatabase suspectDatabase, Throwable sqlException) {
		try {
			this.quarantineDatabase(suspectDatabase, sqlException);
		} catch (UnableToRemoveLastDatabaseFromClusterException doesntNeedToBeHandledAsItsAutomaticAndNotManual) {
			;
		}
	}

	private static ArrayList<Class<? extends Exception>> ABORTING_EXCEPTIONS
			= new ArrayList<Class<? extends Exception>>() {
		private static final long serialVersionUID = 1l;

		{
			add(UnexpectedNumberOfRowsException.class);
			add(AutoCommitActionDuringTransactionException.class);
			add(AccidentalDroppingOfTableException.class);
			add(CloneNotSupportedException.class);
			add(AccidentalCartesianJoinException.class);
			add(AccidentalBlankQueryException.class);
			add(SQLTimeoutException.class);
		}
	};

	private static enum HandlerAdvice {
		REQUERY,
		SKIP,
		ABORT;
	}

	private HandlerAdvice handleExceptionDuringQuery(Exception e, final DBDatabase readyDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		if (ABORTING_EXCEPTIONS.contains(e.getClass())) {
			return HandlerAdvice.ABORT;
		} else {
			if (size() < 2) {
				return HandlerAdvice.ABORT;
			} else {
				quarantineDatabaseAutomatically(readyDatabase, e);
				return HandlerAdvice.REQUERY;
			}
		}
	}

	private HandlerAdvice handleExceptionDuringAction(Exception e, final DBDatabase readyDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		if (size() < 2) {
			return HandlerAdvice.ABORT;
		} else {
			quarantineDatabaseAutomatically(readyDatabase, e);
			return HandlerAdvice.REQUERY;
		}
	}

	@Override
	public String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException {
		final DBDatabase readyDatabase = this.getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.getSQLForDBQuery(query);
		}
	}

	ArrayList<DBStatement> getDBStatements() throws SQLException {
		ArrayList<DBStatement> arrayList = new ArrayList<>();
		final DBDatabase[] readyDatabases = details.getReadyDatabases();
		for (DBDatabase db : readyDatabases) {
			synchronized (db) {
				arrayList.add(db.getDBStatement());
			}
		}
		return arrayList;
	}

	@Override
	public DBDefinition getDefinition() throws NoAvailableDatabaseException {
		final DBDatabase readyDatabase = getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.getDefinition();
		}
	}

	@Override
	public void setPrintSQLBeforeExecuting(boolean b) {
		for (DBDatabase db : details.getAllDatabases()) {
			synchronized (db) {
				db.setPrintSQLBeforeExecuting(b);
			}
		}
	}

	@Override
	public boolean supportsMicrosecondPrecision() {
		boolean result = true;
		for (DBDatabase db : getDatabases()) {
			result = result && db.supportsMicrosecondPrecision();
			if (result == false) {
				return result;
			}
		}
		return result;
	}

	@Override
	public boolean supportsNanosecondPrecision() {
		boolean result = true;
		for (DBDatabase db : getDatabases()) {
			result = result && db.supportsNanosecondPrecision();
			if (result == false) {
				return result;
			}
		}
		return result;
	}

	@Override
	public boolean supportsDifferenceBetweenNullAndEmptyString() {
		return details.getSupportsDifferenceBetweenNullAndEmptyString();
	}

	private void addActionToQueue(DBAction action) {
		for (DBDatabase db : details.getAllDatabases()) {
			Queue<DBAction> queue = details.getActionQueue(db);
			queue.add(action);
		}
	}

	private void removeActionFromQueue(DBDatabase database, DBAction action) {
		final Queue<DBAction> queue = details.getActionQueue(database);
		synchronized (queue) {
			if (queue != null) {
				queue.remove(action);
			}
		}
	}

	private void synchronizeSecondaryDatabase(DBDatabase secondary) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		try {
			DBDatabase template = null;
			try {
				template = getTemplateDatabase();
			} catch (NoAvailableDatabaseException except) {
				// must be the first database
			}
			if (template != null) {
				// we need to unpause the template no matter wht happens so use a finally clause
				try {
					// Check that we're not synchronising the reference database
					if (!template.getSettings().equals(secondary.getSettings())) {
						final DBRow[] trackedTables = getTrackedTables();
						for (DBRow table : trackedTables) {
							// make sure the table exists in the cluster already
							if (template.tableExists(table)) {
								// Make sure it exists in the new database
								if (secondary.tableExists(table) == true) {
									secondary.preventDroppingOfTables(false);
									secondary.dropTableNoExceptions(table);
								}
								System.out.println("CREATING: " + table.getClass().getCanonicalName());
								secondary.createTable(table);
								// Check that the table has data
								final DBTable<DBRow> primaryTable = template.getDBTable(table);
								final DBTable<DBRow> secondaryTable = secondary.getDBTable(table);
								final Long primaryTableCount = primaryTable.count();
								if (primaryTableCount > 0) {
									final DBTable<DBRow> primaryData = primaryTable.setBlankQueryAllowed(true).setTimeoutToForever();
									// Check that the new database has data
									LOG.info("CLUSTER FILLING NEW DATABASE TABLE " + table.getTableName());
									List<DBRow> allRows = primaryData.getAllRows();
									secondaryTable.insert(allRows);
								}
							}
						}
					}
				} catch (Throwable e) {
					throw e;
				} finally {
					releaseTemplateDatabase(template);
				}
			}
			synchronizeActions(secondary);
		} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | AutoCommitActionDuringTransactionException ex) {
			quarantineDatabaseAutomatically(secondary, ex);
			throw ex;
		}
	}

	private synchronized void synchronizeActions(DBDatabase db) throws SQLException, NoAvailableDatabaseException, NoAvailableDatabaseException {
		if (db != null) {
			Queue<DBAction> queue = details.getActionQueue(db);
			while (queue != null && !queue.isEmpty()) {
				DBAction action = queue.remove();
				db.executeDBAction(action);
			}
			details.readyDatabase(db);
		}
	}

	private synchronized void synchronizeAddedDatabases(boolean blocking) throws SQLException {
		boolean block = blocking || (details.getReadyDatabases().length < 2);
		final DBDatabase[] dbs = details.getUnsynchronizedDatabases();
		for (DBDatabase addedDatabase : dbs) {
			SynchroniseTask task = new SynchroniseTask(this, addedDatabase);
			if (block) {
				task.synchronise(this, addedDatabase);
			} else {
				try {
					ACTION_THREAD_POOL.submit(task);
				} catch (RejectedExecutionException ex) {
					task.synchronise(this, addedDatabase);
				}
			}
		}
	}

	@Override
	public synchronized boolean tableExists(DBRow table) throws SQLException {
		boolean tableExists = true;
		for (DBDatabase readyDatabase : details.getReadyDatabases()) {
			synchronized (readyDatabase) {
				final boolean tableExists1 = readyDatabase.tableExists(table);
				tableExists &= tableExists1;
			}
		}
		return tableExists;
	}

	/**
	 * Returns the number of ready databases.
	 *
	 * <p>
	 * The size of the cluster is dynamic as databases are added, removed, and
	 * synchronized but this method returns the size of the cluster in terms of
	 * active databases at this point in time.</p>
	 *
	 * <ul>
	 * <li>DBDatabaseClusters within this cluster count as 1 database each.</li>
	 * <li>Unsynchronized databases are not counted by this method.</li>
	 * </ul>.
	 *
	 * @return the number of ready database.
	 */
	public int size() {
		return details.getReadyDatabases().length;
	}

	private synchronized void releaseTemplateDatabase(DBDatabase primary) throws SQLException, NoAvailableDatabaseException {
		if (primary != null) {
			if (details.clusterContainsDatabase(primary)) {
				synchronizeActions(primary);
			} else {
				primary.stop();
			}
		}
	}

	private DBDatabase getTemplateDatabase() throws NoAvailableDatabaseException {
		return details.getTemplateDatabase();
	}

	public String getClusterStatus() {
		final String summary = getStatusOfActiveDatabases();
		final String unsyn = getStatusOfUnsynchronisedDatabases();
		final String ejected = getStatusOfEjectedDatabases();
		return summary + "\n" + unsyn + "\n" + ejected;
	}

	private String getStatusOfEjectedDatabases() {
		return (new Date()).toString() + "Ejected Databases: " + details.getQuarantinedDatabases().size() + " of " + details.getAllDatabases().length;
	}

	private String getStatusOfUnsynchronisedDatabases() {
		return (new Date()).toString() + "Unsynchronised: " + details.getUnsynchronizedDatabases().length + " of " + details.getAllDatabases().length;
	}

	private String getStatusOfActiveDatabases() {
		final DBDatabase[] ready = details.getReadyDatabases();
		return (new Date()).toString() + "Active Databases: " + ready.length + " of " + details.getAllDatabases().length;
	}

	public String getDatabaseStatuses() {
		StringBuilder result = new StringBuilder();
		final DBDatabase[] all = details.getAllDatabases();
		for (DBDatabase db : all) {
			result.append(this.getDatabaseStatus(db).name())
					.append(": ")
					.append(db.getSettings().toString().replaceAll("DATABASECONNECTIONSETTINGS: ", ""))
					.append("\n");
		}
		return result.toString();
	}

	public final boolean getAutoRebuild() {
		return details.getAutoRebuild();
	}

	public final void setAutoRebuild(boolean b) {
		details.setAutoRebuild(b);
	}

	public final void setAutoReconnect(boolean b) {
		details.setAutoReconnect(b);
	}

	public final boolean getAutoReconnect() {
		return details.getAutoReconnect();
	}

	@Override
	public synchronized void stop() {
		try {
			shutdownClusterProcesses();
			LOG.info("STOPPING: contained databases");
			for (DBDatabase db : details.getAllDatabases()) {
				db.stop();
			}
			LOG.info("STOPPING: removing all databases");
			details.removeAllDatabases();
			super.stop();
		} catch (SQLException ex) {
			Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Override
	public void close() {
		dismantle();
	}

	/**
	 * Cleans up the cluster's databases after the cluster exits scope.
	 *
	 * <p>
	 * Removes all databases from the cluster without terminating them and
	 * shutdown all cluster processes.
	 *
	 * <p>
	 * Dismantling the cluster is only needed in a small number of scenarios,
	 * mostly testing.
	 *
	 * <p>
	 * Dismantling the cluster ends all threads, removes all databases, and
	 * removes the authoritative database configuration.
	 *
	 * <p>
	 * This process is similar to {@link DBDatabaseCluster#stop()
	 * } but does not stop or dismantle the individual databases.
	 */
	private static final Cleaner cleaner = Cleaner.create();

	private static class ClusterCleanupActions implements Runnable {

		private final ClusterDetails details;
		private final Log log;
		private final ExecutorService actionThreadPool;

		private ClusterCleanupActions(ClusterDetails details, Log log, ExecutorService actionThreadPool) {
			this.details = details;
			this.log = log;
			this.actionThreadPool = actionThreadPool;
		}

		@Override
		public void run() {
			log.info("CLEANING UP CLUSTER...");
			System.out.println("CLEANING UP CLUSTER...");
			actionThreadPool.shutdown();
			try {
				details.removeAllDatabases();
			} catch (SQLException ex) {
				Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	private ClusterCleanupActions clusterCleanupActions;
	private Cleaner.Cleanable cleanable;

	private void addCleaner() {
		clusterCleanupActions = new ClusterCleanupActions(details, LOG, ACTION_THREAD_POOL);
		cleanable = cleaner.register(this, clusterCleanupActions);
	}

	/**
	 * Removes all databases from the cluster without terminating them and
	 * shutdown all cluster processes.
	 *
	 * <p>
	 * Dismantling the cluster is only needed in a small number of scenarios,
	 * mostly testing.
	 *
	 * <p>
	 * Dismantling the cluster ends all threads, removes all databases, and
	 * removes the authoritative database configuration.
	 *
	 * <p>
	 * This process is similar to {@link DBDatabaseCluster#stop()
	 * } but does not stop or dismantle the individual databases.
	 */
	public synchronized void dismantle() {
		shutdownClusterProcesses();
		try {
			details.dismantle();
		} catch (SQLException ex) {
			Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private synchronized void shutdownClusterProcesses() {
		LOG.info("STOPPING: action thread pool");
		ACTION_THREAD_POOL.shutdown();
	}

	@Override
	public boolean isMemoryDatabase() {
		return !details.getAutoRebuild() || !details.hasAuthoritativeDatabase();
	}

//	@Override
	public synchronized void setRequiredToProduceEmptyStringsForNull(boolean required) {
		getClusterDetails().setSupportsDifferenceBetweenNullAndEmptyString(!required);
//		super.setRequiredToProduceEmptyStringsForNull(required);
	}

	private static class ActionTask implements Callable<DBActionList> {

		private final DBDatabase database;
		private final DBAction action;
		private final DBDatabaseCluster cluster;
		private DBActionList actionList = new DBActionList();

		public ActionTask(DBDatabaseCluster cluster, DBDatabase db, DBAction action) {
			this.cluster = cluster;
			this.database = db;
			this.action = action;
		}

		@Override
		public DBActionList call() throws SQLException, NoAvailableDatabaseException {
			try {
				DBActionList actions = database.executeDBAction(action);
				setActionList(actions);
				return getActionList();
			} catch (SQLException | NoAvailableDatabaseException e) {
				if (cluster.handleExceptionDuringAction(e, database).equals(HandlerAdvice.ABORT)) {
					throw e;
				}
			}
			return getActionList();
		}

		public synchronized DBActionList getActionList() {
			final DBActionList newList = new DBActionList();
			newList.addAll(actionList);
			return newList;
		}

		private synchronized void setActionList(DBActionList actions) {
			this.actionList = actions;
		}
	}

	private static class SynchroniseTask implements Callable<Void> {

		private final DBDatabaseCluster cluster;
		private final DBDatabase database;

		public SynchroniseTask(DBDatabaseCluster cluster, DBDatabase db) {
			this.cluster = cluster;
			this.database = db;
		}

		@Override
		final public Void call() throws Exception {
			return synchronise(getCluster(), getDatabase());
		}

		/**
		 * @return the cluster
		 */
		final public DBDatabaseCluster getCluster() {
			return cluster;
		}

		/**
		 * @return the database
		 */
		final public DBDatabase getDatabase() {
			return database;
		}

		final public Void synchronise(DBDatabaseCluster cluster, DBDatabase database) throws SQLException {
			cluster.synchronizeSecondaryDatabase(database);
			return null;
		}
	}

	public String reconnectQuarantinedDatabases() throws UnableToRemoveLastDatabaseFromClusterException, SQLException {
		StringBuilder str = new StringBuilder();
		DBDatabase[] ejecta = details.getQuarantinedDatabases().toArray(new DBDatabase[]{});
		for (DBDatabase ejected : ejecta) {
			str.append(ejected.getSettings());
			try {
				addDatabase(ejected);
				str.append("").append(ejected.getLabel()).append(" added");
			} catch (SQLException ex) {
				quarantineDatabase(ejected, ex);
				str.append("").append(ejected.getLabel()).append(" quarantined: ").append(ex.getLocalizedMessage());
			} finally {
				str.append("\n");
			}
		}
		return str.toString();
	}

	public DBRow[] getTrackedTables() {
		return details.getTrackedTables();
	}

	public void setTrackedTables(Collection<DBRow> rows) {
		details.setTrackedTables(rows);
	}

	public void addTrackedTable(DBRow row) {
		details.addTrackedTable(row);
	}

	public void addTrackedTables(Collection<DBRow> rows) {
		details.addTrackedTables(rows);
	}

	public static class Configuration {

		private boolean useAutoRebuild;
		private boolean useAutoReconnect;

		private Configuration(boolean useAutoRebuild, boolean useAutoReconnect) {
			this.useAutoRebuild = useAutoRebuild;
			this.useAutoReconnect = useAutoReconnect;
		}

		public static Configuration manual() {
			return new Configuration(false, false);
		}

		public static Configuration autoRebuild() {
			return new Configuration(true, false);
		}

		public static Configuration autoReconnect() {
			return new Configuration(false, true);
		}

		public static Configuration autoRebuildAndReconnect() {
			return new Configuration(true, true);
		}

		/**
		 * @return the useAutoRebuild
		 */
		public boolean isUseAutoRebuild() {
			return useAutoRebuild;
		}

		/**
		 * @return the useAutoReconnect
		 */
		public boolean isUseAutoReconnect() {
			return useAutoReconnect;
		}

		/**
		 * @param useAutoRebuild the useAutoRebuild to set
		 */
		public void setUseAutoRebuild(boolean useAutoRebuild) {
			this.useAutoRebuild = useAutoRebuild;
		}

		/**
		 * @param useAutoReconnect the useAutoReconnect to set
		 */
		public void setUseAutoReconnect(boolean useAutoReconnect) {
			this.useAutoReconnect = useAutoReconnect;
		}

	}
}
