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

import java.io.Serializable;
import java.lang.ref.Cleaner;
import nz.co.gregs.dbvolution.utility.ReconnectionProcess;
import java.lang.reflect.InvocationTargetException;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.actions.*;
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
import nz.co.gregs.dbvolution.utility.LoopVariable;
import nz.co.gregs.dbvolution.internal.database.ClusterCleanupActions;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Creates a database cluster programmatically.
 *
 * <p>
 * Clustering provides several benefits: automatic replication, reduced server
 * load on individual servers, improved server failure tolerance, and dynamic
 * server replacement.</p>
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
 * Upon creation, known and required tables are synchronized, the first database
 * in the cluster being used as the template. Added databases are synchronized
 * before being used</p>
 *
 * <p>
 * Automatically generated keys are still supported with a slight change: the
 * key will be generated in the first database and used as a literal value in
 * all other databases.</p>
 *
 * <p>
 * Adding an Oracle database to the cluster will change the cluster to
 * Oracle-compatible mode: null strings and empty strings will be equivalent.
 * This may change the results of your queries.</p>
 *
 * @author gregorygraham
 */
public class DBDatabaseCluster extends DBDatabase {

	static final private Log LOG = LogFactory.getLog(DBDatabaseCluster.class);

	private static final long serialVersionUID = 1l;

	private ClusterDetails details;
	private transient final ExecutorService ACTION_THREAD_POOL;
	private boolean requeryPermitted = true;
	private LoopVariable startup = new LoopVariable();

	public DBDatabaseCluster(DBDatabaseClusterSettingsBuilder builder) throws SQLException {
		super(builder);
		Configuration config = builder.getConfiguration();
		final ClusterDetails clusterDetails = getDetails();
		clusterDetails.setConfiguration(config);

		ACTION_THREAD_POOL = Executors.newCachedThreadPool();

		if (config.useAutoRebuild) {
			clusterDetails.loadTrackedTables();
		}

		if (config.useAutoStart) {
			startupCluster();
		}

		// add any hosts found in the settings
		for (var clusterHost : builder.getClusterHosts()) {
			try {
				addDatabaseAndWait(clusterHost.createDBDatabase());
			} catch (Exception e) {
				LOG.error("FAILED TO ADD DATABASE: " + clusterHost.toString(), e);
			}
		}

		if (config.useAutoConnect) {
			connectSavedDatabases();
		}
	}

	/**
	 * Nope.
	 *
	 * @return ClusterDetails
	 */
	public final ClusterDetails getDetails() {
		if (details == null) {
			details = new ClusterDetails(getSettings().getLabel());
			details.setClusterSettings(getSettings());
		}
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

	public void waitUntilSynchronised() {
		getDetails().waitUntilSynchronised();
	}

	public void waitUntilDatabaseIsSynchronised(DBDatabase database) {
		getDetails().waitUntilDatabaseHasSynchronised(database);
	}

	public void waitUntilDatabaseIsSynchronised(DBDatabase database, long timeoutInMilliseconds) {
		getDetails().waitUntilDatabaseHasSynchronised(database, timeoutInMilliseconds);
	}

	public synchronized boolean requeryPermitted() {
		return requeryPermitted;
	}

	public synchronized void setRequeryPermitted(boolean requeryAllowed) {
		requeryPermitted = requeryAllowed;
	}

	public static enum Status {
		/**
		 * A READY database has fully implemented the database schema and has
		 * up-to-date data.
		 *
		 * Ready databases are used to execute queries on the cluster.
		 */
		READY,
		/**
		 * Unsynchronised databases have not yet had the schema implemented nor the
		 * data updated.
		 */
		UNSYNCHRONISED,
		/**
		 * Paused databases are ready databases that are being use to synchronize
		 * other databases.
		 */
		PAUSED,
		/**
		 * DEAD databases have been quarantined and then failed reconnection.
		 *
		 * DEAD databases are still included when reconnecting databases so they may
		 * re-appear as a ready database, but they are not counting toward
		 * synchronizing the whole cluster.
		 */
		DEAD,
		/**
		 * QUARANTINED databases have failed to complete an expected query or action
		 * and been isolated from the cluster.
		 *
		 * <p>
		 * QUARANTINED database will be reconnected during automatic or manual
		 * reconnect but only count toward synchronizing the cluster when
		 * auto-reconnection is active.
		 */
		QUARANTINED,
		/**
		 * UNKNOWN.
		 * 
		 */
		UNKNOWN,
		/**
		 * PROCESSING.
		 * 
		 * <p>Currently unused</p>
		 */
		PROCESSING,
		/**
		 * SYNCHRONIZING databases are being actively updated to match the cluster
		 * schema and data.
		 */
		SYNCHRONIZING
	}

	public DBDatabaseCluster() throws SQLException {
		this("", Configuration.autoRebuildReconnectAndStart());
	}

	public DBDatabaseCluster(String clusterLabel, Configuration config) throws SQLException {
		this(new DBDatabaseClusterSettingsBuilder().setLabel(clusterLabel).setConfiguration(config));
	}

	private void startupCluster() {
		if (startup.isNeeded()) {
			addReconnectionProcessor();
			addCleaner();
			startup.done();
		}
	}

	private void connectSavedDatabases() {
		List<DBDatabase> loadTheseDatabases = getDetails().getClusterHostsFromPrefs();
		for (var newDB : loadTheseDatabases) {
			try {
				addDatabase(newDB);
			} catch (SQLException ex) {
				Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	private void addReconnectionProcessor() {
		final ReconnectionProcess reconnectionProcessor = new ReconnectionProcess();
		reconnectionProcessor.setTimeOffset(ChronoUnit.MINUTES, 1);
		addRegularProcess(reconnectionProcessor);
	}

	public DBDatabase start() {
		startupCluster();
		return this;
	}

	public boolean isStarted() {
		return startup.hasHappened();
	}

	public DBDatabaseCluster(String clusterLabel) throws SQLException {
		this(clusterLabel, Configuration.autoRebuildReconnectAndStart());
	}

	public DBDatabaseCluster(String clusterLabel, Configuration config, DBDatabase... databases) throws SQLException {
		this(clusterLabel, config);
		initDatabase(databases);
	}

	public DBDatabaseCluster(String clusterLabel, Configuration config, DatabaseConnectionSettings... settings) throws SQLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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

	public DBDatabaseCluster(DatabaseConnectionSettings settings) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, SQLException {
		this(new DBDatabaseClusterSettingsBuilder().fromSettings(settings));
	}

	public DBDatabaseCluster(String clusterLabel, DatabaseConnectionSettings... settings) throws SQLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		this(clusterLabel);
		setDefinition(new ClusterDatabaseDefinition());
		for (DatabaseConnectionSettings setting : settings) {
			this.addDatabase(setting.createDBDatabase());
		}
	}

	private void initDatabase(DBDatabase[] databases) {
		for (DBDatabase database : databases) {
			addDatabaseWithoutWaiting(database);
		}
		setDefinition(new ClusterDatabaseDefinition());
		getDetails().synchronizeSecondaryDatabases();
	}

	/**
	 * Creates a new cluster with the configuration supplied.
	 *
	 * <p>
	 * Use this method to ensure that the new cluster will not clash with any
	 * existing clusters.</p>
	 *
	 * @param config the database configuration
	 * @param databases a database to build the cluster with
	 * @return a cluster with a random name based on the configuration and the
	 * database
	 * @throws SQLException database errors may occur while intialising the
	 * database and synchronising
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
	 * @param databases a database to build the cluster with
	 * @return a cluster with a random name based on the manual configuration and
	 * the database
	 * @throws SQLException database errors may be thrown during initialisation
	 */
	public static DBDatabaseCluster randomManualCluster(DBDatabase databases) throws SQLException {
		final String dbName = getRandomClusterName();
		return new DBDatabaseCluster(dbName, Configuration.fullyManual(), databases);
	}

	/**
	 * Creates a new cluster with auto-rebuild and auto-reconnect.
	 *
	 * <p>
	 * Use this method to ensure that the new cluster will not clash with any
	 * existing clusters.</p>
	 *
	 * @param databases a database to base the cluster on
	 * @return a cluster with a random name based the database, that will
	 * automatically start, rebuild the structure, and reconnect added databases
	 * @throws SQLException database errors may be thrown during initialisation
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
		getDetails().setClusterLabel(databaseName);
	}

	@Override
	public void setQuietExceptionsPreference(boolean bln) {
		super.setQuietExceptionsPreference(bln);
		getDetails().setQuietExceptionsPreference(bln);
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
	public final synchronized boolean addDatabaseAndWait(DBDatabase database) throws SQLException {
		return addDatabaseWithWaiting(database, true);
	}

	private synchronized boolean addDatabaseWithWaiting(DBDatabase database, boolean wait) throws SQLException {
		boolean add = addDatabaseWithoutWaiting(database);
		synchronizeAddedDatabases(wait);
		return add;
	}

	private boolean addDatabaseWithoutWaiting(DBDatabase database) {
		getSettings().addClusterHost(database.getSettings());
		boolean add = getDetails().add(database);
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
		return getDetails().getAllDatabases();
	}

	public Status getDatabaseStatus(DBDatabase db) {
		return getDetails().getStatusOf(db);
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
	 * is present (optional operation).If this list does not contain the element,
	 * it is unchanged.More formally, removes the element with the lowest index
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
	public synchronized boolean removeDatabases(List<DBDatabase> databases) throws UnableToRemoveLastDatabaseFromClusterException {
		return removeDatabases(databases.toArray(new DBDatabase[]{}));
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present (optional operation).If this list does not contain the element,
	 * it is unchanged.More formally, removes the element with the lowest index i
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
	public synchronized boolean removeDatabases(DBDatabase... databases) throws UnableToRemoveLastDatabaseFromClusterException {
		for (DBDatabase database : databases) {
			removeDatabase(database);
		}
		return true;
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present (optional operation).If this list does not contain the element,
	 * it is unchanged. More formally, removes the element with the lowest index i
	 * such that
	 * <code>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</code>
	 * (if such an element exists). Returns <code>true</code> if this list
	 * contained the specified element (or equivalently, if this list changed as a
	 * result of the call).
	 *
	 * @param database DBDatabase to be removed from this list, if present
	 * @return <code>true</code> if the database was removed
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
	public boolean removeDatabase(DBDatabase database) throws UnableToRemoveLastDatabaseFromClusterException {
		boolean removed = getSettings().removeClusterHost(database.getSettings());
		if (removed) {
			return getDetails().removeDatabase(database);
		} else {
			return removed;
		}
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
		getDetails().quarantineDatabase(database, except);
	}

	private void deadDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		getDetails().deadDatabase(database, except);
	}

	/**
	 * Returns a single random database that is ready for queries
	 *
	 * @return a ready database
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException the
	 * cluster is current unable to service requests
	 */
	public DBDatabase getReadyDatabase() throws NoAvailableDatabaseException {
		final DBDatabase ready = getDetails().getReadyDatabase();
		return ready;
	}

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent, StatementDetails details) throws Exception {
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
		DBDatabase[] dbs = getDetails().getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.preventDroppingOfDatabases(justLeaveThisAtTrue);
		}
	}

	@Override
	public synchronized void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		super.preventDroppingOfTables(droppingTablesIsAMistake);
		DBDatabase[] dbs = getDetails().getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.preventDroppingOfTables(droppingTablesIsAMistake);
		}
	}

	@Override
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		super.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		DBDatabase[] dbs = getDetails().getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		}
	}

	@Override
	public synchronized boolean batchSQLStatementsWhenPossible() {
		super.batchSQLStatementsWhenPossible();
		boolean result = true;
		DBDatabase[] dbs = getDetails().getReadyDatabases();
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
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
	 * @throws SQLException database errors may occur
	 * @throws AccidentalDroppingOfTableException Always ensure that this not done
	 * accidentally
	 * @throws AutoCommitActionDuringTransactionException dropping a table within
	 * a transaction is not permitted
	 */
	@Override
	public <TR extends DBRow> void dropTableIfExists(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, SQLException {
		LOG.debug("DROPPING TABLE IFEXISTS: " + tableRow.getTableName());
		removeTrackedTable(tableRow);
		if (getPreventAccidentalDroppingOfTables()) {
			throw new AccidentalDroppingOfTableException();
		}
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.dropTableIfExists(tableRow);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, UnableToRemoveLastDatabaseFromClusterException {
		LOG.debug("DROPPING TABLE NOEXEC: " + tableRow.getTableName());
		removeTrackedTable(tableRow);
		if (getPreventAccidentalDroppingOfTables()) {
			throw new AccidentalDroppingOfTableException();
		}
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
		LOG.debug("DROPPING TABLE: " + tableRow.getTableName());
		removeTrackedTable(tableRow);
		if (getPreventAccidentalDroppingOfTables()) {
			throw new AccidentalDroppingOfTableException();
		}
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
		addTrackedTable(newTableRow);
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
	public synchronized void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		LOG.debug("CREATING TABLE: " + newTableRow.getTableName());
		addTrackedTable(newTableRow);
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createTable(newTableRow, includeForeignKeyClauses);
						finished = true;
					} catch (Exception e) {
						if (getQuietExceptionsPreference()) {
						} else {
							System.out.println("nz.co.gregs.dbvolution.databases.DBDatabaseCluster.createTable(DBRow, boolean): " + e.getLocalizedMessage());
							e.printStackTrace();
						}
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
		addTrackedTables(newTables);
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
		addTrackedTables(Arrays.asList(newTables));
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
		addTrackedTables(Arrays.asList(newTables));
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
		addTrackedTable(newTable);
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
		addTrackedTable(newTable);
		boolean finished = false;
		do {
			DBDatabase[] dbs = getDetails().getReadyDatabases();
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
			DBDatabase[] dbs = getDetails().getReadyDatabases();
			if (dbs.length == 0) {
				finished = true;
			} else {
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
			final DBDatabase[] readyDatabases = getDetails().getReadyDatabases();
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

	@Override
	public DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		throw new UnsupportedOperationException("DBDatabase.getConnection should not be used.");
	}

	@Override
	protected DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		throw new UnsupportedOperationException("DBDatabase.getLowLevelStatement should not be used.");
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public synchronized DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		return executeDBActionOnClusterMembers(action);
	}

	private DBActionList executeDBActionOnClusterMembers(DBAction action) throws NoAvailableDatabaseException, DBRuntimeException, SQLException {
		LOG.debug("EXECUTING ACTION: " + action.getSQLStatements(this));
		addActionToQueue(action);
		List<ActionTask> tasks = new ArrayList<ActionTask>();
		DBActionList actionsPerformed = new DBActionList();
		try {
			DBDatabase firstDatabase = getReadyDatabase();
			boolean finished = false;
			do {
				try {
					if (action.requiresRunOnIndividualDatabaseBeforeCluster()) {
						// Because of autoincrement PKs we need to execute on one database first
						actionsPerformed = new ActionTask(this, firstDatabase, action).call();
						removeActionFromQueue(firstDatabase, action);
						finished = true;
					} else {
						finished = true;
					}
				} catch (SQLException e) {
					if (handleExceptionDuringAction(e, firstDatabase).equals(HandlerAdvice.ABORT)) {
						throw e;
					}
				}
			} while (!finished && size() > 1);
			final DBDatabase[] databases = getDetails().getReadyDatabases();
			// Now execute on all the other databases
			for (DBDatabase next : databases) {
				if (action.runOnDatabaseDuringCluster(firstDatabase, next)) {
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
	public DBActionList executeDBAction(DBInsert action) throws SQLException, NoAvailableDatabaseException {
		return executeDBActionOnClusterMembers(action);
	}

	@Override
	public DBActionList executeDBAction(DBUpdate action) throws SQLException, NoAvailableDatabaseException {
		return executeDBActionOnClusterMembers(action);
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, UnableToRemoveLastDatabaseFromClusterException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		final DBDatabase workingDB = getReadyDatabase();
		workingDB.setQuietExceptionsPreference(this.getQuietExceptionsPreference());
		HandlerAdvice advice = HandlerAdvice.REQUERY;
		try {
			// set oracle compatibility 
			query.setReturnEmptyStringForNullString(query.getReturnEmptyStringForNullString() || !getDefinition().canProduceNullStrings());
			// hand the job down to the next layer
			return workingDB.executeDBQuery(query);
		} catch (AccidentalBlankQueryException | AccidentalCartesianJoinException | NoAvailableDatabaseException errorWithTheQueryException) {
			throw errorWithTheQueryException;
		} catch (SQLException e) {
			advice = handleExceptionDuringQuery(e, workingDB);
			if (advice.equals(HandlerAdvice.REQUERY) && requeryPermitted()) {
				return executeDBQuery(query);
			} else {
				getDetails().quarantineDatabaseAutomatically(workingDB, e);
				throw e;
			}
		}
	}

	@Override
	public void handleErrorDuringExecutingSQL(DBDatabase suspectDatabase, Throwable sqlException, String sqlString) {
		getDetails().quarantineDatabaseAutomatically(suspectDatabase, sqlException);
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
				getDetails().quarantineDatabaseAutomatically(readyDatabase, e);
				return HandlerAdvice.REQUERY;
			}
		}
	}

	private HandlerAdvice handleExceptionDuringAction(Exception e, final DBDatabase readyDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		if (size() < 2) {
			return HandlerAdvice.ABORT;
		} else {
			getDetails().quarantineDatabaseAutomatically(readyDatabase, e);
			return HandlerAdvice.REQUERY;
		}
	}

	@Override
	public String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException {
		final DBDatabase readyDatabase = this.getReadyDatabase();
		synchronized (readyDatabase) {
			readyDatabase.setQuietExceptionsPreference(getQuietExceptionsPreference());
			return readyDatabase.getSQLForDBQuery(query);
		}
	}

	ArrayList<DBStatement> getDBStatements() throws SQLException {
		ArrayList<DBStatement> arrayList = new ArrayList<>();
		final DBDatabase[] readyDatabases = getDetails().getReadyDatabases();
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
		for (DBDatabase db : getDetails().getAllDatabases()) {
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
		return getDetails().getSupportsDifferenceBetweenNullAndEmptyString();
	}

	private void addActionToQueue(DBAction action) {
		for (DBDatabase db : getDetails().getAllDatabases()) {
			Queue<DBAction> queue = getDetails().getActionQueue(db);
			queue.add(action);
		}
	}

	private void removeActionFromQueue(DBDatabase database, DBAction action) {
		final Queue<DBAction> queue = getDetails().getActionQueue(database);
		synchronized (queue) {
			if (queue != null) {
				queue.remove(action);
			}
		}
	}

	private synchronized void synchronizeAddedDatabases(boolean blocking) throws SQLException {
		boolean block = blocking || (getDetails().getReadyDatabases().length < 2);
		final DBDatabase[] dbs = getDetails().getUnsynchronizedDatabases();
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
		for (DBDatabase readyDatabase : getDetails().getReadyDatabases()) {
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
		return getDetails().getReadyDatabases().length;
	}

	public String getClusterStatus() {
		final String summary = getStatusOfActiveDatabases();
		final String unsyn = getStatusOfUnsynchronisedDatabases();
		final String quarantined = getStatusOfQuarantinedDatabases();
		return summary + "\n" + unsyn + "\n" + quarantined;
	}

	private String getStatusOfQuarantinedDatabases() {
		return (new Date()).toString() + "Quarantined Databases: " + getDetails().getQuarantinedDatabases().length + " of " + getDetails().getAllDatabases().length;
	}

	private String getStatusOfUnsynchronisedDatabases() {
		return (new Date()).toString() + "Unsynchronised: " + getDetails().getUnsynchronizedDatabases().length + " of " + getDetails().getAllDatabases().length;
	}

	private String getStatusOfActiveDatabases() {
		final DBDatabase[] ready = getDetails().getReadyDatabases();
		return (new Date()).toString() + "Active Databases: " + ready.length + " of " + getDetails().getAllDatabases().length;
	}

	public String getDatabaseStatuses() {
		StringBuilder result = new StringBuilder();
		final DBDatabase[] all = getDetails().getAllDatabases();
		for (DBDatabase db : all) {
			result.append(this.getDatabaseStatus(db).name())
					.append(": ")
					.append(db.getSettings().toString().replaceAll("DATABASECONNECTIONSETTINGS: ", ""))
					.append("\n");
		}
		return result.toString();
	}

	public final boolean getAutoRebuild() {
		return getDetails().getAutoRebuild();
	}

	/**
	 * Returns true if the cluster is set to automatically reconnect with
	 * quarantined databases
	 *
	 * @return the autoreconnect setting
	 */
	public final boolean getAutoReconnect() {
		return getDetails().getAutoReconnect();
	}

	@Override
	public synchronized void stop() {
		stopCluster();
	}

	/**
	 * Stops this cluster and it's contained databases.
	 *
	 * See {@link #stopCluster() } and {@link DBDatabaseInterface#stop() }.
	 */
	public void stopClusterAndDatabases() {
		stopClusterInternal(true);
	}

	/**
	 * Stops the cluster without effecting the contained databases.
	 *
	 * <p>
	 * To stop all databases in the cluster as well as the cluster use
	 * {@link #stop}</p>
	 */
	public void stopCluster() {
		stopClusterInternal(false);
	}

	private synchronized void stopClusterInternal(boolean andDatabases) {
		try {
			shutdownClusterProcesses();
			if (andDatabases) {
				LOG.debug("STOPPING: contained databases");
				for (DBDatabase db : getDetails().getAllDatabases()) {
					db.stop();
				}
				LOG.debug("STOPPING: removing all databases");
			}
			getDetails().removeAllDatabases();
			super.stop();
		} catch (SQLException ex) {
			LOG.error(this, ex);
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

	private ClusterCleanupActions clusterCleanupActions;
	private transient Cleaner.Cleanable cleanable;

	private void addCleaner() {
		clusterCleanupActions = new ClusterCleanupActions(getDetails(), LOG, ACTION_THREAD_POOL);
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
			getDetails().dismantle();
		} catch (SQLException ex) {
			Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private synchronized void shutdownClusterProcesses() {
		LOG.debug("STOPPING: action thread pool");
		ACTION_THREAD_POOL.shutdown();
	}

	@Override
	public boolean isMemoryDatabase() {
		return !details.getAutoRebuild() || !details.hasAuthoritativeDatabase();
	}

	public synchronized void setRequiredToProduceEmptyStringsForNull(boolean required) {
		getDetails().setSupportsDifferenceBetweenNullAndEmptyString(!required);
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

		final public Void synchronise(DBDatabaseCluster cluster, DBDatabase database) {
			cluster.getDetails().synchronizeSecondaryDatabase(database);
			return null;
		}
	}

	public String reconnectQuarantinedDatabases() throws UnableToRemoveLastDatabaseFromClusterException, SQLException {
		StringBuilder str = new StringBuilder();
		DBDatabase[] reconnectables = details.getDatabasesForReconnecting();
		if (reconnectables.length == 0) {
			LOG.trace(this.getLabel() + " HAS NO QUARANTINED/DEAD DATABASES");
		} else {
			for (DBDatabase reconnectee : reconnectables) {
				reconnectQuarantinedDatabase(str, reconnectee);
			}
		}

		return str.toString();
	}

	private void reconnectQuarantinedDatabase(StringBuilder str, DBDatabase quarantee) throws UnableToRemoveLastDatabaseFromClusterException {
		str.append(quarantee.getSettings());
		try {
			LOG.info(this.getLabel() + " RECONNECTING DATABASE: " + quarantee.getLabel());
			addDatabase(quarantee);
			LOG.info(this.getLabel() + " RECONNECTED DATABASE: " + quarantee.getLabel());
			str.append("").append(quarantee.getLabel()).append(" added");
		} catch (SQLException ex) {
			LOG.info(this.getLabel() + " RECONNECTION FAILED FOR DATABASE: " + quarantee.getLabel());
			LOG.info(this.getLabel() + " DEAD DATABASE: " + quarantee.getLabel());
			deadDatabase(quarantee, ex);
			str.append("").append(quarantee.getLabel()).append(" quarantined: ").append(ex.getLocalizedMessage());
		} finally {
			str.append("\n");
		}
	}

	public DBRow[] getTrackedTables() {
		return getDetails().getRequiredAndTrackedTables();
	}

	public void setTrackedTables(Collection<DBRow> rows) {
		getDetails().setTrackedTables(rows);
	}

	public void addTrackedTable(DBRow row) {
		getDetails().addTrackedTable(row);
	}

	public void addTrackedTables(Collection<DBRow> rows) {
		getDetails().addTrackedTables(rows);
	}

	public void addTrackedTables(DBRow... rows) {
		getDetails().addTrackedTables(Arrays.asList(rows));
	}

	public void removeTrackedTable(DBRow row) {
		getDetails().removeTrackedTable(row);
	}

	public void removeTrackedTables(Collection<DBRow> rows) {
		getDetails().removeTrackedTables(rows);
	}

	public void removeTrackedTables(DBRow... rows) {
		getDetails().removeTrackedTables(Arrays.asList(rows));

	}

	public static class Configuration implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * Auto-rebuild will automatically reload the tracked table, connect to the
		 * authoritative database of the previous instance of this cluster, and
		 * reload the data for the tracked and required tables.
		 *
		 * This provides continuity of schema and data between instances of the
		 * cluster and removes the need to fully specify the schema within a
		 * DataRepo or configuration file.
		 */
		private final boolean useAutoRebuild;
		/**
		 * Auto-reconnect instructs the cluster to reconnect to any cluster members
		 * disconnected during processing. This includes databases that could not be
		 * connected to, and those that were quarantined due to errors.
		 *
		 * Reconnected databases will be synchronized before use.
		 */
		private final boolean useAutoReconnect;
		/**
		 * Auto-start instructs the cluster to immediately perform tasks required to
		 * make the cluster usable as a database.
		 *
		 * These tasks may include reloading the tracked tables and data of the
		 * previous instance, connecting to former members, synchronizing cluster
		 * members, starting the reconnection process, and more.
		 */
		private final boolean useAutoStart;
		/**
		 * Auto-connect loads the list of cluster members from the previous
		 * instance.
		 *
		 * This provides continuity of membership and removes the need fully specify
		 * the members in a code or configurations files.
		 */
		private final boolean useAutoConnect;

		public Configuration() {
			this(false, false, false, false);
		}

		public Configuration(boolean useAutoRebuild, boolean useAutoReconnect, boolean useAutoStart, boolean useAutoConnect) {
			this.useAutoRebuild = useAutoRebuild;
			this.useAutoReconnect = useAutoReconnect;
			this.useAutoStart = useAutoStart;
			this.useAutoConnect = useAutoConnect;
		}

		/**
		 * Use for a database that does not automatically rebuild the data when
		 * restarting the cluster nor reconnect quarantined databases after an error
		 * but does automatically start synchronising databases.
		 *
		 * @return a autostart configuration
		 */
		public static Configuration autoStart() {
			return new Configuration(false, false, true, false);
		}

		/**
		 * Use for a database that does not automatically rebuild the data when
		 * restarting the cluster nor reconnect quarantined databases after an
		 * error.
		 *
		 * @return a manual configuration
		 * @deprecated This version of manual will automatically start the cluster,
		 * use {@link #autoStart() } instead
		 */
		@Deprecated()
		public static Configuration manual() {
			return new Configuration(false, false, true, false);
		}

		/**
		 * Use for a database that does not automatically rebuild the data when
		 * restarting the cluster nor reconnect quarantined databases after an
		 * error.
		 *
		 * <p>
		 * The database will not be started nor will databases in the previous
		 * cluster instance be re-added.</p>
		 *
		 * @return a manual configuration
		 */
		public static Configuration fullyManual() {
			return new Configuration(false, false, false, false);
		}

		/**
		 * A configuration that will try to restore the data from the previous
		 * instance of this cluster.
		 *
		 * <p>
		 * the TrackedTable list will also be rebuilt.</p>
		 *
		 * <p>
		 * Auto-rebuild will automatically reload the tracked table, connect to the
		 * authoritative database of the previous instance of this cluster, and
		 * reload the data for the tracked and required tables.
		 *
		 * This provides continuity of schema and data between instances of the
		 * cluster and removes the need to fully specify the schema within a
		 * DataRepo or configuration file.</p>
		 *
		 * Equivalent to new Configuration(true, false, true, false)
		 *
		 * @return an auto-rebuild configuration
		 */
		public static Configuration autoRebuild() {
			return new Configuration(true, false, true, false);
		}

		/**
		 * A configuration that will try to connect quarantined databases will the
		 * cluster is running.
		 *
		 * <p>
		 * the TrackedTable list will also be rebuilt.</p>
		 *
		 * <p>
		 * Auto-reconnect instructs the cluster to reconnect to any cluster members
		 * disconnected during processing. This includes databases that could not be
		 * connected to, and those that were quarantined due to errors.</p>
		 *
		 * <p>
		 * Reconnected databases will be synchronized before use.</p>
		 *
		 * Equivalent to new Configuration(false, true, true, false)
		 *
		 * @return an auto-reconnect configuration
		 */
		public static Configuration autoReconnect() {
			return new Configuration(false, true, true, false);
		}

		/**
		 * A configuration that will try to restore the data from the previous
		 * instance of this cluster AND try to connect quarantined databases will
		 * the cluster is running.
		 *
		 * Equivalent to new Configuration(true, true, true, false)
		 *
		 * @return an auto-rebuild and reconnect configuration
		 * @deprecated despite the method name, this will also start the cluster.
		 * Use {@link #autoRebuildReconnectAndStart() } instead
		 */
		@Deprecated
		public static Configuration autoRebuildAndReconnect() {
			return new Configuration(true, true, true, false);
		}

		/**
		 * A configuration that will try to restore the data from the previous
		 * instance of this cluster AND try to connect quarantined databases will
		 * the cluster is running.
		 *
		 * Equivalent to new Configuration(true, true, true, false)
		 *
		 * @return an auto-rebuild and reconnect configuration
		 */
		public static Configuration autoRebuildReconnectAndStart() {
			return new Configuration(true, true, true, false);
		}

		/**
		 * Auto-rebuild will automatically reload the tracked tables, connect to the
		 * authoritative database of the previous instance of this cluster, and
		 * reload the data for the tracked and required tables.
		 *
		 * This provides continuity of schema and data between instances of the
		 * cluster and removes the need to fully specify the schema within a
		 * DataRepo or configuration file.
		 *
		 * @return TRUE if the cluster will try to reload data from the previous
		 * version of the cluster.
		 */
		public boolean isUseAutoRebuild() {
			return useAutoRebuild;
		}

		/**
		 * Auto-reconnect instructs the cluster to reconnect to any cluster members
		 * disconnected during processing. This includes databases that could not be
		 * connected to, and those that were quarantined due to errors.
		 *
		 * Reconnected databases will be synchronized before use.
		 *
		 * @return TRUE if the cluster will try to automatically reconnect and
		 * synchronize database while running.
		 */
		public boolean isUseAutoReconnect() {
			return useAutoReconnect;
		}

		/**
		 * Auto-start instructs the cluster to immediately perform tasks required to
		 * make the cluster usable as a database.
		 *
		 * These tasks may include reloading the tracked tables and data of the
		 * previous instance, connecting to former members, synchronizing cluster
		 * members, starting the reconnection process, and more.
		 *
		 * @return the useAutoStart
		 */
		public boolean isUseAutoStart() {
			return useAutoStart;
		}

		/**
		 * Auto-connect loads the list of cluster members from the previous
		 * instance.
		 *
		 * This provides continuity of membership and removes the need fully specify
		 * the members in code or configurations files.
		 *
		 * @return the useAutoConnect
		 */
		public boolean isUseAutoConnect() {
			return useAutoConnect;
		}

		/**
		 * Auto-rebuild will automatically reload the tracked tables, connect to the
		 * authoritative database of the previous instance of this cluster, and
		 * reload the data for the tracked and required tables.
		 *
		 * This provides continuity of schema and data between instances of the
		 * cluster and removes the need to fully specify the schema within a
		 * DataRepo or configuration file.
		 *
		 * @return TRUE if the cluster will try to reload data from the previous
		 * version of the cluster.
		 */
		public Configuration withAutoRebuild() {
			return new Configuration(true, this.useAutoReconnect, this.useAutoStart, this.useAutoConnect);
		}

		/**
		 * Auto-reconnect instructs the cluster to reconnect to any cluster members
		 * disconnected during processing. This includes databases that could not be
		 * connected to, and those that were quarantined due to errors.
		 *
		 * Reconnected databases will be synchronized before use.
		 *
		 * @return TRUE if the cluster will try to automatically reconnect and
		 * synchronize database while running.
		 */
		public Configuration withAutoReconnect() {
			return new Configuration(this.useAutoRebuild, true, this.useAutoStart, this.useAutoConnect);
		}

		/**
		 * Auto-start instructs the cluster to immediately perform tasks required to
		 * make the cluster usable as a database.
		 *
		 * These tasks may include reloading the tracked tables and data of the
		 * previous instance, connecting to former members, synchronizing cluster
		 * members, starting the reconnection process, and more.
		 *
		 * @return the useAutoStart
		 */
		public Configuration withAutoStart() {
			return new Configuration(this.useAutoRebuild, this.useAutoReconnect, true, this.useAutoConnect);
		}

		/**
		 * Auto-connect loads the list of cluster members from the previous
		 * instance.
		 *
		 * This provides continuity of membership and removes the need fully specify
		 * the members in code or configurations files.
		 *
		 * @return the useAutoConnect
		 */
		public Configuration withAutoConnect() {
			return new Configuration(this.useAutoRebuild, this.useAutoReconnect, this.useAutoStart, true);
		}
	}
}
