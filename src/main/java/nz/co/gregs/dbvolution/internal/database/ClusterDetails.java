/*
 * Copyright 2018 gregorygraham.
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
package nz.co.gregs.dbvolution.internal.database;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.reflection.DataModel;
import nz.co.gregs.dbvolution.utility.StringCheck;
import nz.co.gregs.dbvolution.utility.PreferencesImproved;
import nz.co.gregs.dbvolution.utility.encryption.Encryption_Internal;
import nz.co.gregs.separatedstring.SeparatedString;
import nz.co.gregs.separatedstring.SeparatedStringBuilder;

/**
 *
 * @author gregorygraham
 */
public class ClusterDetails implements Serializable {

	private final static long serialVersionUID = 1l;

	private static final Logger LOG = Logger.getLogger(ClusterDetails.class.getName());

	private final DatabaseList members = new DatabaseList();

	private transient final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
	private transient final Set<DBRow> trackedTables = Collections.synchronizedSet(new HashSet<DBRow>());
	private transient final Map<DBDatabase, Queue<DBAction>> queuedActions = Collections.synchronizedMap(new HashMap<DBDatabase, Queue<DBAction>>(0));

	private transient final PreferencesImproved prefs = PreferencesImproved.userNodeForPackage(this.getClass());
	private String clusterLabel = "NotDefined";
	private boolean supportsDifferenceBetweenNullAndEmptyString = true;
	private final ArrayList<String> allAddedDatabases = new ArrayList<String>();
	private boolean quietExceptions = false;
	private DBDatabaseCluster.Configuration configuration = DBDatabaseCluster.Configuration.fullyManual();

	private transient final Lock synchronisingLock = new ReentrantLock();
	private transient final Condition aDatabaseHasBeenSynchronised = synchronisingLock.newCondition();
	private transient final Condition allDatabasesAreSynchronised = synchronisingLock.newCondition();
	private transient final Condition someDatabasesNeedSynchronizing = synchronisingLock.newCondition();
	private transient final Condition readyDatabaseIsAvailable = synchronisingLock.newCondition();
	private DatabaseConnectionSettings clusterSettings;
	private DBDatabase preferredDatabase;

	private final static Random RANDOM = new Random();
	private boolean preferredDatabaseRequired;

	public ClusterDetails(String label) {
		this.clusterLabel = label;
	}

	public final synchronized boolean add(DBDatabase databaseToAdd) {
		if (databaseToAdd != null) {
			DBDatabase database = databaseToAdd;
			final boolean clusterSupportsDifferenceBetweenNullAndEmptyString = getSupportsDifferenceBetweenNullAndEmptyString();
			boolean databaseSupportsDifferenceBetweenNullAndEmptyString = database.supportsDifferenceBetweenNullAndEmptyString();
			if (clusterSupportsDifferenceBetweenNullAndEmptyString) {
				if (databaseSupportsDifferenceBetweenNullAndEmptyString) {
					// both support the diference so there is no conflict
				} else {
					// the cluster needs to change to handle Oracle-like behaviour
					setSupportsDifferenceBetweenNullAndEmptyString(false);
				}
			} else {
				if (databaseSupportsDifferenceBetweenNullAndEmptyString) {
					// currently the cluster and query should avoid any need to change the database behaviour
				}
			}

			if (clusterContains(database)) {
				members.setUnsynchronised(database);
			} else {
				addDatabaseAsUnsynchronized(database);
				saveClusterSettingsToPrefs();
				return true;
			}
		}
		return false;
	}

	private synchronized boolean addDatabaseAsUnsynchronized(DBDatabase database) {
		members.add(database);
		signalSomeDatabasesNeedSynchronising();
		return true;
	}

	private void signalSomeDatabasesNeedSynchronising() {
		synchronisingLock.lock();
		try {
			someDatabasesNeedSynchronizing.signalAll();
		} finally {
			synchronisingLock.unlock();
		}
	}

	public DBDatabase[] getAllDatabases() {
		synchronisingLock.lock();
		try {
			return members.getDatabases();
		} finally {
			synchronisingLock.unlock();
		}
	}

	public synchronized void quarantineDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		if (clusterContains(database)) {
			if (hasTooFewReadyDatabases() && members.isReady(database)) {
				// Unable to quarantine the only remaining database
				throw new UnableToRemoveLastDatabaseFromClusterException();
			}

			if (quietExceptions) {
			} else {
				LOG.log(Level.WARNING, "QUARANTINING: {0}", database.getLabel());
				LOG.log(Level.WARNING, "QUARANTINING: {0}", database.getSettings().toString());
				LOG.log(Level.WARNING, "QUARANTINING: {0}", except.getLocalizedMessage());
			}
			database.setLastException(except);
			members.setQuarantined(database);
			queuedActions.remove(database);
			setAuthoritativeDatabase();
		}
	}

	public synchronized void deadDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		if (clusterContains(database)) {
			if (hasTooFewReadyDatabases() && members.isReady(database)) {
				// Unable to quarantine the only remaining database
				throw new UnableToRemoveLastDatabaseFromClusterException();
			}

			if (quietExceptions) {
			} else {
				LOG.log(Level.WARNING, "DEAD: {0}", database.getLabel());
				LOG.log(Level.WARNING, "DEAD: {0}", database.getSettings().toString());
				LOG.log(Level.WARNING, "DEAD: {0}", except.getLocalizedMessage());
			}
			database.setLastException(except);
			members.setDead(database);
			queuedActions.remove(database);
			setAuthoritativeDatabase();
		}
	}

	public synchronized boolean removeDatabase(DBDatabase databaseToRemove) {
		DBDatabase database = databaseToRemove;
		if (hasTooFewReadyDatabases() && members.isReady(database)) {
			throw new UnableToRemoveLastDatabaseFromClusterException();
		} else {
			members.remove(database);
			setAuthoritativeDatabase();
			saveClusterSettingsToPrefs();
			checkSupportForDifferenceBetweenNullAndEmptyString();
			return true;
		}
	}

	protected boolean hasTooFewReadyDatabases() {
		return members.countReadyDatabases() < 2;
	}

	public synchronized DBDatabase[] getUnsynchronizedDatabases() {
		return members.getDatabases(DBDatabaseCluster.Status.UNSYNCHRONISED);
	}

	public Queue<DBAction> getActionQueue(DBDatabase db) {
		synchronized (queuedActions) {
			Queue<DBAction> queue = queuedActions.get(db);
			if (queue == null) {
				queue = new LinkedBlockingQueue<DBAction>();
				queuedActions.put(db, queue);
			}
			return queue;
		}
	}

	public synchronized DBRow[] getRequiredAndTrackedTables() {
		var tables = new ArrayList<DBRow>();
		tables.addAll(requiredTables);
		tables.addAll(trackedTables);
		return tables.toArray(new DBRow[]{});
	}

	public void setTrackedTables(Collection<DBRow> rows) {
		synchronized (trackedTables) {
			trackedTables.clear();
			trackedTables.addAll(rows);
		}
		saveTrackedTables();
	}

	public void addTrackedTable(DBRow row) {
		synchronized (trackedTables) {
			trackedTables.add(row);
		}
		saveTrackedTables();
	}

	public void addTrackedTables(Collection<DBRow> rows) {
		synchronized (trackedTables) {
			trackedTables.addAll(rows);
		}
		saveTrackedTables();
	}

	public void removeTrackedTable(DBRow row) {
		synchronized (trackedTables) {
			trackedTables.remove(row);
		}
		saveTrackedTables();
	}

	public void removeTrackedTables(Collection<DBRow> rows) {
		synchronized (trackedTables) {
			trackedTables.removeAll(rows);
		}
		saveTrackedTables();
	}

	public synchronized void readyDatabase(DBDatabase databaseToReady) throws SQLException {
		DBDatabase secondary = databaseToReady;
		try {
			if (hasReadyDatabases()) {
				DBDatabase readyDatabase = getRandomReadyDatabase();
				if (readyDatabase != null) {
					secondary.setPrintSQLBeforeExecuting(readyDatabase.getPrintSQLBeforeExecuting());
					secondary.setBatchSQLStatementsWhenPossible(readyDatabase.getBatchSQLStatementsWhenPossible());
				}
			}
		} catch (NoAvailableDatabaseException ex) {

		}
		members.setReady(secondary);
		setAuthoritativeDatabase();
		signalThatADatabaseHasBeenSynchronised();
		signalReadyDatabaseIsAvailable();
	}

	private void signalReadyDatabaseIsAvailable() {
		synchronisingLock.lock();
		try {
			readyDatabaseIsAvailable.signalAll();
		} finally {
			synchronisingLock.unlock();
		}
	}

	protected boolean hasReadyDatabases() {
		return members.countReadyDatabases() > 0;
	}

	public synchronized DBDatabase[] getReadyDatabases() {
		return members.getDatabases(DBDatabaseCluster.Status.READY);
	}

	public synchronized DBDatabase getPausedDatabase() throws NoAvailableDatabaseException {
		DBDatabase template = getRandomReadyDatabase();
		members.setPaused(template);
		return template;
	}

	public synchronized DBDatabase getPausedDatabase(DBDatabase db) throws NoAvailableDatabaseException {
		members.setPaused(db);
		return db;
	}

	public DBDatabase getReadyDatabase() throws NoAvailableDatabaseException {
		if (hasPreferredDatabase() && preferredDatabaseIsReady()) {
			return preferredDatabase;
		} else if (hasPreferredDatabase() && preferredDatabaseRequired) {
			waitUntilDatabaseHasSynchronised(preferredDatabase);
			return preferredDatabase;
		} else {
			return getRandomReadyDatabase();
		}
	}

	private DBDatabase getRandomReadyDatabase() throws NoAvailableDatabaseException {
		DBDatabase[] dbs = getReadyDatabases();
		int tries = 0;
		while (dbs.length < 1 && members.countPausedDatabases() > 0 && tries <= 10) {
			awaitReadyDatabase();
			dbs = getReadyDatabases();
			tries++;
		}
		if (dbs.length > 0) {
			final int randNumber = RANDOM.nextInt(dbs.length);
			DBDatabase randomElement = dbs[randNumber];
			return randomElement;
		}
		throw new NoAvailableDatabaseException();
	}

	private void awaitReadyDatabase() {
		synchronisingLock.lock();
		try {
			readyDatabaseIsAvailable.await(100, TimeUnit.MILLISECONDS);
		} catch (InterruptedException ex) {
			Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			synchronisingLock.unlock();
		}
	}

	public synchronized void addAll(DBDatabase[] databases) throws SQLException {
		for (DBDatabase database : databases) {
			add(database);
		}
	}

	public synchronized void addAll(Collection<DBDatabase> databases) throws SQLException {
		for (DBDatabase database : databases) {
			add(database);
		}
	}

	public synchronized DBDatabase getTemplateDatabase() throws NoAvailableDatabaseException {
		if (members.size() == 1 && configuration.isUseAutoRebuild()) {
			return getAuthoritativeDatabase();
		} else {
			if (members.countReadyDatabases() == 0 && members.countPausedDatabases() == 0) {
				throw new NoAvailableDatabaseException();
			}
			return getPausedDatabase();
		}
	}

	private synchronized DBDatabase getAuthoritativeDatabase() throws NoAvailableDatabaseException {
		final DatabaseConnectionSettings authoritativeDCS = getAuthoritativeDatabaseConnectionSettings();
		if (authoritativeDCS != null) {
			try {
				return authoritativeDCS.createDBDatabase();
			} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				LOG.log(Level.SEVERE, null, ex);
				throw new NoAvailableDatabaseException();
			}
		} else {
			throw new NoAvailableDatabaseException();
		}
	}

	private synchronized void removedTrackedTablesFromPrefs() {
		prefs.remove(getTrackedTablesPrefsIdentifier());
	}

	private synchronized void saveTrackedTables() {
		if (configuration.isUseAutoRebuild()) {
			final String name = getTrackedTablesPrefsIdentifier();
			SeparatedString rowClasses = getTrackedTablesSeparatedStringTemplate();
			for (DBRow trackedTable : trackedTables) {
				rowClasses.add(trackedTable.getClass().getName());
			}
			String encodedTablenames = rowClasses.encode();
			try {
				final String encryptedText = Encryption_Internal.encrypt(encodedTablenames);
				prefs.put(name, encryptedText);
			} catch (CannotEncryptInputException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	public synchronized List<String> getSavedTrackedTables() {

		String encodedSettings = "";
		final String rawPrefsValue = prefs.get(getTrackedTablesPrefsIdentifier(), null);
		if (StringCheck.isNotEmptyNorNull(rawPrefsValue)) {
			try {
				encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
			} catch (UnableToDecryptInput ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
		var seps = getTrackedTablesSeparatedStringTemplate();
		List<String> decodedRowClasses = seps.decode(encodedSettings);
		return decodedRowClasses;
	}

	public synchronized void loadTrackedTables() {
		if (configuration.isUseAutoRebuild()) {
			List<String> savedTrackedTables = getSavedTrackedTables();
			for (String savedTrackedTable : savedTrackedTables) {
				try {
					@SuppressWarnings("unchecked")
					Class<DBRow> trackedTableClass = (Class<DBRow>) Class.forName(savedTrackedTable);
					DBRow dbRow = DBRow.getDBRow(trackedTableClass);
					trackedTables.add(dbRow);
				} catch (ClassNotFoundException ex) {
					ex.printStackTrace();
					LOG.log(
							Level.SEVERE,
							"Tracked Table {0} requested but not found while trying to rebuild cluster {1}",
							new Object[]{savedTrackedTable, getClusterLabel()}
					);
				}
			}
		}
	}

	private String getTrackedTablesPrefsIdentifier() {
		return getClusterLabel() + "_trackedtables";
	}

	private SeparatedString getTrackedTablesSeparatedStringTemplate() {
		return SeparatedStringBuilder.commaSeparated();
	}

	private synchronized void removeAuthoritativeDatabaseFromPrefs() {
		prefs.remove(getClusterLabel());
	}

	private synchronized void setAuthoritativeDatabase() {
		if (configuration.isUseAutoRebuild()) {
			for (DBDatabase db : members.getDatabases(DBDatabaseCluster.Status.READY)) {
				final String name = getClusterLabel();
				if (!db.isMemoryDatabase() && StringCheck.isNotEmptyNorNull(name)) {
					final String encode = db.getSettings().encode();
					try {
						prefs.put(name, Encryption_Internal.encrypt(encode));
					} catch (CannotEncryptInputException ex) {
						LOG.log(Level.SEVERE, null, ex);
						prefs.put(name, encode);
					}
					return;
				}
			}
		}
	}

	public synchronized DatabaseConnectionSettings getAuthoritativeDatabaseConnectionSettings() {
		if (configuration.isUseAutoRebuild()) {
			String encodedSettings = "";
			final String rawPrefsValue = prefs.get(getClusterLabel(), null);
			if (StringCheck.isNotEmptyNorNull(rawPrefsValue)) {
				try {
					encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
				} catch (UnableToDecryptInput ex) {
					LOG.log(Level.SEVERE, null, ex);
					encodedSettings = rawPrefsValue;
				}
			}
			if (StringCheck.isNotEmptyNorNull(encodedSettings)) {
				DatabaseConnectionSettings settings = DatabaseConnectionSettings.decode(encodedSettings);
				return settings;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	public boolean clusterContains(DBDatabase database) {
		return members.contains(database);
	}

	/**
	 * @return the clusterLabel
	 */
	public String getClusterLabel() {
		return clusterLabel;
	}

	/**
	 * @param clusterLabel the clusterLabel to set
	 */
	public void setClusterLabel(String clusterLabel) {
		this.clusterLabel = clusterLabel;
		setAuthoritativeDatabase();
	}

	public DBDatabase[] getQuarantinedDatabases() {
		return members.getDatabases(DBDatabaseCluster.Status.QUARANTINED);
	}

	public void removeAllDatabases() throws SQLException {
		members.clear();
	}

	public synchronized void dismantle() throws SQLException {
		try {
			removeAllDatabases();
		} catch (Exception ex) {
			LOG.warning(ex.getLocalizedMessage());
		}
		try {
			removeAuthoritativeDatabaseFromPrefs();
		} catch (Exception ex) {
			LOG.warning(ex.getLocalizedMessage());
		}
		try {
			removeAddedDatabasesFromPrefs();
		} catch (Exception ex) {
			LOG.warning(ex.getLocalizedMessage());
		}
		try {
			removedTrackedTablesFromPrefs();
		} catch (Exception ex) {
			LOG.warning(ex.getLocalizedMessage());
		}
	}

	public boolean getAutoReconnect() {
		return configuration.isUseAutoReconnect();
	}

	public boolean getAutoRebuild() {
		return configuration.isUseAutoRebuild();
	}

	public boolean hasAuthoritativeDatabase() {
		return this.getAuthoritativeDatabaseConnectionSettings() != null;
	}

	public synchronized void setSupportsDifferenceBetweenNullAndEmptyString(boolean result) {
		supportsDifferenceBetweenNullAndEmptyString = result;
	}

	public synchronized boolean getSupportsDifferenceBetweenNullAndEmptyString() {
		checkSupportForDifferenceBetweenNullAndEmptyString();
		return supportsDifferenceBetweenNullAndEmptyString;
	}

	private void checkSupportForDifferenceBetweenNullAndEmptyString() {
		boolean supportsDifference = true;
		for (DBDatabase database : getAllDatabases()) {
			supportsDifference = supportsDifference && database.supportsDifferenceBetweenNullAndEmptyString();
		}
		setSupportsDifferenceBetweenNullAndEmptyString(supportsDifference);
	}

	public void printAllFormerDatabases() {
		allAddedDatabases.forEach(db -> {
			System.out.println("DB: " + db);
		});
	}

	public void setQuietExceptionsPreference(boolean bln) {
		this.quietExceptions = bln;
	}

	public void setConfiguration(DBDatabaseCluster.Configuration config) {
		this.configuration = config;
	}

	private synchronized void removeAddedDatabasesFromPrefs() {
		prefs.remove(getPrefsClusterSettingsKey());
	}

	private synchronized void saveClusterSettingsToPrefs() {
		if (configuration.isUseAutoConnect()) {
			final String name = getPrefsClusterSettingsKey();
			try {
				final String encode = clusterSettings.encode();
				final String encrypt = Encryption_Internal.encrypt(encode);
				prefs.put(name, encrypt);
			} catch (CannotEncryptInputException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	private String getPrefsClusterSettingsKey() {
		return getClusterLabel() + "_settings";
	}

	public synchronized List<DBDatabase> getClusterHostsFromPrefs() {
		List<DBDatabase> databases = new ArrayList<>();
		if (configuration.isUseAutoConnect()) {
			String encodedSettings = "";
			final String rawPrefsValue = prefs.get(getPrefsClusterSettingsKey(), null);
			if (StringCheck.isNotEmptyNorNull(rawPrefsValue)) {
				try {
					encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
				} catch (UnableToDecryptInput ex) {
					LOG.log(Level.SEVERE, null, ex);
					encodedSettings = rawPrefsValue;
				}
			}
			if (StringCheck.isNotEmptyNorNull(encodedSettings)) {
				try {
					final DatabaseConnectionSettings settings = DatabaseConnectionSettings.decode(encodedSettings);
					List<DatabaseConnectionSettings> decodedSettings = settings.getClusterHosts();
					for (DatabaseConnectionSettings host : decodedSettings) {
						try {
							final DBDatabase db = host.createDBDatabase();
							databases.add(db);
						} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
							Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
						}
					}
				} catch (Exception exc) {
					exc.printStackTrace();
				}
			}
		}
		return databases;
	}

	public boolean isSynchronized() {
		if (configuration.isUseAutoReconnect()) {
			return members.getDatabases(DBDatabaseCluster.Status.READY).length == members.size();
		} else {
			return members.getDatabases(
					DBDatabaseCluster.Status.READY,
					DBDatabaseCluster.Status.QUARANTINED,
					DBDatabaseCluster.Status.DEAD).length
					== members.size();
		}
	}

	public boolean isNotSynchronized() {
		return !isSynchronized();
	}

	public void waitUntilSynchronised() {
		synchronisingLock.lock();
		try {
			while (isNotSynchronized()) {
				allDatabasesAreSynchronised.await(1, TimeUnit.SECONDS);
			}
		} catch (InterruptedException ex) {
			Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			synchronisingLock.unlock();
		}
	}

	public void waitUntilDatabaseHasSynchronised(DBDatabase db) {
		waitUntilDatabaseHasSynchronised(db, 0L);
	}

	public void waitUntilDatabaseHasSynchronised(DBDatabase database, long timeoutInMilliseconds) {
		synchronisingLock.lock();
		try {
			if (isEligibleForSynchronizing(database) && getStatusOf(database) != DBDatabaseCluster.Status.READY) {
				if (timeoutInMilliseconds > 0) {
					aDatabaseHasBeenSynchronised.await(timeoutInMilliseconds, TimeUnit.MILLISECONDS);
				} else {
					while (clusterContains(database) && getStatusOf(database) != DBDatabaseCluster.Status.READY) {
						aDatabaseHasBeenSynchronised.await(100, TimeUnit.MILLISECONDS);
					}
				}
				DBDatabaseCluster.Status status = getStatusOf(database);
				if (status.equals(DBDatabaseCluster.Status.READY)) {
					return;
				}
			}
		} catch (InterruptedException ex) {
			Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			synchronisingLock.unlock();
		}
	}

	private boolean isEligibleForSynchronizing(DBDatabase database) {
		final DBDatabaseCluster.Status statusOfDatabase = getStatusOf(database);
		final boolean notQuarantined = statusOfDatabase != DBDatabaseCluster.Status.QUARANTINED;
		return clusterContains(database) && (notQuarantined || configuration.isUseAutoReconnect());
	}

	public void synchronizeSecondaryDatabases() {
		DBDatabase[] addedDBs;
		addedDBs = members.getDatabases(DBDatabaseCluster.Status.UNSYNCHRONISED);
		for (DBDatabase db : addedDBs) {
			//Do The Synchronising...
			synchronizeSecondaryDatabase(db);
		}
	}

	public synchronized void synchronizeSecondaryDatabase(DBDatabase secondary) {
		members.setSynchronising(secondary);

		DBDatabase template = null;
		boolean proceedWithSynchronization = true;
		final String secondaryLabel = secondary.getLabel();
		LOG.log(Level.FINEST, "{0} SYNCHRONISING: {1}", new Object[]{clusterLabel, secondaryLabel});
		try {
			// we need to unpause the template no matter what happens so use a finally clause
			try {
				template = getTemplateDatabase();
				if (proceedWithSynchronization && template != null) {
					// Check that we're not synchronising the reference database
					if (!template.getSettings().equals(secondary.getSettings())) {
						LOG.log(Level.FINEST, "{0} CAN SYNCHRONISE: {1}", new Object[]{clusterLabel, secondaryLabel});
						if(copyTemplateActionQueueToSecondary(template, secondary)){
						for (DBRow table : getRequiredAndTrackedTables()) {
							final String tableName = table.getTableName();
							if (proceedWithSynchronization) {
								LOG.log(Level.FINEST, "{0} CHECKING TABLE: {1}", new Object[]{clusterLabel, tableName});
								// make sure the table exists in the cluster already
								if (template.tableExists(table)) {
									LOG.log(Level.FINEST, "{0} INCLUDES TABLE: {1}", new Object[]{clusterLabel, tableName});
									// Make sure it exists in the new database
									if (secondary.tableExists(table) == true) {
										LOG.log(Level.FINEST, "{0} REMOVING FROM {1}: {2}", new Object[]{clusterLabel, secondaryLabel, tableName});
										secondary.preventDroppingOfTables(false);
										secondary.dropTableNoExceptions(table);
									}
									LOG.log(Level.FINEST, "{0} CREATING ON {1}: {2}", new Object[]{clusterLabel, secondaryLabel, tableName});
									secondary.createTable(table);
									LOG.log(Level.FINEST, "{0} CREATED ON {1}: {2}", new Object[]{clusterLabel, secondaryLabel, tableName});
									// Check that the table has data
									final DBTable<DBRow> primaryTable = template.getDBTable(table);
									try {
										final Long primaryTableCount = primaryTable.count();
										try {
											if (primaryTableCount > 0) {
												final DBTable<DBRow> primaryData = primaryTable.setBlankQueryAllowed(true).setTimeoutToForever();
												// Check that the new database has data
												LOG.log(Level.FINEST, "{0} CLUSTER FILLING TABLE ON {1}:{2}", new Object[]{clusterLabel, secondaryLabel, tableName});
												List<DBRow> allRows = primaryData.getAllRows();
												LOG.log(Level.FINEST, "{0} CLUSTER FILLING TABLE ON {1}:{2} with {3} rows", new Object[]{clusterLabel, secondaryLabel, tableName, allRows.size()});
												final DBTable<DBRow> secondaryTable = secondary.getDBTable(table);
												try {
													secondaryTable.insert(allRows);
													LOG.log(Level.FINEST, "{0} FILLED TABLE ON {1}:{2}", new Object[]{clusterLabel, secondaryLabel, tableName});
												} catch (SQLException ex) {
													proceedWithSynchronization = false;
													LOG.log(Level.SEVERE, "QUARANTINING DATABASE {0}: {1}", new Object[]{secondaryLabel, ex.getLocalizedMessage()});
													ex.printStackTrace();
													quarantineDatabaseAutomatically(secondary, ex);
													//exit the loop, to avoid unnecessary tests
													break;
												}
											}
										} catch (SQLException exceptionGettingData) {
											LOG.log(Level.WARNING, "FAIL TO RETREIVE TABLE DATA: {0} - {1}", new Object[]{tableName, exceptionGettingData.getLocalizedMessage()});
											LOG.log(Level.WARNING, "SKIPPING TABLE: {0} - {1}", new Object[]{tableName, exceptionGettingData.getLocalizedMessage()});
											// lets just skip this table since it seems to be broken
										}
									} catch (SQLException exceptionCountingPrimaryTable) {
										LOG.log(Level.WARNING, "FAILED TO COUNT TABLE: {0} - {1}", new Object[]{tableName, exceptionCountingPrimaryTable.getLocalizedMessage()});
										LOG.log(Level.WARNING, "SKIPPING TABLE: {0} - {1}", new Object[]{tableName, exceptionCountingPrimaryTable.getLocalizedMessage()});
										// lets just skip this table since it seems to be broken
									}
								}
							}
							LOG.log(Level.FINEST, "{0} FINISHED WITH TABLE: {1}", new Object[]{clusterLabel, tableName});
						}}
					}
				}
			} catch (NoAvailableDatabaseException except) {
				// must be the first database
			} catch (Throwable throwable) {
				proceedWithSynchronization = false;
				LOG.severe(throwable.getLocalizedMessage());
				throwable.printStackTrace();
			}
			if (proceedWithSynchronization) {
				LOG.log(Level.FINEST, "{0} START SYNCHRONISING ACTIONS ON: {1}", new Object[]{clusterLabel, secondaryLabel});
				synchronizeActions(secondary);
			}
		} finally {
			releaseTemplateDatabase(template);
		}
		// Successfully synchronised the new database :)
	}

	private synchronized void releaseTemplateDatabase(DBDatabase primary) throws NoAvailableDatabaseException {
		if (primary != null) {
			if (clusterContains(primary)) {
				synchronizeActions(primary);
			} else {
				LOG.log(Level.INFO, "SYNCHRONISING - STOPPING {0} {1}", new Object[]{primary.getLabel(), primary.getJdbcURL()});
				primary.stop();
			}
		}
	}

	private synchronized boolean copyTemplateActionQueueToSecondary(DBDatabase template, DBDatabase secondary) {
		boolean result = false;
		Queue<DBAction> templateQ = getActionQueue(template);
		Queue<DBAction> secondaryQ = getActionQueue(secondary);
		secondaryQ.clear();
		secondaryQ.addAll(templateQ);
		return result;
	}

	private synchronized void synchronizeActions(DBDatabase db) throws NoAvailableDatabaseException {
		if (db != null) {
			try {
				Queue<DBAction> queue = getActionQueue(db);
				while (queue != null && !queue.isEmpty()) {
					DBAction action = queue.remove();
					db.executeDBAction(action);
				}
				readyDatabase(db);
			} catch (SQLException e) {
				quarantineDatabase(db, e);
			}
		}
	}

	public void quarantineDatabaseAutomatically(DBDatabase suspectDatabase, Throwable sqlException) {
		try {
			quarantineDatabase(suspectDatabase, sqlException);
		} catch (UnableToRemoveLastDatabaseFromClusterException doesntNeedToBeHandledAsItsAutomaticAndNotManual) {
			;
		}
	}

	private void signalThatAllDatabasesHaveBeenSynchronised() {
		synchronisingLock.lock();
		try {
			allDatabasesAreSynchronised.signalAll();
		} finally {
			synchronisingLock.unlock();
		}
	}

	private void signalThatADatabaseHasBeenSynchronised() {
		synchronisingLock.lock();
		try {
			aDatabaseHasBeenSynchronised.signalAll();
			if (isSynchronized()) {
				signalThatAllDatabasesHaveBeenSynchronised();
			}
		} finally {
			synchronisingLock.unlock();
		}
	}

	public void setClusterSettings(DatabaseConnectionSettings settings) {
		this.clusterSettings = settings;
	}

	public DBDatabaseCluster.Status getStatusOf(DBDatabase db) {
		return members.getStatusOf(db);
	}

	public void setPreferredDatabase(DBDatabase database) {
		preferredDatabase = database;
	}

	public boolean hasPreferredDatabase() {
		return preferredDatabase != null;
	}

	private boolean preferredDatabaseIsReady() {
		return getStatusOf(preferredDatabase).equals(DBDatabaseCluster.Status.READY);
	}

	public void setPreferredDatabaseRequired(boolean b) {
		preferredDatabaseRequired = b;
	}

	public boolean isPreferredDatabaseRequired() {
		return preferredDatabaseRequired;
	}

	public DBDatabase[] getDatabasesForReconnecting() {
		return members.getDatabases(DBDatabaseCluster.Status.QUARANTINED, DBDatabaseCluster.Status.DEAD);
	}

	public DBDatabase getPreferredDatabase() {
		return preferredDatabase;
	}
}
