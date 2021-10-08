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

	private final DatabaseList allDatabases = new DatabaseList();
	private final DatabaseList unsynchronizedDatabases = new DatabaseList();
	private final DatabaseList synchronizingDatabases = new DatabaseList();
	private final DatabaseList readyDatabases = new DatabaseList();
	private final DatabaseList pausedDatabases = new DatabaseList();
	private final DatabaseList quarantinedDatabases = new DatabaseList();

	private final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
	private final Set<DBRow> trackedTables = Collections.synchronizedSet(new HashSet<DBRow>());
	private final transient Map<DBDatabase, Queue<DBAction>> queuedActions = Collections.synchronizedMap(new HashMap<DBDatabase, Queue<DBAction>>(0));

	private final PreferencesImproved prefs = PreferencesImproved.userNodeForPackage(this.getClass());
	private String clusterLabel = "NotDefined";
	private boolean supportsDifferenceBetweenNullAndEmptyString = true;
	private final ArrayList<String> allAddedDatabases = new ArrayList<String>();
	private boolean quietExceptions = false;
	private DBDatabaseCluster.Configuration configuration = DBDatabaseCluster.Configuration.fullyManual();

	private final Lock synchronisingLock = new ReentrantLock();
	private final Condition allDatabasesAreSynchronised = synchronisingLock.newCondition();
	private final Condition someDatabasesNeedSynchronizing = synchronisingLock.newCondition();

	public ClusterDetails(String clusterName) {
		this.clusterLabel = clusterName;
	}

	public final synchronized boolean add(DBDatabase databaseToAdd) throws SQLException {
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

			if (clusterContainsDatabase(database)) {
				readyDatabases.remove(database);
				pausedDatabases.remove(database);
				quarantinedDatabases.remove(database);
				return addDatabaseAsUnsynchronized(database);
			} else {
				allAddedDatabases.add(database.getSettings().encode());
				addDatabaseAsUnsynchronized(database);
				boolean added = allDatabases.add(database);
				if (added) {
					saveAddedDatabasesToPrefs();
				}
				return added;
			}
		}
		return false;
	}

	private boolean addDatabaseAsUnsynchronized(DBDatabase database) {
		boolean add = false;
		try {
			synchronisingLock.lock();
			add = unsynchronizedDatabases.add(database);
			someDatabasesNeedSynchronizing.signalAll();
		} finally {
			synchronisingLock.unlock();
		}
		return add;
	}

	public DBDatabase[] getAllDatabases() {
		synchronized (allDatabases) {
			return allDatabases.toArray(new DBDatabase[]{});
		}
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase getStatusOfThisDatabase) {
		DBDatabase db = getStatusOfThisDatabase;
		final boolean ready = readyDatabases.contains(db);
		final boolean paused = pausedDatabases.contains(db);
		final boolean quarantined = quarantinedDatabases.contains(db);
		final boolean unsynched = unsynchronizedDatabases.contains(db);
		final boolean synching = synchronizingDatabases.contains(db);
		if (ready) {
			return DBDatabaseCluster.Status.READY;
		}
		if (paused) {
			return DBDatabaseCluster.Status.PAUSED;
		}
		if (quarantined) {
			return DBDatabaseCluster.Status.QUARANTINED;
		}
		if (unsynched) {
			return DBDatabaseCluster.Status.UNSYNCHRONISED;
		}
		if (synching) {
			return DBDatabaseCluster.Status.SYNCHRONIZING;
		}
		return DBDatabaseCluster.Status.UNKNOWN;
	}

	public synchronized void quarantineDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		if (clusterContainsDatabase(database)) {
			if (hasTooFewReadyDatabases() && readyDatabases.contains(database)) {
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

			readyDatabases.remove(database);
			pausedDatabases.remove(database);
			removeDatabaseFromUnsynchronized(database);
			removeDatabaseFromSynchronizing(database);

			queuedActions.remove(database);

			quarantinedDatabases.add(database);

			setAuthoritativeDatabase();
		}
	}

	private synchronized boolean removeDatabaseFromUnsynchronized(DBDatabase database) {
		return unsynchronizedDatabases.remove(database);
	}

	public synchronized boolean removeDatabase(DBDatabase databaseToRemove) throws SQLException {
		DBDatabase database = databaseToRemove;
		if (hasTooFewReadyDatabases() && readyDatabases.contains(database)) {
			// Unable to quarantine the only remaining database
			throw new UnableToRemoveLastDatabaseFromClusterException();
		} else {
			final boolean result = removeDatabaseFromAllLists(database);
			if (result) {
				setAuthoritativeDatabase();
				saveAddedDatabasesToPrefs();
				checkSupportForDifferenceBetweenNullAndEmptyString();
			}
			return result;
		}
	}

	protected boolean hasTooFewReadyDatabases() {
		return readyDatabases.size() < 2;
	}

	private synchronized boolean removeDatabaseFromAllLists(DBDatabase databaseToRemove) throws SQLException {
		DBDatabase database = databaseToRemove;
		LOG.log(Level.INFO, "REMOVING: {0}", database.getLabel());
		boolean result = removeDatabaseFromAllLists_Internal(database);
		return result;
	}

	private synchronized boolean removeDatabaseFromAllLists_Internal(DBDatabase database) {
		boolean result = queuedActions.containsKey(database) ? queuedActions.remove(database) != null : true;
		result = result && quarantinedDatabases.contains(database) ? quarantinedDatabases.remove(database) : true;
		result = result && unsynchronizedDatabases.contains(database) ? removeDatabaseFromUnsynchronized(database) : true;
		result = result && synchronizingDatabases.contains(database) ? removeDatabaseFromUnsynchronized(database) : true;
		result = result && pausedDatabases.contains(database) ? pausedDatabases.remove(database) : true;
		result = result && readyDatabases.contains(database) ? readyDatabases.remove(database) : true;
		result = result && allDatabases.contains(database) ? allDatabases.remove(database) : true;
		return result;
	}

	public synchronized DBDatabase[] getUnsynchronizedDatabases() {
		return unsynchronizedDatabases.toArray(new DBDatabase[]{});
	}

	public synchronized void synchronizingDatabase(DBDatabase datbaseIsSynchronized) throws SQLException {
		DBDatabase database = datbaseIsSynchronized;
		removeDatabaseFromUnsynchronized(database);
		synchronizingDatabases.add(database);
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
			saveTrackedTables();
		}
	}

	public void addTrackedTable(DBRow row) {
		synchronized (trackedTables) {
			trackedTables.add(row);
			saveTrackedTables();
		}
	}

	public void addTrackedTables(Collection<DBRow> rows) {
		synchronized (trackedTables) {
			trackedTables.addAll(rows);
			saveTrackedTables();
		}
	}

	public void removeTrackedTable(DBRow row) {
		synchronized (trackedTables) {
			trackedTables.remove(row);
			saveTrackedTables();
		}
	}

	public void removeTrackedTables(Collection<DBRow> rows) {
		synchronized (trackedTables) {
			trackedTables.removeAll(rows);
			saveTrackedTables();
		}
	}

	public synchronized void readyDatabase(DBDatabase databaseToReady) throws SQLException {
		DBDatabase secondary = databaseToReady;
		removeDatabaseFromUnsynchronized(secondary);
		removeDatabaseFromSynchronizing(secondary);
		pausedDatabases.remove(secondary);
		try {
			if (hasReadyDatabases()) {
				DBDatabase readyDatabase = getReadyDatabase();
				if (readyDatabase != null) {
					secondary.setPrintSQLBeforeExecuting(readyDatabase.getPrintSQLBeforeExecuting());
					secondary.setBatchSQLStatementsWhenPossible(readyDatabase.getBatchSQLStatementsWhenPossible());
				}
			}
		} catch (NoAvailableDatabaseException ex) {

		}
		readyDatabases.add(secondary);
		setAuthoritativeDatabase();
	}

	protected boolean hasReadyDatabases() {
		return readyDatabases.size() > 0;
	}

	public synchronized DBDatabase[] getReadyDatabases() {
		if (readyDatabases == null || readyDatabases.isEmpty()) {
			return new DBDatabase[]{};
		} else {
			return readyDatabases.toArray(new DBDatabase[]{});
		}
	}

	public synchronized void pauseDatabase(DBDatabase databaseToPause) {
		if (databaseToPause != null) {
			DBDatabase database = databaseToPause;
			readyDatabases.remove(database);
			pausedDatabases.add(database);
		}
	}

	public synchronized DBDatabase getPausedDatabase() throws NoAvailableDatabaseException {
		DBDatabase template = getReadyDatabase();
		pauseDatabase(template);
		return template;
	}

	public DBDatabase getReadyDatabase() throws NoAvailableDatabaseException {
		DBDatabase[] dbs = getReadyDatabases();
		int tries = 0;
		while (dbs.length < 1 && pausedDatabases.size() > 0 && tries <= 1000) {
			tries++;
			try {
				Thread.sleep(1);
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
			dbs = getReadyDatabases();
		}
		Random rand = new Random();
		if (dbs.length > 0) {
			final int randNumber = rand.nextInt(dbs.length);
			DBDatabase randomElement = dbs[randNumber];
			return randomElement;
		}
		throw new NoAvailableDatabaseException();
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
		if (allDatabases.size() == 1 && configuration.isUseAutoRebuild()) {
			return getAuthoritativeDatabase();
		} else {
			if (readyDatabases.isEmpty() && pausedDatabases.isEmpty()) {
				throw new NoAvailableDatabaseException();
			}
			return getPausedDatabase();
		}
	}

	private DBDatabase getAuthoritativeDatabase() throws NoAvailableDatabaseException {
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
			for (DBDatabase db : readyDatabases.toArray()) {
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

	public boolean clusterContainsDatabase(DBDatabase database) {
		if (database != null) {
			final DatabaseConnectionSettings newEncode = database.getSettings();
			for (DBDatabase db : allDatabases.toArray()) {
				if (db.getSettings().equals(newEncode)) {
					return true;
				}
			}
		}
		return false;
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
		return quarantinedDatabases.toArray();
	}

	public synchronized void removeAllDatabases() throws SQLException {
		DBDatabase[] dbs = allDatabases.toArray(new DBDatabase[]{});
		for (DBDatabase db : dbs) {
			removeDatabaseFromAllLists(db);
		}
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
		prefs.remove(getAddedDatabasesPrefsIdentifier());
	}

	private synchronized void saveAddedDatabasesToPrefs() {
		if (configuration.isUseAutoConnect()) {
			final String name = getAddedDatabasesPrefsIdentifier();
			SeparatedString databaseConnections = getAddedDatabasesSeparatedStringTemplate();
			for (DBDatabase db : allDatabases.toArray()) {
				final String encode = db.getSettings().encode();
				databaseConnections.add(encode);
			}
			String encodedDBs = databaseConnections.encode();
			try {
				prefs.put(name, Encryption_Internal.encrypt(encodedDBs));
			} catch (CannotEncryptInputException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	private String getAddedDatabasesPrefsIdentifier() {
		return getClusterLabel() + "_addeddatabases";
	}

	private SeparatedString getAddedDatabasesSeparatedStringTemplate() {
		return SeparatedStringBuilder.commaSeparated();
	}

	public synchronized List<DBDatabase> getAddedDatabasesFromPrefs() {
		List<DBDatabase> databases = new ArrayList<>();
		if (configuration.isUseAutoConnect()) {
			String encodedSettings = "";
			final String rawPrefsValue = prefs.get(getAddedDatabasesPrefsIdentifier(), null);
			if (StringCheck.isNotEmptyNorNull(rawPrefsValue)) {
				try {
					encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
				} catch (UnableToDecryptInput ex) {
					LOG.log(Level.SEVERE, null, ex);
					encodedSettings = rawPrefsValue;
				}
			}
			if (StringCheck.isNotEmptyNorNull(encodedSettings)) {
				List<String> decodedSettings = getAddedDatabasesSeparatedStringTemplate().decode(encodedSettings);
				for (String setting : decodedSettings) {
					try {
						databases.add(DatabaseConnectionSettings.decode(setting).createDBDatabase());
					} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
						Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
			}
		}
		return databases;
	}

	public boolean isNotSynchronized() {
		return unsynchronizedDatabases.size() > 0
				|| synchronizingDatabases.size() > 0
				|| (configuration.isUseAutoReconnect() && quarantinedDatabases.size() > 0);
	}

	public void waitUntilSynchronised() {
		synchronisingLock.lock();
		try {
			while (isNotSynchronised()) {
				allDatabasesAreSynchronised.await();
			}
		} catch (InterruptedException ex) {
			Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			synchronisingLock.unlock();
		}
	}

	public boolean isSynchronised() {
		return unsynchronizedDatabases.isEmpty() && synchronizingDatabases.isEmpty();
	}

	public boolean isNotSynchronised() {
		return !isSynchronised();
	}

	public void synchronizeSecondaryDatabases() throws SQLException {
		DBDatabase[] addedDBs;
		addedDBs = getUnsynchronizedDatabases();
		for (DBDatabase db : addedDBs) {
			//Do The Synchronising...
			synchronizeSecondaryDatabase(db);
		}
	}

	public synchronized void synchronizeSecondaryDatabase(DBDatabase secondary) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		synchronizingDatabase(secondary);

		final String secondaryLabel = secondary.getLabel();
		LOG.log(Level.FINEST, "{0} SYNCHRONISING: {1}", new Object[]{clusterLabel, secondaryLabel});
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
						LOG.log(Level.FINEST, "{0} CAN SYNCHRONISE: {1}", new Object[]{clusterLabel, secondaryLabel});
						for (DBRow table : getRequiredAndTrackedTables()) {
							LOG.log(Level.FINEST, "{0} CHECKING TABLE: {1}", new Object[]{clusterLabel, table.getTableName()});
							// make sure the table exists in the cluster already
							if (template.tableExists(table)) {
								LOG.log(Level.FINEST, "{0} INCLUDES TABLE: {1}", new Object[]{clusterLabel, table.getTableName()});
								// Make sure it exists in the new database
								if (secondary.tableExists(table) == true) {
									LOG.log(Level.FINEST, "{0} REMOVING FROM {1}: {2}", new Object[]{clusterLabel, secondaryLabel, table.getTableName()});
									secondary.preventDroppingOfTables(false);
									secondary.dropTableNoExceptions(table);
								}
								LOG.log(Level.FINEST, "{0} CREATING ON {1}: {2}", new Object[]{clusterLabel, secondaryLabel, table.getTableName()});
								secondary.createTable(table);
								LOG.log(Level.FINEST, "{0} CREATED ON {1}: {2}", new Object[]{clusterLabel, secondaryLabel, table.getTableName()});
								// Check that the table has data
								final DBTable<DBRow> primaryTable = template.getDBTable(table);
								final DBTable<DBRow> secondaryTable = secondary.getDBTable(table);
								final Long primaryTableCount = primaryTable.count();
								if (primaryTableCount > 0) {
									final DBTable<DBRow> primaryData = primaryTable.setBlankQueryAllowed(true).setTimeoutToForever();
									// Check that the new database has data
									LOG.log(Level.FINEST, "{0} CLUSTER FILLING TABLE ON {1}:{2}", new Object[]{clusterLabel, secondaryLabel, table.getTableName()});
									List<DBRow> allRows = primaryData.getAllRows();
									secondaryTable.insert(allRows);
									LOG.log(Level.FINEST, "{0} FILLED TABLE ON {1}:{2}", new Object[]{clusterLabel, secondaryLabel, table.getTableName()});
								}
							}
							LOG.log(Level.FINEST, "{0} FINSHED WITH TABLE: {1}", new Object[]{clusterLabel, table.getTableName()});
						}
					}
				} catch (Throwable e) {
					throw e;
				} finally {
					releaseTemplateDatabase(template);
				}
			}
			LOG.log(Level.FINEST, "{0} START SYNCHRONISING ACTIONS ON: {1}", new Object[]{clusterLabel, secondaryLabel});
			synchronizeActions(secondary);
		} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException | AutoCommitActionDuringTransactionException ex) {
			quarantineDatabaseAutomatically(secondary, ex);
		}
	}

	private synchronized void releaseTemplateDatabase(DBDatabase primary) throws SQLException, NoAvailableDatabaseException {
		if (primary != null) {
			if (clusterContainsDatabase(primary)) {
				synchronizeActions(primary);
			} else {
				primary.stop();
			}
		}
	}

	private synchronized void synchronizeActions(DBDatabase db) throws SQLException, NoAvailableDatabaseException, NoAvailableDatabaseException {
		if (db != null) {
			Queue<DBAction> queue = getActionQueue(db);
			while (queue != null && !queue.isEmpty()) {
				DBAction action = queue.remove();
				db.executeDBAction(action);
			}
			readyDatabase(db);
		}
	}

	public void quarantineDatabaseAutomatically(DBDatabase suspectDatabase, Throwable sqlException) {
		try {
			quarantineDatabase(suspectDatabase, sqlException);
		} catch (UnableToRemoveLastDatabaseFromClusterException doesntNeedToBeHandledAsItsAutomaticAndNotManual) {
			;
		}
	}

	private void removeDatabaseFromSynchronizing(DBDatabase database) {
		synchronizingDatabases.remove(database);
		if (synchronizingDatabases.isEmpty() && unsynchronizedDatabases.isEmpty()) {
			try {
				synchronisingLock.lock();
				allDatabasesAreSynchronised.signalAll();
			} finally {
				synchronisingLock.unlock();
			}
		}
	}
}
