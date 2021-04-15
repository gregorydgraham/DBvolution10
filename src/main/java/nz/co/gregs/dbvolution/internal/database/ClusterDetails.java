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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.prefs.Preferences;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException;
import nz.co.gregs.dbvolution.exceptions.UnableToDecryptInput;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import nz.co.gregs.dbvolution.generation.DBTableClass;
import nz.co.gregs.dbvolution.reflection.DataModel;
import nz.co.gregs.dbvolution.utility.encryption.Encryption_Internal;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author gregorygraham
 */
public class ClusterDetails implements Serializable {

	private final static long serialVersionUID = 1l;

	private static final Log LOG = LogFactory.getLog(ClusterDetails.class);

	private final List<DBDatabase> allDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> unsynchronizedDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> readyDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> pausedDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> quarantinedDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));

	private final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
	private final Set<DBRow> trackedTables = Collections.synchronizedSet(new HashSet<DBRow>());
	private final transient Map<DBDatabase, Queue<DBAction>> queuedActions = Collections.synchronizedMap(new HashMap<DBDatabase, Queue<DBAction>>(0));

	private final Preferences prefs = Preferences.userNodeForPackage(this.getClass());
	private String clusterLabel = "NotDefined";
	private boolean useAutoRebuild = false;
	private boolean autoreconnect = false;
	private boolean supportsDifferenceBetweenNullAndEmptyString = true;
	private final ArrayList<String> allAddedDatabases = new ArrayList<String>();

	public ClusterDetails(String clusterName) {
		this();
		this.clusterLabel = clusterName;
	}

	public ClusterDetails(String clusterName, boolean autoRebuild) {
		this(clusterName);
		setAutoRebuild(autoRebuild);
	}

	public ClusterDetails() {
	}

	public final synchronized boolean add(DBDatabase databaseToAdd) throws SQLException {
		if (databaseToAdd != null) {
			DBDatabase database = getClusteredVersionOfDatabase(databaseToAdd);
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
				return unsynchronizedDatabases.add(database);
			} else {
				allAddedDatabases.add(database.getSettings().encode());
				unsynchronizedDatabases.add(database);
				return allDatabases.add(database);
			}
		}
		return false;
	}

	public DBDatabase[] getAllDatabases() {
		synchronized (allDatabases) {
			return allDatabases.toArray(new DBDatabase[]{});
		}
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase getStatusOfThisDatabase) {
		DBDatabase db = getClusteredVersionOfDatabase(getStatusOfThisDatabase);
		final boolean ready = readyDatabases.contains(db);
		final boolean paused = pausedDatabases.contains(db);
		final boolean quarantined = quarantinedDatabases.contains(db);
		final boolean unsynched = unsynchronizedDatabases.contains(db);
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
		return DBDatabaseCluster.Status.UNKNOWN;
	}

	private DBDatabase getClusteredVersionOfDatabase(DBDatabase getStatusOfThisDatabase) {
		DBDatabase db;
		db = getStatusOfThisDatabase;
		return db;
	}

	public synchronized void quarantineDatabase(DBDatabase databaseToQuarantine, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		DBDatabase database = getClusteredVersionOfDatabase(databaseToQuarantine);
		if (clusterContainsDatabase(database)) {
			if (hasTooFewReadyDatabases() && readyDatabases.contains(database)) {
				// Unable to quarantine the only remaining database
				throw new UnableToRemoveLastDatabaseFromClusterException();
			}

			System.out.println("QUARANTINING: " + databaseToQuarantine.getSettings().toString());
			database.setLastException(except);

			readyDatabases.remove(database);
			pausedDatabases.remove(database);
			unsynchronizedDatabases.remove(database);

			queuedActions.remove(database);

			quarantinedDatabases.add(database);

			setAuthoritativeDatabase();
		}
	}

	public synchronized boolean removeDatabase(DBDatabase databaseToRemove) throws SQLException {
		DBDatabase database = getClusteredVersionOfDatabase(databaseToRemove);
		if (hasTooFewReadyDatabases() && readyDatabases.contains(database)) {
			// Unable to quarantine the only remaining database
			throw new UnableToRemoveLastDatabaseFromClusterException();
		} else {
			final boolean result = removeDatabaseFromAllLists(database);
			if (result) {
				setAuthoritativeDatabase();
				checkSupportForDifferenceBetweenNullAndEmptyString();
			}
			return result;
		}
	}

	protected boolean hasTooFewReadyDatabases() {
		return readyDatabases.size() < 2;
	}

	private synchronized boolean removeDatabaseFromAllLists(DBDatabase databaseToRemove) throws SQLException {
		DBDatabase database = getClusteredVersionOfDatabase(databaseToRemove);
		LOG.info("REMOVING: " + database.getLabel());
		boolean result = queuedActions.containsKey(database) ? queuedActions.remove(database) != null : true;
		result = result && quarantinedDatabases.contains(database) ? quarantinedDatabases.remove(database) : true;
		result = result && unsynchronizedDatabases.contains(database) ? unsynchronizedDatabases.remove(database) : true;
		result = result && pausedDatabases.contains(database) ? pausedDatabases.remove(database) : true;
		result = result && readyDatabases.contains(database) ? readyDatabases.remove(database) : true;
		result = result && allDatabases.contains(database) ? allDatabases.remove(database) : true;
		return result;
	}

	public synchronized DBDatabase[] getUnsynchronizedDatabases() {
		return unsynchronizedDatabases.toArray(new DBDatabase[]{});
	}

	public synchronized void synchronizingDatabase(DBDatabase datbaseIsSynchronized) throws SQLException {
		DBDatabase database = getClusteredVersionOfDatabase(datbaseIsSynchronized);
		unsynchronizedDatabases.remove(database);
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

	public synchronized DBRow[] getTrackedTables() {
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
	}

	public void addTrackedTable(DBRow row) {
		synchronized (trackedTables) {
			trackedTables.add(row);
		}
	}

	public void addTrackedTables(Collection<DBRow> rows) {
		synchronized (trackedTables) {
			trackedTables.addAll(rows);
		}
	}

	public synchronized void readyDatabase(DBDatabase databaseToReady) throws SQLException {
		DBDatabase secondary = getClusteredVersionOfDatabase(databaseToReady); // new DBDatabase(databaseToReady);
		unsynchronizedDatabases.remove(secondary);
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
			DBDatabase database = databaseToPause; // new DBDatabase(databaseToPause);
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
				Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
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
//		return null;
	}

	public synchronized void addAll(DBDatabase[] databases) throws SQLException {
		for (DBDatabase database : databases) {
			add(database);
		}
	}

	public synchronized DBDatabase getTemplateDatabase() throws NoAvailableDatabaseException {
//		if (allDatabases.isEmpty()) {
		if (allDatabases.size() < 2) {
			final DatabaseConnectionSettings authoritativeDCS = getAuthoritativeDatabaseConnectionSettings();
			if (authoritativeDCS != null) {
				try {
					return getClusteredVersionOfDatabase(authoritativeDCS.createDBDatabase());
				} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
					Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
					throw new NoAvailableDatabaseException();
				}
			} else {
				return null;
			}
		} else {
			if (readyDatabases.isEmpty() && pausedDatabases.isEmpty()) {
				throw new NoAvailableDatabaseException();
			}
			return getPausedDatabase();
		}
	}

	private synchronized void setAuthoritativeDatabase() {
		if (useAutoRebuild) {
			for (DBDatabase db : allDatabases) {
				final String name = getClusterLabel();
				if (!db.isMemoryDatabase() && name != null && !name.isEmpty()) {
					final String encode = db.getSettings().encode();
					try {
						prefs.put(name, Encryption_Internal.encrypt(encode));
					} catch (CannotEncryptInputException ex) {
						Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
						prefs.put(name, encode);
					}
					return;
				}
			}
		}
	}

	private synchronized void removeAuthoritativeDatabase() {
		prefs.remove(getClusterLabel());
	}

	public DatabaseConnectionSettings getAuthoritativeDatabaseConnectionSettings() {
		if (useAutoRebuild) {
			String encodedSettings = "";
			final String rawPrefsValue = prefs.get(getClusterLabel(), null);
			if (rawPrefsValue != null) {
				try {
					encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
				} catch (UnableToDecryptInput ex) {
					Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
					encodedSettings = rawPrefsValue;
				}
			}
			if (encodedSettings != null && !encodedSettings.isEmpty()) {
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
			for (DBDatabase db : allDatabases) {
				if (db.getSettings().equals(newEncode)) {
					return true;
				}
			}
		}
		return false;
	}

	public final void setAutoRebuild(boolean b) {
		useAutoRebuild = b;
		if (useAutoRebuild) {
			setAuthoritativeDatabase();
		} else {
			removeAuthoritativeDatabase();
		}
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

	public List<DBDatabase> getQuarantinedDatabases() {
		return quarantinedDatabases
				.stream()
				.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
	}

	public synchronized void removeAllDatabases() throws SQLException {
		DBDatabase[] dbs = allDatabases.toArray(new DBDatabase[]{});
		for (DBDatabase db : dbs) {
			removeDatabaseFromAllLists(db);
		}
	}

	public synchronized void dismantle() throws SQLException {
		removeAuthoritativeDatabase();
		removeAllDatabases();
	}

	public void setAutoReconnect(boolean useAutoReconnect) {
		this.autoreconnect = useAutoReconnect;
	}

	public boolean getAutoReconnect() {
		return this.autoreconnect;
	}

	public boolean getAutoRebuild() {
		return this.useAutoRebuild;
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
}
