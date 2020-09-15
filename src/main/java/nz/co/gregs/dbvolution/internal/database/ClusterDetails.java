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

import nz.co.gregs.dbvolution.databases.ClusteredDatabase;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.prefs.Preferences;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBAction;
//import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException;
import nz.co.gregs.dbvolution.exceptions.UnableToDecryptInput;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
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

	private final List<ClusteredDatabase> allDatabases = Collections.synchronizedList(new ArrayList<ClusteredDatabase>(0));
	private final List<ClusteredDatabase> unsynchronizedDatabases = Collections.synchronizedList(new ArrayList<ClusteredDatabase>(0));
	private final List<ClusteredDatabase> readyDatabases = Collections.synchronizedList(new ArrayList<ClusteredDatabase>(0));
	private final List<ClusteredDatabase> pausedDatabases = Collections.synchronizedList(new ArrayList<ClusteredDatabase>(0));
	private final List<ClusteredDatabase> quarantinedDatabases = Collections.synchronizedList(new ArrayList<ClusteredDatabase>(0));

	private final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
	private final transient Map<ClusteredDatabase, Queue<DBAction>> queuedActions = Collections.synchronizedMap(new HashMap<ClusteredDatabase, Queue<DBAction>>(0));

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

	public final synchronized boolean add(ClusteredDatabase databaseToAdd) {
		if (databaseToAdd != null) {
			ClusteredDatabase database = databaseToAdd;
			final boolean supportsDifferenceBetweenNullAndEmptyString1 = getSupportsDifferenceBetweenNullAndEmptyString();
			boolean result = supportsDifferenceBetweenNullAndEmptyString1 && database.supportsDifferenceBetweenNullAndEmptyString();
			if (result != supportsDifferenceBetweenNullAndEmptyString1) {
				setSupportsDifferenceBetweenNullAndEmptyString(result);
				database.getDefinition().setRequiredToProduceEmptyStringsForNull(result);
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

	public ClusteredDatabase[] getAllDatabases() {
		synchronized (allDatabases) {
			return allDatabases.toArray(new ClusteredDatabase[]{});
		}
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(ClusteredDatabase getStatusOfThisDatabase) {
		ClusteredDatabase db = getStatusOfThisDatabase; // new ClusteredDatabase(getStatusOfThisDatabase);
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

	public synchronized void quarantineDatabase(ClusteredDatabase databaseToQuarantine, Exception except) throws UnableToRemoveLastDatabaseFromClusterException {
		ClusteredDatabase database = databaseToQuarantine;// new ClusteredDatabase(databaseToQuarantine);
		if (hasTooFewReadyDatabases() && readyDatabases.contains(database)) {
			// Unable to quarantine the only remaining database
			throw new UnableToRemoveLastDatabaseFromClusterException();
		} else {
//			except.printStackTrace();
			database.setLastException(except);

			readyDatabases.remove(database);
			pausedDatabases.remove(database);
			unsynchronizedDatabases.remove(database);

			queuedActions.remove(database);

			quarantinedDatabases.add(database);
			setAuthoritativeDatabase();
		}
	}

	public synchronized boolean removeDatabase(ClusteredDatabase databaseToRemove) {
		ClusteredDatabase database = databaseToRemove; //new ClusteredDatabase(databaseToRemove);
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

	private synchronized boolean removeDatabaseFromAllLists(ClusteredDatabase database) {
		LOG.info("REMOVING: " + database.getLabel());
		database.getDefinition().setRequiredToProduceEmptyStringsForNull(false);
		boolean result = queuedActions.containsKey(database) ? queuedActions.remove(database) != null : true;
		result = result && quarantinedDatabases.contains(database) ? quarantinedDatabases.remove(database) : true;
		result = result && unsynchronizedDatabases.contains(database) ? unsynchronizedDatabases.remove(database) : true;
		result = result && pausedDatabases.contains(database) ? pausedDatabases.remove(database) : true;
		result = result && readyDatabases.contains(database) ? readyDatabases.remove(database) : true;
		result = result && allDatabases.contains(database) ? allDatabases.remove(database) : true;
		return result;
	}

	public synchronized ClusteredDatabase[] getUnsynchronizedDatabases() {
		return unsynchronizedDatabases.toArray(new ClusteredDatabase[]{});
	}

	public synchronized void synchronizingDatabase(ClusteredDatabase datbaseIsSynchronized) {
		ClusteredDatabase database = datbaseIsSynchronized; //new ClusteredDatabase(datbaseIsSynchronized);
		unsynchronizedDatabases.remove(database);
	}

	public Queue<DBAction> getActionQueue(ClusteredDatabase db) {
		synchronized (queuedActions) {
			Queue<DBAction> queue = queuedActions.get(db);
			if (queue == null) {
				queue = new LinkedBlockingQueue<DBAction>();
				queuedActions.put(db, queue);
			}
			return queue;
		}
	}

	public DBRow[] getRequiredTables() {
		synchronized (requiredTables) {
			return requiredTables.toArray(new DBRow[]{});
		}
	}

	public synchronized void readyDatabase(ClusteredDatabase databaseToReady) {
		ClusteredDatabase secondary = databaseToReady; // new ClusteredDatabase(databaseToReady);
		unsynchronizedDatabases.remove(secondary);
		pausedDatabases.remove(secondary);
		try {
			if (hasReadyDatabases()) {
				ClusteredDatabase readyDatabase = getReadyDatabase();
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

	public synchronized ClusteredDatabase[] getReadyDatabases() {
		if (readyDatabases == null || readyDatabases.isEmpty()) {
			return new ClusteredDatabase[]{};
		} else {
			return readyDatabases.toArray(new ClusteredDatabase[]{});
		}
	}

	public synchronized void pauseDatabase(ClusteredDatabase databaseToPause) {
		if (databaseToPause != null) {
			ClusteredDatabase database = databaseToPause; // new ClusteredDatabase(databaseToPause);
			readyDatabases.remove(database);
			pausedDatabases.add(database);
		}
	}

	public synchronized ClusteredDatabase getPausedDatabase() throws NoAvailableDatabaseException {
		ClusteredDatabase template = getReadyDatabase();
		pauseDatabase(template);
		return template;
	}

	public ClusteredDatabase getReadyDatabase() throws NoAvailableDatabaseException {
		ClusteredDatabase[] dbs = getReadyDatabases();
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
			ClusteredDatabase randomElement = dbs[randNumber];
			return randomElement;
		}
		throw new NoAvailableDatabaseException();
//		return null;
	}

	public synchronized void addAll(ClusteredDatabase[] databases) {
		for (ClusteredDatabase database : databases) {
			add(database);
		}
	}

	public synchronized ClusteredDatabase getTemplateDatabase() throws NoAvailableDatabaseException {
//		if (allDatabases.isEmpty()) {
		if (allDatabases.size() < 2) {
			final DatabaseConnectionSettings authoritativeDCS = getAuthoritativeDatabaseConnectionSettings();
			if (authoritativeDCS != null) {
				try {
					return new ClusteredDatabase(authoritativeDCS.createDBDatabase());
				} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SQLException ex) {
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
			for (ClusteredDatabase db : allDatabases) {
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

	public boolean clusterContainsDatabase(ClusteredDatabase database) {
		if (database != null) {
			final DatabaseConnectionSettings newEncode = database.getSettings();
			for (ClusteredDatabase db : allDatabases) {
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

	public List<ClusteredDatabase> getQuarantinedDatabases() {
		return quarantinedDatabases
				.stream()
				.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
	}

	public synchronized void removeAllDatabases() {
		ClusteredDatabase[] dbs = allDatabases.toArray(new ClusteredDatabase[]{});
		for (ClusteredDatabase db : dbs) {
			removeDatabaseFromAllLists(db);
			db.stopClustering();
		}
	}

	public synchronized void dismantle() {
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
		allDatabases.forEach((db) -> db.setRequiredToProduceEmptyStringsForNull(!result));
	}

	public synchronized boolean getSupportsDifferenceBetweenNullAndEmptyString() {
		return supportsDifferenceBetweenNullAndEmptyString;
	}

	private void checkSupportForDifferenceBetweenNullAndEmptyString() {
		boolean supportsDifference = true;
		for (ClusteredDatabase database : getAllDatabases()) {
			supportsDifference = supportsDifference && database.getDefinition().supportsDifferenceBetweenNullAndEmptyStringNatively();
		}
		setSupportsDifferenceBetweenNullAndEmptyString(supportsDifference);
	}

	public void printAllFormerDatabases() {
		allAddedDatabases.forEach(db -> {
			System.out.println("DB: " + db);
		});
	}
}
