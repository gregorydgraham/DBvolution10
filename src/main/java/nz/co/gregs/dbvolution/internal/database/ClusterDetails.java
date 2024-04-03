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

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import nz.co.gregs.dbvolution.utility.TableSet;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Configuration;
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

	private final ClusterMemberList members;

	private transient final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
	private transient final Set<DBRow> trackedTables = Collections.synchronizedSet(new HashSet<DBRow>());

	private transient final PreferencesImproved prefs = PreferencesImproved.userNodeForPackage(this.getClass());
	private String clusterLabel = "NotDefined";
	private boolean supportsDifferenceBetweenNullAndEmptyString = true;
	private boolean quietExceptions = false;
	private DBDatabaseCluster.Configuration configuration = DBDatabaseCluster.Configuration.fullyManual();

	private DatabaseConnectionSettings clusterSettings;
	private DBDatabase preferredDatabase;

	private boolean preferredDatabaseRequired;
	private boolean stillRunning = true;
	private final PropertyChangeSupport propertyChangeSupport;

	public ClusterDetails(String label) {
		this.clusterLabel = label;
		members = new ClusterMemberList(this);
		propertyChangeSupport = new PropertyChangeSupport(this);
	}

	public void addPropertyChangeListener(PropertyChangeListener pcl) {
		propertyChangeSupport.addPropertyChangeListener(pcl);
	}

	public void removePropertyChangeListener(PropertyChangeListener pcl) {
		propertyChangeSupport.removePropertyChangeListener(pcl);
	}

	public final synchronized boolean add(DBDatabase databaseToAdd) {
		if (databaseToAdd != null) {
			propertyChangeSupport.firePropertyChange("introducing new member", null, databaseToAdd);
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
			// make a Action Queue to buffer asynchronous actions

			if (clusterContains(database)) {
				members.remove(database);
				addSynchronised(database);
				return false;
			} else {
				addSynchronised(database);
				saveClusterSettingsToPrefs();
				propertyChangeSupport.firePropertyChange("added new member", null, databaseToAdd);
				return true;
			}
		}
		return false;
	}

	private void addSynchronised(DBDatabase database) {
		// make sure that we synchronise the first database before moving on
		if (members.size() == 0) {
			members.add(database);
			members.waitUntilDatabaseHasSynchonized(database);
		} else {
			members.add(database);
		}
	}

	public DBDatabase[] getAllDatabases() {
		return members.getDatabases();
	}

	public synchronized void quarantineDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		if (clusterContains(database)) {
			if (hasTooFewReadyDatabases() && members.isReady(database)) {
				// Unable to quarantine the only remaining READY database
				propertyChangeSupport.firePropertyChange("failed to quarantine member", null, database);
				throw new UnableToRemoveLastDatabaseFromClusterException();
			}

			if (quietExceptions) {
			} else {
				LOG.log(Level.WARNING, "QUARANTINING: DATABASE LABEL {0}", database.getLabel());
				LOG.log(Level.WARNING, "QUARANTINE INFO: JDBCURL {0}", database.getJdbcURL());
				Throwable e = except;
				while (e != null) {
					LOG.log(Level.WARNING, "QUARANTINE INFO: EXCEPTION {0}", except.getClass().getCanonicalName());
					LOG.log(Level.WARNING, "QUARANTINE INFO: MESSAGE {0}", except.getMessage());
					LOG.log(Level.WARNING, "QUARANTINE INFO: LOCALIZED {0}", except.getLocalizedMessage());
					e = e.getCause();
				}
			}
			database.setLastException(except);
			members.setQuarantined(database);
			propertyChangeSupport.firePropertyChange("quarantined member", null, database);
			setAuthoritativeDatabase();
			if (database instanceof DBDatabaseCluster) {
				DBDatabaseCluster cluster = (DBDatabaseCluster) database;
				cluster.setHasQuarantined(true);
			}
		}
	}

	public synchronized void deadDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
		if (clusterContains(database)) {
			if (hasTooFewReadyDatabases() && members.isReady(database)) {
				// Unable to quarantine the only remaining database
				propertyChangeSupport.firePropertyChange("last member can not die", null, database);
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
			propertyChangeSupport.firePropertyChange("member has died", null, database);
			setAuthoritativeDatabase();
		}
	}

	public synchronized boolean removeDatabase(DBDatabase databaseToRemove) {
		DBDatabase database = databaseToRemove;
		if (hasTooFewReadyDatabases() && members.isReady(database)) {
			propertyChangeSupport.firePropertyChange("unable to remove last member", null, database);
			throw new UnableToRemoveLastDatabaseFromClusterException();
		} else {
			members.remove(database);
			propertyChangeSupport.firePropertyChange("removed database", null, database);
			setAuthoritativeDatabase();
			saveClusterSettingsToPrefs();
			checkSupportForDifferenceBetweenNullAndEmptyString();
			return true;
		}
	}

	protected boolean hasTooFewReadyDatabases() {
		return members.countReadyDatabases() < 2;
	}

	public synchronized DBRow[] getRequiredAndTrackedTables() {
		var tables = new TableSet();

		tables.addAll(requiredTables);
		tables.addAll(trackedTables);
		return tables.toArray(new DBRow[]{});
	}

	public void setTrackedTables(Collection<DBRow> rows) {
		ArrayList<DBRow> oldValue = new ArrayList<>(trackedTables);
		trackedTables.clear();
		propertyChangeSupport.firePropertyChange("cleared tracked tables", oldValue, trackedTables);
		for (DBRow row : rows) {
			addTrackedTable(row, false);
		}
		saveTrackedTables();
	}

	public void addTrackedTable(DBRow row) {
		addTrackedTable(row, true);
	}

	private void addTrackedTable(DBRow row, boolean saveTablesAutomatically) {
		synchronized (trackedTables) {
			trackedTables.add(DBRow.getDBRow(row.getClass()));
			propertyChangeSupport.firePropertyChange("added tracked table", null, row);
		}
		if (saveTablesAutomatically) {
			saveTrackedTables();
		}
	}

	public void addTrackedTables(Collection<DBRow> rows) {
		for (DBRow row : rows) {
			addTrackedTable(row, false);
		}
		saveTrackedTables();
	}

	public void removeTrackedTable(DBRow row) {
		removeTrackedTable(row, true);
	}

	private void removeTrackedTable(DBRow row, boolean andSave) {
		synchronized (trackedTables) {
			trackedTables.remove(row);
			propertyChangeSupport.firePropertyChange("removed tracked table", null, row);
		}
		if (andSave) {
			saveTrackedTables();
		}
	}

	public void removeTrackedTables(Collection<DBRow> rows) {
		for (DBRow row : rows) {
			removeTrackedTable(row, false);
		}
		saveTrackedTables();
	}

	protected boolean hasReadyDatabases() {
		return members.countReadyDatabases() > 0;
	}

	public synchronized DBDatabase[] getReadyDatabases() {
		return members.getDatabasesByStatus(DBDatabaseCluster.Status.READY);
	}

	public synchronized DBDatabase getPausedDatabase() throws NoAvailableDatabaseException {
		DBDatabase template = members.getReadyDatabase();
		setPausedDatabase(template);
		return template;
	}

	private synchronized DBDatabase setPausedDatabase(DBDatabase db) throws NoAvailableDatabaseException {
		members.setPaused(db);
		return db;
	}

	public DBDatabase getReadyDatabase() throws NoAvailableDatabaseException {
		if (hasPreferredDatabase() && preferredDatabaseIsReady()) {
			return preferredDatabase;
		} else if (hasPreferredDatabase() && preferredDatabaseRequired) {
			waitUntilDatabaseHasSynchronized(preferredDatabase);
			return preferredDatabase;
		} else {
			return members.getReadyDatabase();
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

	protected synchronized DBDatabase getAuthoritativeDatabase() throws NoAvailableDatabaseException {
		final DatabaseConnectionSettings authoritativeDCS = getAuthoritativeDatabaseConnectionSettings();
		if (authoritativeDCS != null) {
			try {
				return authoritativeDCS.createDBDatabase();
			} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
		throw new NoAvailableDatabaseException();
	}

	private synchronized void removedTrackedTablesFromPrefs() {
		prefs.remove(getTrackedTablesPrefsIdentifier());
	}

	private synchronized void saveTrackedTables() {
		if (configuration.isUseAutoRebuild()) {
			Set<Class<?>> previousClasses = new HashSet<>(0);
			SeparatedString rowClasses = getTrackedTablesSeparatedStringTemplate();
			for (DBRow trackedTable : trackedTables) {
				if (!previousClasses.contains(trackedTable.getClass())) {
					previousClasses.add(trackedTable.getClass());
					rowClasses.add(trackedTable.getClass().getName());
				}
			}
			String encodedTablenames = rowClasses.encode();
			try {
				final String encryptedText = Encryption_Internal.encrypt(encodedTablenames);
				final String name = getTrackedTablesPrefsIdentifier();
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
		Set<Class<DBRow>> previousClasses = new HashSet<>(0);
		if (configuration.isUseAutoRebuild()) {
			List<String> savedTrackedTables = getSavedTrackedTables();
			for (String savedTrackedTable : savedTrackedTables) {
				try {
					@SuppressWarnings("unchecked")
					Class<DBRow> trackedTableClass = (Class<DBRow>) Class.forName(savedTrackedTable);
					if (!previousClasses.contains(trackedTableClass)) {
						previousClasses.add(trackedTableClass);
						DBRow dbRow = DBRow.getDBRow(trackedTableClass);
						trackedTables.add(dbRow);
					}
				} catch (ClassNotFoundException ex) {
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
			for (DBDatabase db : members.getDatabasesByStatus(DBDatabaseCluster.Status.READY)) {
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
		return members.getDatabasesByStatus(DBDatabaseCluster.Status.QUARANTINED);
	}

	public synchronized void removeAllDatabases() throws SQLException {
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
			}
		}
		return databases;
	}

	public boolean waitUntilSynchronised() {
		return members.waitUntilSynchronised();
	}

	public boolean waitUntilSynchronised(int timeout) {
		return members.waitUntilSynchronised(timeout);
	}

	public boolean waitUntilDatabaseHasSynchronized(DBDatabase db) {
		return members.waitUntilDatabaseHasSynchonized(db);
	}

	public boolean waitUntilDatabaseHasSynchronized(DBDatabase database, long timeoutInMilliseconds) {
		return members.waitUntilDatabaseHasSynchronized(database, timeoutInMilliseconds);
	}

	public synchronized void addTemplateActionQueueToSecondary(DBDatabase template, DBDatabase secondary) {
		members.copyFromTo(template, secondary);
	}

	public void quarantineDatabaseAutomatically(DBDatabase suspectDatabase, Throwable sqlException) {
		try {
			quarantineDatabase(suspectDatabase, sqlException);
		} catch (UnableToRemoveLastDatabaseFromClusterException doesntNeedToBeHandledAsItsAutomaticAndNotManual) {
			;
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
		return members.getDatabasesByStatus(DBDatabaseCluster.Status.QUARANTINED, DBDatabaseCluster.Status.DEAD);
	}

	public void shutdown() {
		this.stillRunning = false;
		members.clear();
	}

	public boolean isShuttingDown() {
		return !stillRunning;
	}

	public void queueAction(DBDatabase nextDatabase, DBAction action) {
		members.queueAction(nextDatabase, action);
	}

	public void queueAction(DBDatabase target, DBActionList actions) {
		for (DBAction action : actions) {
			if (!action.hasSucceeded()) {
				members.queueAction(target, action);
			}
		}
	}

	public synchronized void executeOnAllDatabasesExcluding(DBAction action, DBDatabase... excludedDatabases) {
		List<DBDatabase> excludedDBs = Arrays.asList(excludedDatabases);
		for (DBDatabase nextDatabase : getAllDatabases()) {
			if (excludedDBs.contains(nextDatabase)) {
				// skip this database as it's already been actioned
			} else {
				queueAction(nextDatabase, action);
			}
		}
	}

	public DBDatabase[] getDatabasesByStatus(DBDatabaseCluster.Status value) {
		return members.getDatabasesByStatus(value);
	}

	public ClusterMemberList getMembers() {
		return members;
	}

	public boolean waitOnStatusChange(DBDatabaseCluster.Status status, int timeout, DBDatabase... affectedMembers) {
		return getMembers().waitOnStatusChange(status, timeout, affectedMembers);
	}

	public DBDatabase waitOnStatusChange(DBDatabase affectedMember, int timeout, DBDatabaseCluster.Status... appropriateStatuses) {
		return getMembers().waitOnStatusChange(affectedMember, timeout, appropriateStatuses);
	}

	public StatusSnapshot getClusterStatusSnapshot() {
		return new StatusSnapshot(this);
	}

	Configuration getConfiguration() {
		return configuration;
	}

	public boolean isSynchronised(DBDatabase database) {
		return members.isSynchronised(database);
	}

	public void printClusterStatuses() {
		getClusterStatusSnapshot().getMembers().stream().forEachOrdered((m)->System.out.println("STATUS: "+m.database.getLabel()+"|"+m.status.name()));
	}

	public static class StatusSnapshot {

		private final Instant snapshotTime;
		private final List<MemberSnapshot> members;
		private final String label;

		public StatusSnapshot(ClusterDetails dets) {
			snapshotTime = Instant.now();
			label = dets.getClusterLabel();
			ArrayList<MemberSnapshot> list = new ArrayList<>();
			for (ClusterMember memb : dets.getMembers().getMembers()) {
				list.add(new MemberSnapshot(memb));
			}
			members = Collections.unmodifiableList(list);
		}

		public Instant getSnapshotTime() {
			return snapshotTime;
		}

		public List<MemberSnapshot> getMembers() {
			return members;
		}

		public List<MemberSnapshot> getByDatabase(DBDatabase... dbs) {
			List<DBDatabase> asList = Arrays.asList(dbs);
			return StatusSnapshot.this.getByDatabase(asList);
		}

		public List<MemberSnapshot> getByDatabase(List<DBDatabase> dbs) {
			List<MemberSnapshot> collected = members.stream().filter(memb -> dbs.contains(memb.database)).collect(Collectors.toList());
			return collected;
		}

		public List<MemberSnapshot> getByStatus(DBDatabaseCluster.Status... statuses) {
			List<DBDatabaseCluster.Status> asList = Arrays.asList(statuses);
			return getByStatus(asList);
		}

		public List<MemberSnapshot> getByStatus(List<DBDatabaseCluster.Status> statuses) {
			List<MemberSnapshot> collected = members.stream().filter(memb -> statuses.contains(memb.status)).collect(Collectors.toList());
			return collected;
		}
		
		public void print(){
			System.out.println("-------"+label+"-------");
			members.stream().forEachOrdered((m)->System.out.println("STATUS: "+m.database.getLabel()+"|"+m.status.name()));
		}
	}

	public static class MemberSnapshot {

		public final DBDatabase database;
//		public final ActionQueue queue;
		public final DBDatabaseCluster.Status status;
		public final Integer quarantineCount;
		public final String memberId;

		public MemberSnapshot(ClusterMember memb) {
			this.database = memb.getDatabase();
//			this.queue = memb.getQueue();
			this.status = memb.getStatus();
			this.quarantineCount = memb.getQuarantineCount();
			this.memberId = memb.getMemberId();
		}
	}
}
