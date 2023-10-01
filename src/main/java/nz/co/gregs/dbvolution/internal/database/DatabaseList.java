/*
 * Copyright 2021 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
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
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueueList;
import nz.co.gregs.dbvolution.internal.cluster.SynchronisationAction;
import nz.co.gregs.dbvolution.utility.StopWatch;

/**
 *
 * @author gregorygraham
 */
public class DatabaseList implements Serializable {

	private static final long serialVersionUID = 1L;

	/* TODO combine these into one list of a data object */
	private final ActionQueueList actionQueues;
	private final HashMap<String, DBDatabase> databaseMap = new HashMap<String, DBDatabase>();
	private final HashMap<String, DBDatabaseCluster.Status> statusMap = new HashMap<String, DBDatabaseCluster.Status>(0);
	private final HashMap<String, Integer> quarantineCountMap = new HashMap<String, Integer>(0);
	private final ClusterDetails details;
	private final transient Object A_DATABASE_IS_READY = new Object();
	private final transient Object ALL_DATABASES_ARE_READY = new Object();

	public synchronized int size() {
		return databaseMap.size();
	}

	public synchronized boolean isEmpty() {
		return databaseMap.isEmpty();
	}

	public synchronized boolean contains(DBDatabase db) {
		return databaseMap.containsKey(getKey(db));
	}

	public synchronized Iterator<DBDatabase> iterator() {
		return databaseMap.values().iterator();
	}

	public synchronized DBDatabase[] toArray() {
		return toArray(new DBDatabase[]{});
	}

	public synchronized DBDatabase[] toArray(DBDatabase[] a) {
		return databaseMap.values().toArray(a);
	}

	public synchronized final boolean add(DBDatabase db) {
		final String key = getKey(db);
		quarantineCountMap.put(key, 0);
		addUnsynchronisedDatabase(key, db);
		return true;
	}

	private void addUnsynchronisedDatabase(final String key, DBDatabase db) {
		statusMap.put(key, PROCESSING);
		databaseMap.put(key, db);
		actionQueues.add(db);
		actionQueues.queueAction(db, new SynchronisationAction(details, db));
	}

	public synchronized final boolean add(DBDatabase... dbs) {
		boolean result = true;
		for (DBDatabase db : dbs) {
			result = result && add(db);
		}
		return result;
	}

	public synchronized boolean remove(DBDatabase db) {
		actionQueues.remove(db);
		databaseMap.remove(getKey(db));
		statusMap.remove(getKey(db));
		return true;
	}

	public synchronized boolean containsAll(Collection<DBDatabase> c) {
		boolean allAreInTheMap = c
				.stream()
				.allMatch(t -> databaseMap.containsKey(getKey(t))
				);
		return allAreInTheMap;
	}

	public synchronized boolean addAll(Collection<? extends DBDatabase> collectionOfDatabases) {
		for (DBDatabase dBDatabase : collectionOfDatabases) {
			this.add(dBDatabase);
		}
		return true;
	}

	public synchronized boolean removeAll(Collection<DBDatabase> collectionOfDatabases) {
		for (DBDatabase db : collectionOfDatabases) {
			remove(db);
		}
		return true;
	}

	private synchronized String getKey(DBDatabase db) {
		return db.getSettings().encode();
	}

	public DatabaseList(ClusterDetails clusterDetails) {
		this.details = clusterDetails;
		this.actionQueues = new ActionQueueList(this);
	}

	public DatabaseList(ClusterDetails clusterDetails, DBDatabase firstDB, DBDatabase... databases) {
		this(clusterDetails);
		add(firstDB);
		for (var db : databases) {
			add(db);
		}
	}

	private synchronized void set(DBDatabaseCluster.Status status, DBDatabase db) {
		if (statusMap.containsKey(getKey(db))) {
			statusMap.put(getKey(db), status);
			if (QUARANTINED.equals(status)) {
				incrementQuarantineCount(db);
			}
			if (READY.equals(status)) {
				clearQuarantineCount(db);
			}
		}
	}

	public synchronized void setReady(DBDatabase db) {
		set(READY, db);
		notifyADatabaseIsReady();
	}

	public synchronized void setUnsynchronised(DBDatabase db) {
		actionQueues.remove(db);
		addUnsynchronisedDatabase(getKey(db), db);
	}

	public synchronized void setPaused(DBDatabase... dbs) {
		for (DBDatabase db : dbs) {
			setPaused(db);
		}
	}

	public synchronized void setPaused(DBDatabase db) {
		set(PAUSED, db);
		actionQueues.pause(db);
	}

	public synchronized void setUnpaused(DBDatabase db) {
		setProcessing(db);
	}

	public synchronized void setDead(DBDatabase db) {
		set(DEAD, db);
		actionQueues.remove(db);
	}

	public synchronized void setQuarantined(DBDatabase db) {
		set(QUARANTINED, db);
		actionQueues.remove(db);
	}

	public synchronized void setUnknown(DBDatabase db) {
		set(UNKNOWN, db);
	}

	public synchronized void setProcessing(DBDatabase db) {
		set(PROCESSING, db);
		actionQueues.unpause(db);
	}

//	public synchronized void setSynchronising(DBDatabase db) {
//		set(SYNCHRONIZING, db);
//	}
	public synchronized DBDatabase[] getDatabases() {
		return databaseMap.values().toArray(new DBDatabase[]{});
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase statusOfThisDatabase) {
		return statusMap.getOrDefault(getKey(statusOfThisDatabase), UNKNOWN);
	}

	public synchronized boolean isReady(DBDatabase database) {
		return statusMap.getOrDefault(getKey(database), UNKNOWN).equals(READY);
	}

	public DBDatabase[] getReadyDatabases() {
		return getDatabasesByStatus(READY);
	}

	public synchronized DBDatabase[] getDatabasesByStatus(DBDatabaseCluster.Status... statuses) {
		final List<DBDatabase> list = getDatabasesByStatusAsList(statuses);
		if (list == null || list.size() == 0) {
			return new DBDatabase[]{};
		} else {
			return list.toArray(new DBDatabase[0]);
		}
	}

	public List<DBDatabase> getReadyDatabasesList() {
		return getDatabasesByStatusAsList(READY);
	}

	public synchronized List<DBDatabase> getDatabasesByStatusAsList(DBDatabaseCluster.Status... statuses) {
		List<DBDatabase> found = new ArrayList<>(0);
		for (Map.Entry<String, DBDatabaseCluster.Status> entry : statusMap.entrySet()) {
			String key = entry.getKey();
			DBDatabaseCluster.Status val = entry.getValue();
			for (DBDatabaseCluster.Status status : statuses) {
				if (val.equals(status)) {
					DBDatabase db = databaseMap.get(key);
					found.add(db);
					break;
				}
			}
		}
		return found;
	}

	public synchronized int countReadyDatabases() {
		return countDatabases(READY);
	}

	public synchronized long countPausedDatabases() {
		return statusMap.values().stream().filter(t -> t.equals(PAUSED)).count();
	}

	public synchronized int countDatabases(DBDatabaseCluster.Status... statuses) {
		return getDatabasesByStatus(statuses).length;
	}

	public synchronized void clear() {
		actionQueues.clear();
		statusMap.clear();
		databaseMap.clear();
	}

	public synchronized boolean areAllReady() {
		return countDatabases(DBDatabaseCluster.Status.READY) == databaseMap.size();
	}

	private synchronized void incrementQuarantineCount(DBDatabase db) {
		String key = getKey(db);
		Integer currentValue = quarantineCountMap.get(key);
		if (currentValue == null) {
			currentValue = 0;
		}
		quarantineCountMap.put(key, currentValue + 1);
	}

	private synchronized void clearQuarantineCount(DBDatabase db) {
		String key = getKey(db);
		quarantineCountMap.put(key, 0);
	}

	public synchronized boolean isDead(DBDatabase db) {
		if (DEAD.equals(getStatusOf(db))) {
			return true;
		}
		String key = getKey(db);
		Integer currentValue = quarantineCountMap.get(key);
		if (currentValue == null) {
			currentValue = 0;
		}
		if (currentValue >= 3) {
			setDead(db);
			return true;
		}
		return false;
	}

	public synchronized DBDatabase getReadyDatabase() {
		List<DBDatabase> readyDatabases = getReadyDatabasesList();
		if (readyDatabases.size() == 0) {
			throw new NoAvailableDatabaseException();
		}
		if (readyDatabases.size() == 1) {
			return readyDatabases.get(0);
		}
		DBDatabase ready;
		try {
			ready = readyDatabases.get(new Random().nextInt(readyDatabases.size()));
		} catch (Exception e) {
			ready = null;
		}

		return ready;
	}

	public synchronized DBDatabase getReadyDatabase(int millisecondsToWait) {
		List<DBDatabase> readyDatabases = getReadyDatabasesList();
		if (readyDatabases.size() == 0) {
			waitUntilADatabaseIsReady(millisecondsToWait);
			readyDatabases = getReadyDatabasesList();
		}
		if (readyDatabases.size() == 0) {
			throw new NoAvailableDatabaseException();
		}
		if (readyDatabases.size() == 1) {
			return readyDatabases.get(0);
		}
		DBDatabase ready = readyDatabases.get(new Random().nextInt(readyDatabases.size()));
		return ready;
	}

	boolean waitUntilSynchronised() {
		if (getReadyDatabases().length == this.databaseMap.size()) {
			return true;
		} else {
			try {
				waitOnAllDatabasesAreReady();
				return true;
			} catch (InterruptedException ex) {
				Logger.getLogger(DatabaseList.class.getName()).log(Level.SEVERE, null, ex);
			}
			return false;
		}
	}

	boolean waitUntilDatabaseHasSynchonized(DBDatabase database) {
		if (getStatusOf(database).equals(READY)) {
			return true;
		} else {
			while (!getStatusOf(database).equals(READY)) {
				waitUntilADatabaseIsReady();
			}
			return true;
		}
	}

	boolean waitUntilDatabaseHasSynchronized(DBDatabase database, long timeoutInMilliseconds) {
		StopWatch timer = StopWatch.start();
		if (getStatusOf(database).equals(READY)) {
			return true;
		} else {
			while (!getStatusOf(database).equals(READY) && timer.splitTime()<timeoutInMilliseconds) {
				waitUntilADatabaseIsReady();
			}
			return true;
		}
	}

	synchronized void queueAction(DBDatabase db, DBAction action) {
		if (getStatusOf(db).equals(READY)) {
			setProcessing(db);
		}
		actionQueues.queueAction(db, action);
	}

	void copyFromTo(DBDatabase template, DBDatabase secondary) {
		actionQueues.copyFromTo(template, secondary);
	}

	public ActionQueueList getActionQueueList() {
		return this.actionQueues;
	}

	private boolean waitUntilADatabaseIsReady() {
		synchronized (A_DATABASE_IS_READY) {
			try {
				A_DATABASE_IS_READY.wait();
				return true;
			} catch (InterruptedException ex) {
				Logger.getLogger(DatabaseList.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	private boolean waitUntilADatabaseIsReady(long millisecondsToWait) {
		synchronized (A_DATABASE_IS_READY) {
			try {
				A_DATABASE_IS_READY.wait(millisecondsToWait);
				return true;
			} catch (InterruptedException ex) {
				Logger.getLogger(DatabaseList.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	public void notifyADatabaseIsReady() {
		synchronized (A_DATABASE_IS_READY) {
			A_DATABASE_IS_READY.notifyAll();
		}
		notifyAllDatabasesAreReadyIfNecessary();
	}

	public synchronized void quarantineDatabase(DBDatabase database, SQLException ex) {
		this.setQuarantined(database);
		database.setLastException(ex);
	}

	public void handleEmptyQueue(DBDatabase database) {
		if (getStatusOf(database).equals(PROCESSING)) {
			setReady(database);
		}
	}

	private void waitOnAllDatabasesAreReady() throws InterruptedException {
		synchronized (ALL_DATABASES_ARE_READY) {
			ALL_DATABASES_ARE_READY.wait();
		}
	}

	private void notifyAllDatabasesAreReadyIfNecessary() {
		if (databaseMap.size() == getReadyDatabases().length) {
			synchronized (ALL_DATABASES_ARE_READY) {
				ALL_DATABASES_ARE_READY.notifyAll();
			}
		}
	}
}
