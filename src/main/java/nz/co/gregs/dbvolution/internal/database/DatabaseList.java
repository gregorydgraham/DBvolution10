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
import java.util.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;

/**
 *
 * @author gregorygraham
 */
public class DatabaseList implements Serializable {

	private static final long serialVersionUID = 1L;

	/* TODO combine these into one list of a data object */
	private final HashMap<String, DBDatabase> databaseMap = new HashMap<String, DBDatabase>();
	private final HashMap<String, DBDatabaseCluster.Status> statusMap = new HashMap<String, DBDatabaseCluster.Status>(0);
	private final HashMap<String, Integer> quarantineCountMap = new HashMap<String, Integer>(0);

//	private final Map<String, DBDatabase> databaseMap = Collections.synchronizedMap(new HashMap<String, DBDatabase>());
//	private final Map<String, DBDatabaseCluster.Status> statusMap = Collections.synchronizedMap(new HashMap<String, DBDatabaseCluster.Status>(0));
//	private final Map<String, Integer> quarantineCountMap = Collections.synchronizedMap(new HashMap<String, Integer>(0));

	public synchronized int size() {
		return databaseMap.size();
	}

	public synchronized boolean isEmpty() {
		return databaseMap.isEmpty();
	}

	public synchronized boolean contains(Object o) {
		if (o instanceof DBDatabase) {
			DBDatabase db = (DBDatabase) o;
			return databaseMap.containsKey(getKey(db));
		} else {
			return false;
		}
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

	public synchronized final boolean add(DBDatabase e) {
		databaseMap.put(getKey(e), e);
		statusMap.put(getKey(e), UNSYNCHRONISED);
		return true;
	}

	public synchronized boolean remove(DBDatabase e) {
		databaseMap.remove(getKey(e));
		statusMap.remove(getKey(e));
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

	public synchronized boolean addAll(int index, Collection<? extends DBDatabase> c) {
		return addAll(c);
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

	public DatabaseList() {
	}

	public DatabaseList(DBDatabase firstDB, DBDatabase... databases) {
		add(firstDB);
		for (var db : databases) {
			add(db);
		}
	}

	private synchronized void set(DBDatabase db, DBDatabaseCluster.Status status) {
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
		set(db, READY);
	}

	public synchronized void setUnsynchronised(DBDatabase db) {
		set(db, UNSYNCHRONISED);
	}

	public synchronized void setPaused(DBDatabase db) {
		set(db, PAUSED);
	}

	public synchronized void setDead(DBDatabase db) {
		set(db, DEAD);
	}

	public synchronized void setQuarantined(DBDatabase db) {
		set(db, QUARANTINED);
	}

	public synchronized void setUnknown(DBDatabase db) {
		set(db, UNKNOWN);
	}

	public synchronized void setProcessing(DBDatabase db) {
		set(db, PROCESSING);
	}

	public synchronized void setSynchronising(DBDatabase db) {
		set(db, SYNCHRONIZING);
	}

	public synchronized DBDatabase[] getDatabases() {
		return databaseMap.values().toArray(new DBDatabase[0]);
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase statusOfThisDatabase) {
		return statusMap.getOrDefault(getKey(statusOfThisDatabase), UNKNOWN);
	}

	public synchronized boolean isReady(DBDatabase database) {
		return statusMap.getOrDefault(getKey(database), UNKNOWN).equals(READY);
	}

	public synchronized DBDatabase[] getDatabases(DBDatabaseCluster.Status... statuses) {
		List<DBDatabase> found = new ArrayList<>(0);
		for (Map.Entry<String, DBDatabaseCluster.Status> entry : statusMap.entrySet()) {
			String key = entry.getKey();
			DBDatabaseCluster.Status val = entry.getValue();
			for (DBDatabaseCluster.Status status : statuses) {
				if (val.equals(status)) {
					DBDatabase db = databaseMap.get(key);
					found.add(db);
				}
			}
		}
		DBDatabase[] array = found.toArray(new DBDatabase[]{});
		return array;
	}

	public synchronized long countReadyDatabases() {
		return countDatabases(READY);
	}

	public synchronized long countPausedDatabases() {
		return statusMap.values().stream().filter(t -> t.equals(PAUSED)).count();
	}

	public synchronized long countDatabases(DBDatabaseCluster.Status... statuses) {
		return getDatabases(statuses).length;
	}

	public synchronized void clear() {
		statusMap.clear();
		databaseMap.clear();
	}

	public synchronized boolean areAllReady() {
		return countDatabases(DBDatabaseCluster.Status.READY) == databaseMap.size();
	}

	private synchronized void incrementQuarantineCount(DBDatabase db) {
		String key = getKey(db);
		Integer currentValue = quarantineCountMap.get(key);
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
		if(currentValue >= 3){
			setDead(db);
			return true;
		}
		return false;
	}
}
