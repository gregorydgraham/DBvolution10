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

import java.util.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;

/**
 *
 * @author gregorygraham
 */
public class DatabaseList {

	private final Map<String, DBDatabase> databaseMap = Collections.synchronizedMap(new HashMap<String, DBDatabase>());
	private final Map<String, DBDatabaseCluster.Status> statusMap = Collections.synchronizedMap(new HashMap<String, DBDatabaseCluster.Status>(0));

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

	public boolean containsAll(Collection<DBDatabase> c) {
		boolean allAreInTheMap = c
				.stream()
				.allMatch(t -> databaseMap.containsKey(getKey(t))
				);
		return allAreInTheMap;
	}

	public boolean addAll(Collection<? extends DBDatabase> collectionOfDatabases) {
		for (DBDatabase dBDatabase : collectionOfDatabases) {
			this.add(dBDatabase);
		}
		return true;
	}

	public boolean addAll(int index, Collection<? extends DBDatabase> c) {
		return addAll(c);
	}

	public boolean removeAll(Collection<DBDatabase> collectionOfDatabases) {
		for (DBDatabase db : collectionOfDatabases) {
			remove(db);
		}
		return true;
	}

	private String getKey(DBDatabase db) {
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
		}
	}

	public void setReady(DBDatabase db) {
		set(db, READY);
	}

	public void setUnsynchronised(DBDatabase db) {
		set(db, UNSYNCHRONISED);
	}

	public void setPaused(DBDatabase db) {
		set(db, PAUSED);
	}

	public void setDead(DBDatabase db) {
		set(db, DEAD);
	}

	public void setQuarantined(DBDatabase db) {
		set(db, QUARANTINED);
	}

	public void setUnknown(DBDatabase db) {
		set(db, UNKNOWN);
	}

	public void setProcessing(DBDatabase db) {
		set(db, PROCESSING);
	}

	public void setSynchronising(DBDatabase db) {
		set(db, SYNCHRONIZING);
	}

	public synchronized DBDatabase[] getDatabases() {
		return databaseMap.values().toArray(new DBDatabase[]{});
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase statusOfThisDatabase) {
		return statusMap.getOrDefault(getKey(statusOfThisDatabase), UNKNOWN);
	}

	public synchronized boolean isReady(DBDatabase database) {
		return statusMap.getOrDefault(getKey(database), UNKNOWN).equals(READY);
	}

	public synchronized DBDatabase[] getDatabases(DBDatabaseCluster.Status status) {
		List<DBDatabase> found = new ArrayList<>(0);
		for (Map.Entry<String, DBDatabaseCluster.Status> entry : statusMap.entrySet()) {
			String key = entry.getKey();
			DBDatabaseCluster.Status val = entry.getValue();
			if (val.equals(status)) {
				DBDatabase db = databaseMap.get(key);
				found.add(db);
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

	public synchronized long countDatabases(DBDatabaseCluster.Status status) {
		return statusMap.values().stream().filter(t -> t.equals(status)).count();
	}

	public synchronized void clear() {
		statusMap.clear();
		databaseMap.clear();
	}

	public synchronized boolean areAllSynchronised() {
		return countDatabases(DBDatabaseCluster.Status.SYNCHRONIZING) == 0
				&& countDatabases(DBDatabaseCluster.Status.UNSYNCHRONISED) == 0;
	}
}
