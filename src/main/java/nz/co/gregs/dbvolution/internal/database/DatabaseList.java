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
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueue;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueueList;
import nz.co.gregs.dbvolution.utility.StopWatch;

/**
 *
 * @author gregorygraham
 */
public class DatabaseList implements Serializable {

	private static final long serialVersionUID = 1L;
	private final transient Object A_DATABASE_IS_READY = new Object();
	private final transient Object CLUSTER_HAS_SYNCHRONISED = new Object();

	private final HashMap<String, ClusterMember> members = new HashMap<>(2);
	private final ClusterDetails details;

	public DatabaseList(ClusterDetails clusterDetails) {
		this.details = clusterDetails;
	}

	public DatabaseList(ClusterDetails clusterDetails, DBDatabase firstDB, DBDatabase... databases) {
		this(clusterDetails);
		add(firstDB);
		for (var db : databases) {
			add(db);
		}
	}

	public synchronized int size() {
		return members.size();
	}

	public synchronized boolean isEmpty() {
		return members.isEmpty();
	}

	public synchronized boolean contains(DBDatabase db) {
		return members.containsKey(getKey(db));
	}

	public synchronized Iterator<ClusterMember> iterator() {
		return members
				.values()
				.stream()
				.collect(Collectors.toList())
				.iterator();
	}

	public synchronized ClusterMember[] toArray() {
		return members
				.values()
				.stream()
				.collect(Collectors.toList())
				.toArray(new ClusterMember[]{});
	}

	public synchronized final boolean add(DBDatabase db) {
		final ClusterMember clusterMember = addWithoutStart(db);
		clusterMember.start();
		return true;
	}

	public synchronized final ClusterMember addWithoutStart(DBDatabase db) {
		final ClusterMember clusterMember = new ClusterMember(details, this, db);
		members.put(getKey(db), clusterMember);
		return clusterMember;
	}

	public synchronized final boolean add(DBDatabase... dbs) {
		boolean result = true;
		ArrayList<ClusterMember> newMembers = new ArrayList<>(dbs.length);
		for (DBDatabase db : dbs) {
			newMembers.add(addWithoutStart(db));
		}
		for (ClusterMember newMember : newMembers) {
			newMember.start();
		}
		return result;
	}

	public synchronized boolean remove(DBDatabase db) {
		final String key = getKey(db);
		final ClusterMember member = members.get(key);
		if (member != null) {
			members.remove(key);
			member.stop();
		}
		return true;
	}

	public synchronized boolean containsAll(Collection<DBDatabase> c) {
		boolean allAreInTheMap = c
				.stream()
				.allMatch(t -> members.containsKey(getKey(t))
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

	private synchronized void set(DBDatabaseCluster.Status status, DBDatabase db) {
		ClusterMember member = getMember(db);
		if (member != null) {
			member.setStatus(status);
		}
	}

	public synchronized void setReady(DBDatabase db) {
		set(READY, db);
		notifyADatabaseIsReady();
	}

	public synchronized void setPaused(DBDatabase... dbs) {
		for (DBDatabase db : dbs) {
			set(PAUSED, db);
		}
	}

	public synchronized void setPaused(DBDatabase db) {
		set(PAUSED, db);
	}

	public synchronized void setUnpaused(DBDatabase db) {
		setProcessing(db);
	}

	public synchronized void setDead(DBDatabase db) {
		set(DEAD, db);
	}

	public synchronized void setQuarantined(DBDatabase db) {
		set(QUARANTINED, db);
	}

	public synchronized void setUnknown(DBDatabase db) {
		set(UNKNOWN, db);
	}

	public synchronized void setProcessing(DBDatabase db) {
		set(PROCESSING, db);
	}

	public synchronized DBDatabase[] getDatabases() {
		return members
				.values()
				.stream()
				.map((t) -> t.getDatabase())
				.collect(Collectors.toList())
				.toArray(new DBDatabase[]{});
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase statusOfThisDatabase) {
		ClusterMember member = members.getOrDefault(getKey(statusOfThisDatabase), null);
		if (member == null) {
			return UNKNOWN;
		} else {
			return member.getStatus();
		}
	}

	public synchronized boolean isReady(DBDatabase database) {
		ClusterMember member = members.getOrDefault(getKey(database), null);
		if (member == null) {
			return false;
		} else {
			return member.getStatus().equals(READY);
		}
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
		for (Map.Entry<String, ClusterMember> entry : members.entrySet()) {
			String key = entry.getKey();
			ClusterMember member = entry.getValue();
			DBDatabaseCluster.Status val = member.getStatus();
			for (DBDatabaseCluster.Status status : statuses) {
				if (val.equals(status)) {
					DBDatabase db = members.get(key).getDatabase();
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
		return members.values().stream().filter(t -> t.getStatus().equals(PAUSED)).count();
	}

	public synchronized int countDatabases(DBDatabaseCluster.Status... statuses) {
		return getDatabasesByStatus(statuses).length;
	}

	public synchronized void clear() {
		members.forEach((key, member) -> {
			member.stop();
		});
		members.clear();
	}

	public synchronized boolean areAllReady() {
		return countDatabases(DBDatabaseCluster.Status.READY) == members.size();
	}

	public ClusterMember getMember(DBDatabase db) {
		ClusterMember member = members.get(getKey(db));
		return member;
	}

	public List<ClusterMember> getMembers(DBDatabase... dbs) {
		ArrayList<ClusterMember> found = new ArrayList<>(0);
		for (DBDatabase db : dbs) {
			ClusterMember member = getMember(db);
			if (member != null) {
				found.add(member);
			}
		}
		return found;
	}

	public synchronized void clearQuarantineCount(DBDatabase db) {
		getMember(db).resetQuarantineCount();
	}

	public synchronized boolean isDead(DBDatabase db) {
		ClusterMember member = getMember(db);
		if (DEAD.equals(member.getStatus())) {
			return true;
		}
		Integer currentValue = member.getQuarantineCount();
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
		if (getReadyDatabases().length == members.size()) {
			return true;
		} else {
			try {
				waitOnClusterHasSynchronised();
				System.out.println("CLUSTER HAS SYNCHRONISED");
				members.values().stream().forEach((member) -> System.out.println("MEMBER " + member.getDatabase().getLabel() + " IS STATUS " + member.getStatus()));
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
			while (!getStatusOf(database).equals(READY) && timer.splitTime() < timeoutInMilliseconds) {
				waitUntilADatabaseIsReady();
			}
			return true;
		}
	}

	synchronized void queueAction(DBDatabase db, DBAction action) {
		if (getStatusOf(db).equals(READY)) {
			setProcessing(db);
		}
		getMember(db).getQueue().add(action);
	}

	void copyFromTo(DBDatabase template, DBDatabase secondary) {
		ActionQueue templateQ = getMember(template).getQueue();
		ActionQueue secondaryQ = getMember(secondary).getQueue();
		boolean templatePaused = templateQ.isPaused();
		boolean secondaryPaused = secondaryQ.isPaused();
		if (!templatePaused) {
			templateQ.pause();
		}
		if (!secondaryPaused) {
			secondaryQ.pause();
		}
		secondaryQ.clear();
		secondaryQ.addAll(templateQ);
		if (!secondaryPaused) {
			secondaryQ.unpause();
		}
		if (!templatePaused) {
			templateQ.unpause();
		}
//		actionQueues.copyFromTo(template, secondary);
	}

//	public ActionQueueList getActionQueueList() {
//		return this.actionQueues;
//	}
	private boolean waitUntilADatabaseIsReady() {
		synchronized (A_DATABASE_IS_READY) {
			try {
				A_DATABASE_IS_READY.wait();
				return true;

			} catch (InterruptedException ex) {
				Logger.getLogger(DatabaseList.class
						.getName()).log(Level.SEVERE, null, ex);
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
				Logger.getLogger(DatabaseList.class
						.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	public void notifyADatabaseIsReady() {
		synchronized (A_DATABASE_IS_READY) {
			A_DATABASE_IS_READY.notifyAll();
		}
		final int readyDatabases = getDatabasesByStatusAsList(READY).size();
		final int numberOfMembers = members.size();
		if (numberOfMembers == readyDatabases) {
			synchronized (CLUSTER_HAS_SYNCHRONISED) {
				CLUSTER_HAS_SYNCHRONISED.notifyAll();
			}
		}
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

	private void waitOnClusterHasSynchronised() throws InterruptedException {
		synchronized (CLUSTER_HAS_SYNCHRONISED) {
			CLUSTER_HAS_SYNCHRONISED.wait();
		}
	}

	public ActionQueueList getDatabaseList() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	ActionQueue[] getActionQueues(DBDatabase... dbs) {
		return getMembers(dbs)
				.stream()
				.map((m) -> m.getQueue())
				.collect(Collectors.toList())
				.toArray(new ActionQueue[]{});
	}
}
