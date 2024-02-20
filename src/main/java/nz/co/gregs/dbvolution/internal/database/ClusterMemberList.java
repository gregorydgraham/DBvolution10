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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.OnlyOneDatabaseInClusterException;
import nz.co.gregs.dbvolution.utility.Random;
import nz.co.gregs.looper.LoopVariable;
import nz.co.gregs.looper.StopWatch;

/**
 *
 * @author gregorygraham
 */
public class ClusterMemberList implements Serializable, AutoCloseable {

	private static final long serialVersionUID = 1L;
	private final transient Object A_DATABASE_IS_READY = new Object();
	private final transient Object CLUSTER_HAS_SYNCHRONISED = new Object();
	private final transient Object A_STATUS_HAS_CHANGED = new Object();

	private final HashMap<String, ClusterMember> members = new HashMap<>(2);
	private final ClusterDetails details;

	public ClusterMemberList(ClusterDetails clusterDetails) {
		this.details = clusterDetails;
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
		final ClusterMember clusterMember = new ClusterMember(details, details.getClusterLabel(), this, db);
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
		ClusterMember member = members.remove(key);
		if (member != null) {
			member.stop();
		} else {
			System.out.println("CLUSTER ERROR - DATABASE NOT REMOVED BECAUSE KEY NOT FOUND: " + key);
		}
		return member != null;
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
			setPaused(db);
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

	public DBDatabase[] getDatabasesByStatus(DBDatabaseCluster.Status... statuses) {
		final List<DBDatabase> list = getDatabasesByStatusAsList(statuses);
		if (list == null || list.size() == 0) {
			return new DBDatabase[0];
		} else {
			return list.toArray(new DBDatabase[0]);
		}
	}

	public List<DBDatabase> getReadyDatabasesList() {
		return getDatabasesByStatusAsList(READY);
	}

	public List<DBDatabase> getDatabasesByStatusAsList(DBDatabaseCluster.Status... statuses) {
		List<DBDatabase> found = new ArrayList<>(0);
		for (Map.Entry<String, ClusterMember> entry : members.entrySet()) {
			String key = entry.getKey();
			if (key != null) {
				ClusterMember member = entry.getValue();
				DBDatabaseCluster.Status val = member.getStatus();
				if(val.oneOf(statuses)){
						DBDatabase db = members.get(key).getDatabase();
						found.add(db);
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
		return countReadyDatabases() == members.size();
	}

	public ClusterMember getMember(DBDatabase db) {
		ClusterMember member = members.get(getKey(db));
		return member;
	}

	public List<ClusterMember> getMembers() {
		ArrayList<ClusterMember> found = new ArrayList<>(2);
		for (ClusterMember value : members.values()) {
			found.add(value);
		}
		return found;
	}

	public List<ClusterMember> getMembers(DBDatabase... dbs) {
		ArrayList<ClusterMember> found = new ArrayList<>(2);
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
		return DEAD.equals(member.getStatus());
	}

	/**
	 *
	 *
	 *
	 * @return a random ready database
	 */
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
			ready = Random.get(readyDatabases);
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
		DBDatabase ready = Random.get(readyDatabases);
		return ready;
	}

	public boolean waitUntilSynchronised() {
		if (isSynchronised()) {
			return true;
		} else {
			try {
				for (ClusterMember member : members.values()) {
					System.out.println("MEMBER " + member.getDatabase().getLabel() + " IS STATUS " + member.getStatus());
				}
				waitOnClusterHasSynchronised();
				System.out.println("AFTER WAIT");
				members.values().stream().forEach((member) -> System.out.println("MEMBER " + member.getDatabase().getLabel() + " IS STATUS " + member.getStatus()));
				if (isSynchronised()) {
					System.out.println("CLUSTER HAS SYNCHRONISED");
					return true;
				} else {
					System.out.println("CLUSTER HAS TIMED OUT");
					return false;
				}
			} catch (InterruptedException ex) {
				Logger.getLogger(ClusterMemberList.class.getName()).log(Level.SEVERE, null, ex);
			}
			return false;
		}
	}

	public boolean waitUntilSynchronised(int timeout) {
		if (isSynchronised()) {
			return true;
		} else {
			try {
				members.values().stream().forEach((member) -> System.out.println("MEMBER " + member.getDatabase().getLabel() + " IS STATUS " + member.getStatus()));
				waitOnClusterHasSynchronised(timeout);
				System.out.println("AFTER WAIT");
				members.values().stream().forEach((member) -> System.out.println("MEMBER " + member.getDatabase().getLabel() + " IS STATUS " + member.getStatus()));
				if (isSynchronised()) {
					System.out.println("CLUSTER HAS SYNCHRONISED");
					return true;
				} else {
					System.out.println("CLUSTER HAS TIMED OUT");
					return false;
				}
			} catch (InterruptedException ex) {
				Logger.getLogger(ClusterMemberList.class.getName()).log(Level.SEVERE, null, ex);
			}
			return false;
		}
	}

	private boolean isSynchronised() {
		return getReadyDatabases().length == members.size();
	}

	public boolean isSynchronised(DBDatabase database) {
		return getStatusOf(database).equals(READY);
	}

	boolean waitUntilDatabaseHasSynchonized(DBDatabase database) {
		if (contains(database)) {
			if (isSynchronised(database)) {
				return true;
			} else {
				while (!isSynchronised(database)) {
					waitUntilADatabaseIsReady();
				}
				return true;
			}
		}
		return false;
	}

	public boolean waitUntilDatabaseHasSynchronized(DBDatabase database, long timeoutInMilliseconds) {
		StopWatch timer = StopWatch.start();
		if (isSynchronised(database)) {
			return true;
		} else {
			while (timer.splitTime() < timeoutInMilliseconds && !isSynchronised(database)) {
				waitUntilADatabaseIsReady(timeoutInMilliseconds);
			}
			timer.stop();
			return isSynchronised(database);
		}
	}

	public synchronized void queueAction(DBDatabase db, DBAction action) {
		final ClusterMember member = getMember(db);
		if (member != null) {
			member.queue(action);
		}
	}

	public synchronized void copyFromTo(DBDatabase template, DBDatabase secondary) {
		if (getMember(template) == null) {
			System.out.println("TEMPLATE NOT FOUND");
			return;
		}
		ClusterMember templateMember = getMember(template);
		ClusterMember secondaryMember = getMember(secondary);
		if (templateMember != null) {
			templateMember.copyTo(secondaryMember);
		}

	}

	private boolean waitUntilADatabaseIsReady() {
		while (getReadyDatabases().length == 0) {
			synchronized (A_DATABASE_IS_READY) {
				try {
					A_DATABASE_IS_READY.wait(100);
				} catch (InterruptedException ex) {
					Logger.getLogger(ClusterMemberList.class
							.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}
		return getReadyDatabases().length > 0;
	}

	private boolean waitUntilADatabaseIsReady(long millisecondsToWait) {
		synchronized (A_DATABASE_IS_READY) {
			try {
				A_DATABASE_IS_READY.wait(millisecondsToWait);
				return getReadyDatabases().length > 0;
			} catch (InterruptedException ex) {
				Logger.getLogger(ClusterMemberList.class
						.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	private boolean waitUntilAStatusHasChanged(long millisecondsToWait) {
		synchronized (A_STATUS_HAS_CHANGED) {
			try {
				A_STATUS_HAS_CHANGED.wait(millisecondsToWait);
				return true;

			} catch (InterruptedException ex) {
				Logger.getLogger(ClusterMemberList.class
						.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	public void notifyADatabaseIsReady() {
		synchronized (A_DATABASE_IS_READY) {
			A_DATABASE_IS_READY.notifyAll();
		}
		if (isSynchronised()) {
			synchronized (CLUSTER_HAS_SYNCHRONISED) {
				CLUSTER_HAS_SYNCHRONISED.notifyAll();
			}
		}
	}

	public void notifyAStatusHasChanged() {
		synchronized (A_STATUS_HAS_CHANGED) {
			A_STATUS_HAS_CHANGED.notifyAll();
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
		while (true) {
			if (isSynchronised()) {
				return;
			} else {
				synchronized (CLUSTER_HAS_SYNCHRONISED) {
					CLUSTER_HAS_SYNCHRONISED.wait(100);
				}
			}
		}
	}

	private void waitOnClusterHasSynchronised(int timeout) throws InterruptedException {
		if (isSynchronised()) {
			return;
		}
		synchronized (CLUSTER_HAS_SYNCHRONISED) {
			CLUSTER_HAS_SYNCHRONISED.wait(timeout);
		}
	}

	void setTemplate(DBDatabase template) {
		System.out.println("TEMPLATE: " + template.getLabel());
		set(TEMPLATE, template);
	}

	public boolean waitOnStatusChange(DBDatabaseCluster.Status status, int timeout, DBDatabase... affectedMembers) {
		if (affectedMembers.length == 0) {
			return false;
		}
		List<ClusterMember> membs = getMembers(affectedMembers);
		if (membs.size() == 0) {
			return false;
		}
		Instant endTime = Instant.now().plus(0L + timeout, ChronoUnit.MICROS);
		while (Instant.now().isBefore(endTime)) {
			for (ClusterMember memb : membs) {
				if (memb.getStatus().equals(status)) {
					return true;
				}
			}
			waitUntilAStatusHasChanged(timeout);
		}
		return false;
	}

	public DBDatabase waitOnStatusChange(DBDatabase db, int timeout, DBDatabaseCluster.Status... statuses) {
		List<DBDatabaseCluster.Status> statusList = Arrays.asList(statuses);
		boolean found = false;
		while (!found) {
			for (ClusterMember memb : members.values()) {
				if (statusList.contains(memb.getStatus())) {
					return memb.getDatabase();
				}
			}
			waitUntilAStatusHasChanged(timeout);
		}
		return null;
	}

	public synchronized DBDatabase getTemplateDatabase(DBDatabase target) throws OnlyOneDatabaseInClusterException, NoAvailableDatabaseException {
		DBDatabase template = null;
		if (getDatabasesByStatus(PROCESSING, TEMPLATE, READY).length < 1) {
			if (details.getConfiguration().isUseAutoRebuild()) {
				// Use the saved database if we're restarting a AutoRebuild cluster
				System.out.println("TEMPLATE: using authorative database");
				template = details.getAuthoritativeDatabase();
			} else {
				// if there's only 1 db and it's not an AutoRebuild cluster
				// then we should NOT be using a template
				throw new OnlyOneDatabaseInClusterException();
			}
		} else {
			// we know that at least one database has synchronised so lets try and get
			// a template by grabbing a READY database
			template = target;
			LoopVariable loop = LoopVariable.factory(10);
			while (loop.attempt()) {
				template = Random.get(getDatabasesByStatus(READY));
				loop.done(template != null && !template.equals(target));
			}
			if (template == null) {
				// we failed to get a READY database, so check for PROCESSING and 
				// TEMPLATE databases
				final DBDatabase[] processingDBs = getDatabasesByStatus(PROCESSING, TEMPLATE, READY);
				if (processingDBs.length > 0) {
					// there are PROCESSING databases so lets wait for one to become READY
					if (waitOnStatusChange(READY, 1000, processingDBs)) {
						// try again now that we have a READY database
						template = Random.get(getDatabasesByStatus(READY));
					} else {
						throw new NoAvailableDatabaseException();
					}
				} else {
					throw new NoAvailableDatabaseException();
				}
			}
		}
		if (template == null) {
			throw new NoAvailableDatabaseException();
		} else {
			setTemplate(template);
			return template;
		}
	}

	public boolean hasDatabasesOfStatus(DBDatabaseCluster.Status status) {
		return countDatabases(status) > 0;
	}

	public DBDatabase getRandomDatabase(DBDatabase... dbs) throws NoAvailableDatabaseException {
		DBDatabase db = Random.get(dbs);
		if (db == null) {
			throw new NoAvailableDatabaseException();
		}
		return db;
	}

	@Override
	public void close() {
		if (members.size() > 0) {
			members.entrySet().stream().forEach((e) -> e.getValue().close());
		}
	}
}
