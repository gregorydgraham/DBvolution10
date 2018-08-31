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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import nz.co.gregs.dbvolution.reflection.DataModel;

/**
 *
 * @author gregorygraham
 */
public class ClusterDetails implements Serializable {

	private final static long serialVersionUID = 1l;

	private final List<DBDatabase> allDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> unsynchronizedDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> readyDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> pausedDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> quarantinedDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));

	private final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
	private final transient Map<DBDatabase, Queue<DBAction>> queuedActions = Collections.synchronizedMap(new HashMap<DBDatabase, Queue<DBAction>>(0));

	public ClusterDetails() {
	}

	public synchronized boolean add(DBDatabase database) {
		unsynchronizedDatabases.add(database);
		return allDatabases.add(database);
	}

	public DBDatabase[] getAllDatabases() {
		synchronized (allDatabases) {
			return allDatabases.toArray(new DBDatabase[]{});
		}
	}

	public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase db) {
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

	public synchronized boolean quarantineDatabase(DBDatabase database) throws UnableToRemoveLastDatabaseFromClusterException {
		if (readyDatabases.size() < 2 && readyDatabases.contains(database)) {
			// Unable to quarantine the only remaining database
			throw new UnableToRemoveLastDatabaseFromClusterException();
		} else {
			return queuedActions.remove(database) != null
					&& allDatabases.remove(database)
					&& readyDatabases.remove(database)
					&& quarantinedDatabases.add(database);

		}
	}

	public synchronized boolean removeDatabase(DBDatabase database) {
		if (readyDatabases.size() < 2 && readyDatabases.contains(database)) {
			// Unable to quarantine the only remaining database
			throw new UnableToRemoveLastDatabaseFromClusterException();
		} else {
			return queuedActions.remove(database) != null
					&& allDatabases.remove(database)
					&& readyDatabases.remove(database)
					&& quarantinedDatabases.remove(database);
		}
	}

	public synchronized DBDatabase[] getUnsynchronizedDatabases() {
		return unsynchronizedDatabases.toArray(new DBDatabase[]{});
	}

	public synchronized void synchronizingDatabase(DBDatabase db) {
		unsynchronizedDatabases.remove(db);
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

	public DBRow[] getRequiredTables() {
		synchronized (requiredTables) {
			return requiredTables.toArray(new DBRow[]{});
		}
	}

	public synchronized void readyDatabase(DBDatabase secondary) {
		unsynchronizedDatabases.remove(secondary);
		pausedDatabases.remove(secondary);
		readyDatabases.add(secondary);
	}

	public synchronized DBDatabase[] getReadyDatabases() {
		return readyDatabases.toArray(new DBDatabase[]{});
	}

	public synchronized void pauseDatabase(DBDatabase template) {
		readyDatabases.remove(template);
		pausedDatabases.add(template);
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
	}

	public synchronized void addAll(DBDatabase[] databases) {
		for (DBDatabase database : databases) {
			add(database);
		}
	}

	public synchronized DBDatabase getTemplateDatabase() {
		if (readyDatabases.isEmpty() && pausedDatabases.isEmpty()) {
			throw new NoAvailableDatabaseException();
		}
		return getPausedDatabase();
	}

}
