/*
 * Copyright 2023 Gregory Graham.
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
package nz.co.gregs.dbvolution.internal.cluster;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.looper.LoopVariable;

/**
 *
 * @author gregorygraham
 */
public class ActionQueueList implements Serializable {

	private static final long serialVersionUID = 1L;

	private final HashMap<String, ActionQueue> queues = new HashMap<>(2);
	private final ClusterDetails clusterDetails;
	private final int maxQueueSize = 100000;
	private transient final Object A_QUEUE_IS_READY = new Object();

	public ActionQueueList(ClusterDetails clusterDetails) {
		this.clusterDetails = clusterDetails;
	}

	private ActionQueue getNewActionQueue(DBDatabase db) {
		return new ActionQueue(db, clusterDetails, maxQueueSize, this);
	}

	public synchronized String add(DBDatabase db) {
		final String key = getKey(db);
		final ActionQueue queue = getNewActionQueue(db);
		return add(key, queue);
	}

	private synchronized String add(String key, ActionQueue queue) {
		queues.put(key, queue);
		queue.start();
		return key;
	}

	public void start(DBDatabase db) {
		ActionQueue queueForDatabase = getQueueForDatabase(db);
		queueForDatabase.start();
	}

	public synchronized int size() {
		return queues.size();
	}

	public synchronized void queueAction(DBDatabase db, DBAction act) {
		final String key = getKey(db);
		ActionQueue queue = queues.get(key);
		if (queue == null) {
			queue = getNewActionQueue(db);
			add(key, queue);
		}
		queue.add(act);
	}

	public synchronized void queueActionForAllDatabases(DBAction action) {
		Collection<ActionQueue> actionQueues = queues.values();
		for (ActionQueue actionQueue : actionQueues) {
			actionQueue.add(action);
		}
	}

	public synchronized void queueActionForAllDatabases(DBAction... actions) {
		for (DBAction action : actions) {
			queueActionForAllDatabases(action);
		}
	}

	public synchronized ActionQueue remove(DBDatabase db) {
		final String key = getKey(db);
		ActionQueue found = queues.get(key);
		if (found != null) {
			found.stop();
			queues.remove(key);
		}
		return found;
	}

	public synchronized void clear() {
		queues.forEach((db, queue) -> {
			queue.stop();
		});
		queues.clear();
	}

	public boolean waitUntilAllQueuesAreEmpty() {
		try {
			Collection<ActionQueue> values = queues.values();
			for (ActionQueue queue : values) {
				queue.waitUntilEmpty();
			}
			return true;
		} catch (Exception ex) {
			LOG.log(Level.SEVERE, null, ex);
			return false;
		}
	}

	public boolean waitUntilAQueueIsReady() {
		synchronized (A_QUEUE_IS_READY) {
			try {
				A_QUEUE_IS_READY.wait();
				return true;
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
				return false;
			}
		}
	}
	private static final Logger LOG = Logger.getLogger(ActionQueueList.class.getName());

	public boolean waitUntilAQueueIsReady(long millisecondsToWait) {
		Optional<ActionQueue> found = queues.values().stream().filter((t) -> t.isEmpty()).findAny();
		if (found.isPresent()) {
			return true;
		}
		synchronized (A_QUEUE_IS_READY) {
			try {
				A_QUEUE_IS_READY.wait(millisecondsToWait);
				return true;
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
				return false;
			}
		}
	}

	void notifyQueueIsReady() {
		synchronized (A_QUEUE_IS_READY) {
			A_QUEUE_IS_READY.notifyAll();
		}
	}

	public boolean waitUntilReady(DBDatabase db) {
		ActionQueue queue = queues.get(getKey(db));
		if (queue != null) {
			queue.waitUntilReady();
			return true;
		}
		return false;
	}

	public boolean waitUntilReady(DBDatabase db, long milliseconds) {
		ActionQueue queue = queues.get(getKey(db));
		if (queue == null) {
			return false;
		}
		queue.waitUntilEmpty(milliseconds);
		return true;

	}

	public void pause(DBDatabase db) {
		ActionQueue q = queues.get(getKey(db));
		q.pause();
	}

	public void unpause(DBDatabase db) {
		ActionQueue q = queues.get(getKey(db));
		q.unpause();
	}

	public void unpause(DBDatabase... dbs) {
		for (DBDatabase db : dbs) {
			unpause(db);
		}
	}

	public String getKey(DBDatabase db) {
		return db.getSettings().encode();
	}

	public synchronized void copyFromTo(DBDatabase template, DBDatabase secondary) {
		ActionQueue templateQ = queues.get(getKey(template));
		ActionQueue secondaryQ = queues.get(getKey(secondary));
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
	}

	ActionQueue getQueueForDatabase(DBDatabase database) {
		String key = getKey(database);
		ActionQueue queue = queues.get(key);
		if (queue == null) {
			key = add(database);
		}
		queue = queues.get(key);
		return queue;
	}

	void pause(DBDatabase... dbs) {
		for (DBDatabase db : dbs) {
			pause(db);
		}
	}

	void add(DBDatabase... dbs) {
		for (DBDatabase db : dbs) {
			add(db);
		}
	}

	ActionQueue[] getQueueForDatabase(DBDatabase... dbs) {
		ActionQueue[] result = new ActionQueue[dbs.length];
		LoopVariable loop = LoopVariable.factory(dbs.length);
		for (DBDatabase db : dbs) {
			result[loop.attempts()] = getQueueForDatabase(db);
			loop.attempt();
		}
		return result;
	}
}