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

import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.internal.database.ClusterMember;

/**
 *
 * @author gregorygraham
 */
public class ActionQueue implements AutoCloseable {

	private static final Logger LOG = Logger.getLogger(ActionQueue.class.getName());

	private transient final Object QUEUE_IS_EMPTY = new Object();
	private transient final Object ACTION_IS_AVAILABLE = new Object();
	private transient final Object QUEUE_IS_STOPPED = new Object();
	private transient final Object QUEUE_IS_PAUSED = new Object();
	private transient final Object QUEUE_IS_UNPAUSED = new Object();
	private transient final Object ACTION_HAS_SUCCEEDED = new Object();

	private final DBDatabase database;
	private final int maxSize;
	private final ClusterMember member;

	private final BlockingQueue<ActionMessage> actionQueue;
	private final QueueReader reader;

	public ActionQueue(DBDatabase database, int maxSize, ClusterMember member) {
		this.database = database;
		this.maxSize = maxSize;
		this.member = member;

		actionQueue = new LinkedBlockingDeque<>(this.maxSize);
		reader = new QueueReader(database, this);
	}

	public void start() {
		reader.unpause();
	}

	public boolean isRunning() {
		return !reader.isPaused() && !reader.hasStopped();
	}

	public synchronized int size() {
		return actionQueue.size();
	}

	public synchronized void add(DBAction action) {
		ActionMessage value = new ActionMessage(action);
		try {
			actionQueue.put(value);
			notifyACTION_IS_AVAILABLE();
		} catch (InterruptedException e) {
		}
	}

	public synchronized void add(DBAction... actions) {
		for (DBAction action : actions) {
			add(action);
		}
	}

	public synchronized ActionMessage getHeadOfQueue() {
		ActionMessage pulled;
		try {
			pulled = actionQueue.poll(1, TimeUnit.MILLISECONDS);
		} catch (InterruptedException intexc) {
			pulled = null;
		}
		return pulled;
	}

	public synchronized ActionMessage peekHeadOfQueue() {
		ActionMessage peeked = actionQueue.peek();
		return peeked;
	}

	public synchronized boolean isEmpty() {
		final boolean result = actionQueue.size() == 0;
		return result;
	}

	public void stop() {
		reader.stop();
	}

	public DBDatabase getDatabase() {
		return database;
	}

	public boolean waitUntilEmpty() {
		if (actionQueue.isEmpty()) {
			// return immediately
			return true;
		} else {
			return waitOnQUEUE_IS_EMPTY();
		}
	}

	public boolean waitUntilEmpty(long milliseconds) {
		if (isEmpty()) {
			// return immediately
			return true;
		} else {
			return waitOnQUEUE_IS_EMPTY(milliseconds);
		}
	}

	public void notifyQueueIsEmpty() {
		notifyQUEUE_IS_EMPTY();
	}

	public void waitUntilActionsAvailable() {
		if (actionQueue.isEmpty()) {
			waitOnACTION_IS_AVAILABLE();
		} else {
			// return immediately
		}
	}

	public boolean waitUntilActionsAvailable(long milliseconds) {
		if (!isEmpty()) {
			return true;
		} else {
			return waitOnACTION_IS_AVAILABLE(milliseconds);
		}
	}

	public void waitUntilUnpause(long milliseconds) {
		synchronized (QUEUE_IS_UNPAUSED) {
			try {
				QUEUE_IS_UNPAUSED.wait(milliseconds);
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	public void waitUntilReady() {
		waitUntilEmpty();
	}

	public void waitUntilReady(long milliseconds) {
		waitUntilEmpty(milliseconds);
	}

	private void notifyACTION_IS_AVAILABLE() {
		synchronized (ACTION_IS_AVAILABLE) {
			ACTION_IS_AVAILABLE.notifyAll();
		}
	}

	public void clear() {
		actionQueue.clear();
	}

	public void addAll(ActionQueue templateQ) {
		actionQueue.addAll(templateQ.actionQueue);
	}

	@Override
	public void close() {
		stop();
	}

	public synchronized boolean hasActionsAvailable() {
		return !isEmpty();
	}

	public void pause() {
		reader.pause();
		waitUntilPaused(10000);
	}

	public void notifyPAUSED() {
		synchronized (QUEUE_IS_PAUSED) {
			QUEUE_IS_PAUSED.notifyAll();
		}
	}

	public void notifySTOPPED() {
		synchronized (QUEUE_IS_STOPPED) {
			QUEUE_IS_STOPPED.notifyAll();
		}
	}

	public void unpause() {
		reader.unpause();
	}

	public void notifyUNPAUSED() {
		synchronized (QUEUE_IS_UNPAUSED) {
			QUEUE_IS_UNPAUSED.notifyAll();
		}
	}

	private void notifyQUEUE_IS_EMPTY() {
		if(member!=null){
			try{
			member.setStatus(DBDatabaseCluster.Status.READY);}catch(Exception e){
				e.printStackTrace();
			}
		}
		synchronized (QUEUE_IS_EMPTY) {
			QUEUE_IS_EMPTY.notifyAll();
		}
	}

	private boolean waitOnQUEUE_IS_EMPTY() {
		if (actionQueue.isEmpty()) {
			return false;
		}
		synchronized (QUEUE_IS_EMPTY) {
			try {
				QUEUE_IS_EMPTY.wait();
				return true;
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
				return false;
			}
		}
	}

	private boolean waitOnQUEUE_IS_EMPTY(long milliseconds) {
		if (actionQueue.isEmpty()) {
			return false;
		}
		synchronized (QUEUE_IS_EMPTY) {
			try {
				QUEUE_IS_EMPTY.wait(milliseconds);
				return true;
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	private void waitOnACTION_IS_AVAILABLE() {
		if (reader.hasStopped()) {
			return;
		}
		synchronized (ACTION_IS_AVAILABLE) {
			try {
				ACTION_IS_AVAILABLE.wait();
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	private boolean waitOnACTION_IS_AVAILABLE(long milliseconds) {
		if (!actionQueue.isEmpty()) {
			return false;
		}
		synchronized (ACTION_IS_AVAILABLE) {
			try {
				ACTION_IS_AVAILABLE.wait(milliseconds);
				return !actionQueue.isEmpty();
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
		return !actionQueue.isEmpty();
	}

	public boolean isPaused() {
		return reader.isPaused();
	}

	public boolean isReady() {
		return isEmpty();
	}

	boolean waitUntilStopped(long milliseconds) {
		if (reader.hasStopped()) {
			return true;
		}
		synchronized (QUEUE_IS_STOPPED) {
			try {
				QUEUE_IS_STOPPED.wait(milliseconds);
				return reader.hasStopped();
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	void waitUntilPaused(long milliseconds) {
		if (reader.isPaused()) {
			return;
		}
		synchronized (QUEUE_IS_PAUSED) {
			try {
				QUEUE_IS_PAUSED.wait(milliseconds);
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	void waitUntilUnpaused(long milliseconds) {
		if (!reader.isPaused()) {
			return;
		}
		synchronized (QUEUE_IS_UNPAUSED) {
			try {
				QUEUE_IS_UNPAUSED.wait(milliseconds);
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	synchronized void quarantineDatabase(SQLException ex) {
		if (member != null) {
			member.quarantineDatabase(database, ex);
		}
	}

	void notifyActionHasSucceeded() {
		synchronized (ACTION_HAS_SUCCEEDED) {
			ACTION_HAS_SUCCEEDED.notifyAll();
		}
	}

	public void waitOnActionHasSucceeded() {
		if (reader.hasStopped()) {
			return;
		}
		synchronized (ACTION_HAS_SUCCEEDED) {
			try {
				ACTION_HAS_SUCCEEDED.wait();
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueue.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	public boolean hasStopped() {
		return this.reader.hasStopped();
	}
}
