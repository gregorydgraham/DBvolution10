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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;

/**
 *
 * @author gregorygraham
 */
public class ActionQueue implements AutoCloseable {

	private static final Logger LOG = Logger.getLogger(ActionQueue.class.getName());

	private final DBDatabase database;
	private final int maxSize;

	private final BlockingQueue<ActionMessage> actionQueue;
	public boolean keepRunning = true;
	private final QueueReader reader;
	private transient final Object QUEUE_IS_EMPTY = new Object();
	private transient final Object ACTION_IS_AVAILABLE = new Object();
	private transient final Object QUEUE_IS_PAUSED = new Object();
	private transient final Object QUEUE_IS_UNPAUSED = new Object();

	public ActionQueue(DBDatabase database, ClusterDetails clusterDetails, int maxSize, ActionQueueList list) {
		this.database = database;
		this.maxSize = maxSize;

		actionQueue = new LinkedBlockingDeque<>(this.maxSize);
		reader = new QueueReader(database, clusterDetails, this);
	}

	public void start() {
		keepRunning = true;
		reader.start();
	}

	public boolean hasStarted() {
		return reader.hasStarted();
	}

	public synchronized void add(DBAction action) {
		ActionMessage value = new ActionMessage(action);
		try {
			System.out.println("ENQUEUING:" + action.getIntent() + " ON " + database.getLabel());
			actionQueue.put(value);
			notifyACTION_IS_AVAILABLE();
		} catch (InterruptedException e) {
			System.out.println("ENQUEUE FAILED: " + e.getMessage());
		}
	}

	public synchronized ActionMessage getHeadOfQueue() {
		ActionMessage pulled;
		if (isEmpty()) {
			try {
				pulled = actionQueue.poll(1, TimeUnit.SECONDS);
			} catch (InterruptedException intexc) {
				pulled = null;
			}
		} else {
			pulled = actionQueue.poll();
		}
		return pulled;
	}

	public synchronized boolean isEmpty() {
		final boolean result = actionQueue.size() == 0;
		return result;
	}

	public void stop() {
		this.keepRunning = false;
		reader.stop();
	}

	public DBDatabase getDatabase() {
		return database;
	}

	public void waitUntilEmpty() {
		if (actionQueue.isEmpty()) {
			// return immediately
		} else {
			waitOnQUEUE_IS_EMPTY();
		}
	}

	public void waitUntilEmpty(long milliseconds) {
		if (actionQueue.isEmpty()) {
			// return immediately
		} else {
			waitOnQUEUE_IS_EMPTY(milliseconds);
		}
	}

	public void notifyQueueIsEmpty() {
		notifyQUEUE_IS_EMPTY();
	}

	public void notifyQueueIsReady() {
		notifyQueueIsEmpty();
	}

	public void waitUntilActionsAvailable() {
		if (actionQueue.isEmpty()) {
			waitOnACTION_IS_AVAILABLE();
		} else {
			// return immediately
		}
	}

	public void waitUntilActionsAvailable(long milliseconds) {
		if (isEmpty()) {
			waitOnACTION_IS_AVAILABLE(milliseconds);
		} else {
			// return immediately
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

	public void waitUntilReady(long milliseconds) {
		waitUntilEmpty(milliseconds);
	}

	private void notifyACTION_IS_AVAILABLE() {
		synchronized (ACTION_IS_AVAILABLE) {
			ACTION_IS_AVAILABLE.notifyAll();
		}
	}

	public synchronized void clear() {
		actionQueue.clear();
	}

	public synchronized void addAll(ActionQueue templateQ) {
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
		notifyPAUSED();
	}

	public void notifyPAUSED() {
		synchronized (QUEUE_IS_PAUSED) {
			QUEUE_IS_PAUSED.notifyAll();
		}
	}

	public void unpause() {
		reader.unpause();
		notifyUNPAUSED();
	}

	public void notifyUNPAUSED() {
		synchronized (QUEUE_IS_UNPAUSED) {
			QUEUE_IS_UNPAUSED.notifyAll();
		}
	}

	private void notifyQUEUE_IS_EMPTY() {
		synchronized (QUEUE_IS_EMPTY) {
			System.out.println("QUEUE IS EMPTY");
			QUEUE_IS_EMPTY.notifyAll();
		}
	}

	private void waitOnQUEUE_IS_EMPTY() {
		synchronized (QUEUE_IS_EMPTY) {
			try {
				QUEUE_IS_EMPTY.wait();
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	private void waitOnQUEUE_IS_EMPTY(long milliseconds) {
		synchronized (QUEUE_IS_EMPTY) {
			try {
				QUEUE_IS_EMPTY.wait(milliseconds);
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	private void waitOnACTION_IS_AVAILABLE() {
		synchronized (ACTION_IS_AVAILABLE) {
			try {
				ACTION_IS_AVAILABLE.wait();
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	private void waitOnACTION_IS_AVAILABLE(long milliseconds) {
		synchronized (ACTION_IS_AVAILABLE) {
			try {
				ACTION_IS_AVAILABLE.wait(milliseconds);
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}

	public boolean isPaused() {
		return reader.isPaused();
	}
}
