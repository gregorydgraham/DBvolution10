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
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;

/**
 *
 * @author gregorygraham
 */
public class QueueReader implements AutoCloseable{

	private static final Logger LOG = Logger.getLogger(QueueReader.class.getName());

	private final ActionQueue actionQueue;
	private final DBDatabase database;
	private final Runner runner;
	private final Object THREAD_DEATH = new Object();

	{
		try {
			Runtime.getRuntime().addShutdownHook(new StopReader(this));
		} catch (Exception exc) {
			/* the only exception I know of is an illegal state exception
			   if we create a QueueReader while the runtime is trying to 
			   shutdown
			 */
		}
	}

	public QueueReader(DBDatabase database, ActionQueue dataQueue) {
		this.database = database;
		this.actionQueue = dataQueue;
		this.runner = getNewRunner();
		runner.start();
	}

	private Runner getNewRunner() {
		return new Runner(this);
	}

	public void pause() {
		runner.pause();
	}

	public void unpause() {
		runner.pause(false);
		actionQueue.notifyUNPAUSED();
	}

	public void stop() {
		runner.stop();
		waitOnThreadDeath(100);
	}

	public boolean isPaused() {
		return runner.isPaused();
	}

	public boolean hasStarted() {
		return runner.hasStarted();
	}

	public boolean hasStopped() {
		return runner.hasStopped();
	}

	private void attemptAction() {
		ActionMessage message = actionQueue.getHeadOfQueue();
		if (message == null) {
			if (!isPaused()) {
				actionQueue.notifyQueueIsEmpty();
			}
		} else {
			final DBAction action = message.getAction();
			doAction(action);
		}
	}

	private void doAction(DBAction action) {
		try {
			database.executeDBAction(action);
			actionQueue.notifyActionHasSucceeded();
		} catch (SQLException ex) {
			LOG.log(Level.SEVERE, "FAILED " + action + " ON DATABASE " + database.getLabel()+" BECAUSE OF "+ex.getLocalizedMessage(), ex);
			actionQueue.quarantineDatabase(ex);
		} catch (NoAvailableDatabaseException ex) {
			LOG.log(Level.SEVERE, "FAILED " + action + " ON DATABASE " + database.getLabel() + " BECAUSE OF NoAvailableDatabaseException", ex);
			actionQueue.quarantineDatabase(new SQLException(ex));
		} catch (Exception ex) {
			LOG.log(Level.SEVERE, "FAILED " + action + " ON DATABASE " + database.getLabel() + " BECAUSE OF " + ex.getLocalizedMessage(), ex);
			actionQueue.quarantineDatabase(new SQLException(ex));
		}
	}

	public boolean waitOnThreadDeath(long milliseconds) {
		if (!runner.isAlive()) {
			return true;
		}
		synchronized (THREAD_DEATH) {
			try {
				THREAD_DEATH.wait(milliseconds);
				if (!runner.isAlive()) {
					return true;
				}
			} catch (InterruptedException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
		return false;
	}

	private void notifyThreadDeath() {
		synchronized (THREAD_DEATH) {
			THREAD_DEATH.notifyAll();
		}
	}

	private boolean waitUntilActionsAvailable(int milliseconds) {
		return actionQueue.waitUntilActionsAvailable(milliseconds);
	}

	private String getLabel() {
		return database.getLabel();

	}

	private void notifyPaused() {
		actionQueue.notifyPAUSED();
	}

	@Override
	public void close() {
		runner.stop();
		waitOnThreadDeath(1000);
	}

	private static class StopReader extends Thread {

		private final QueueReader reader;

		public StopReader(QueueReader aThis) {
			reader = aThis;
		}

		@Override
		public void run() {
			try {
				reader.stop();
			} catch (Exception e) {
				LOG.log(Level.INFO, "Exception while stopping QueueReader: {0}", e.getLocalizedMessage());
			}
		}
	}

	private static class Runner implements Runnable, AutoCloseable {

		private final QueueReader queueReader;
		private final Thread readerThread;

		private boolean proceed = true;
		private boolean paused = true;

		private Runner(QueueReader reader) {
			this.queueReader = reader;

			readerThread = new Thread(this, "READER thread for " + queueReader.getLabel());
		}

		@Override
		public void run() {
			while (proceed) {
				waitUntilActionAvailable();
				dequeue();
			}
			LOG.log(Level.INFO, "Thread {0} for {1} stopped", new Object[]{readerThread.getName(), queueReader.getLabel()});
			queueReader.notifyThreadDeath();
		}

		private synchronized void dequeue() {
			if (!paused) {
				attemptAction();
			} else {
				notifyPaused();
			}
		}

		private void notifyPaused() {
			queueReader.notifyPaused();
		}

		private void attemptAction() {
			queueReader.attemptAction();
		}

		private void waitUntilActionAvailable() {
			queueReader.waitUntilActionsAvailable(10);
		}

		private void stop() {
			proceed = false;
		}

		private void start() {
			readerThread.start();
		}

		private boolean hasStarted() {
			return proceed && readerThread.isAlive();
		}

		private boolean hasStopped() {
			return !readerThread.isAlive();
		}

		private void pause() {
			paused = true;
		}

		private void pause(boolean shouldPause) {
			paused = shouldPause;
		}

		private boolean isPaused() {
			return paused;
		}

		private boolean isAlive() {
			return readerThread.isAlive();
		}

		@Override
		public void close() {
			stop();
		}

	}
}
