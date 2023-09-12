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
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;

/**
 *
 * @author gregorygraham
 */
public class QueueReader implements Runnable {

	private static final Logger LOG = Logger.getLogger(QueueReader.class.getName());

	private final ActionQueue actionQueue;
	private final DBDatabase database;
	private final ClusterDetails clusterDetails;
	private boolean paused = false;
	private final Thread readerThread;
	private boolean keepRunning = true;

	public QueueReader(DBDatabase database, ClusterDetails details, ActionQueue dataQueue) {
		this.database = database;
		this.actionQueue = dataQueue;
		this.clusterDetails = details;

		readerThread = new Thread(this, "DBV-READER-" + database.getLabel());
	}

	@Override
	public void run() {
		while (keepRunning) {
			dequeue();
		}
		LOG.log(Level.INFO, "Thread {0} for {1} stopped", new Object[]{readerThread.getName(), database.getLabel()});
	}

	public void pause() {
		paused = true;
	}

	public void unpause() {
		paused = false;
	}

	public void dequeue() {
		if (paused) {
			actionQueue.waitUntilUnpause(1000);
		} else {
			boolean successful = attemptAction();
			if (successful == false) {
				actionQueue.notifyQueueIsEmpty();
			}
		}
		actionQueue.waitUntilActionsAvailable(100);
	}

	private boolean attemptAction() {
		boolean result = false;
		ActionMessage message = actionQueue.getHeadOfQueue();
		if (message != null) {
			final DBAction action = message.getAction();
			final QueryIntention intent = action.getIntent();
			System.out.println("READING: " + intent + " ON " + database.getLabel());

			doAction(action);
			result = true;
		}
		return result;
	}

	public void stop() {
		keepRunning = false;
	}

	private void doAction(DBAction action) {
		try {
			this.database.executeDBAction(action);
		} catch (SQLException ex) {
			LOG.log(Level.SEVERE, null, ex);
			clusterDetails.quarantineDatabase(database, ex);
		} catch (NoAvailableDatabaseException ex) {
			LOG.log(Level.SEVERE, null, ex);
		} catch (Exception ex) {
			LOG.log(Level.SEVERE, null, ex);
		}
	}

	void start() {
		readerThread.start();
	}

	boolean hasStarted() {
		return readerThread.isAlive();
	}

	boolean isPaused() {
		return paused;
	}
}
