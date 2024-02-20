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
package nz.co.gregs.dbvolution.internal.database;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueue;
import nz.co.gregs.dbvolution.internal.cluster.SynchronisationAction;

/**
 *
 * @author gregorygraham
 */
public class ClusterMember implements AutoCloseable {

	public static final int MAX_QUARANTINES_ALLOWED = 5;
	private static final int MAX_ACTIONQUEUE_SIZE = 10000;

	private final ClusterDetails details;
	private final ClusterMemberList list;
	private final DBDatabase database;
	private ActionQueue queue = null;
	private Status status = Status.CREATED;
	private Integer quarantineCount = 0;
	private String memberId = null;
	private final String clusterLabel;

	public ClusterMember(ClusterDetails details, String clusterLabel, ClusterMemberList list, DBDatabase database) {
		this.details = details;
		this.clusterLabel = clusterLabel;
		this.list = list;
		this.database = database;
	}

	public Status getStatus() {
		return status;
	}

	private void changeToReady() {
		resetQuarantineCount();
		switch (this.status) {
			case PAUSED:
			case TEMPLATE:
				break;
			case SYNCHRONIZING:
			case PROCESSING:
			case READY:
				list.notifyADatabaseIsReady();
				break;
			default:
				throwIllegalStateChangeException(READY);
		}
	}

	private void changeToPaused() {
		switch (this.status) {
			case CREATED:
			case SYNCHRONIZING:
			case PROCESSING:
			case READY:
			case PAUSED:
			case TEMPLATE:
				pause();
				break;
			default:
				throwIllegalStateChangeException(PAUSED);
		}
	}

	private void throwIllegalStateChangeException(Status newStatus) throws DBRuntimeException {
		if (!(status == Status.CREATED && newStatus == Status.READY)) {
			// CREATED => READY just happens shockingly often because of my design so ignore it
			System.out.println("ILLEGAL STATUS CHANGE: " + status + " => " + newStatus.toString() + " on " + this.database);
		}
		throw new DBRuntimeException("ILLEGAL STATUS CHANGE: " + status + " => " + newStatus.toString() + " on " + this.database);
	}

	private void changeToDead() {
		if (QUARANTINED.equals(status)) {
			stop();
		} else {
			throwIllegalStateChangeException(DEAD);
		}
	}

	private void changeToQuarantined() {
		stop();
	}

	public synchronized final void setStatus(Status newStatus) {
		try {
			if (!status.equals(newStatus)) {
				switch (newStatus) {
					case CREATED:
						changeToCreated();
						break;
					case DEAD:
						changeToDead();
						break;
					case PAUSED:
						changeToPaused();
						break;
					case SYNCHRONIZING:
						changeToSynchronising();
						break;
					case PROCESSING:
						changeToProcessing();
						break;
					case QUARANTINED:
						changeToQuarantined();
						break;
					case READY:
						changeToReady();
						break;
					case TEMPLATE:
						changeToTemplate();
						break;
					case UNKNOWN:
						changeToUnknown();
						break;
					default:
						throwIllegalStateChangeException(newStatus);
				}
				this.status = newStatus;
				list.notifyAStatusHasChanged();
			}
			if (QUARANTINED.equals(newStatus)) {
				incrementQuarantineCount();
				checkForDeadDatabase();
			}
		} catch (DBRuntimeException exception) {
			// IGNORING ILLEGAL STATUS CHANGES IS NORMAL, HONEST
		}
	}

	private void checkForDeadDatabase() {
		if (QUARANTINED.equals(status)) {
			if (quarantineCount >= MAX_QUARANTINES_ALLOWED) {
				setStatus(DEAD);
			}
		}
	}

	public synchronized Integer getQuarantineCount() {
		return quarantineCount;
	}

	public synchronized void incrementQuarantineCount() {
		this.quarantineCount++;
	}

	public DBDatabase getDatabase() {
		return database;
	}

	public void stop() {
		if (queue != null) {
			queue.stop();
		}
	}

	@Override
	public void close() {
		if (queue != null) {
			queue.stop();
		}
	}

	public synchronized void resetQuarantineCount() {
		quarantineCount = 0;
	}

	public synchronized final void start() {
		status = PAUSED;
		checkQueue();
		queue.pause();
		queue.clear();
		queue.add(new SynchronisationAction(details, database));
		status = SYNCHRONIZING;
		queue.unpause();
	}

	public synchronized final void unpause() {
		checkQueue();
		queue.unpause();
	}

	public void quarantineDatabase(DBDatabase database, SQLException ex) {
		setStatus(QUARANTINED);
	}

	public void pause() {
		checkQueue();
		queue.pause();
	}

	public String getMemberId() {
		if (memberId == null) {
			this.memberId = clusterLabel + "(" + database.getLabel() + ")";
		}
		return this.memberId;
	}

	private void changeToUnknown() {
		stop();
	}

	private void changeToProcessing() {
		switch (status) {
			case SYNCHRONIZING:
			case PROCESSING:
			case READY:
				//these are all processing statuses and should self-correct
				break;
			case PAUSED:
			case TEMPLATE:
				unpause();
				break;
			case CREATED:
			case QUARANTINED:
				start();
				break;
			case UNKNOWN:
				break;
			default:
				throwIllegalStateChangeException(PROCESSING);
		}
		checkQueue();
	}

	private void changeToTemplate() {
		if (READY.equals(status)) {
			pause();
		} else {
			throwIllegalStateChangeException(TEMPLATE);
		}
	}

	private void changeToSynchronising() {
		switch (status) {
			case PAUSED:
				start();
				break;
			case CREATED:
				start();
				break;
			case SYNCHRONIZING:
			case PROCESSING:
				break;
			case QUARANTINED:
				start();
				break;
			case READY:
				break;
			case TEMPLATE:
				start();
				break;
			case UNKNOWN:
				start();
				break;
			default:
				throwIllegalStateChangeException(SYNCHRONIZING);
		}
	}

	private void changeToCreated() {
		start();
	}

	private void getNewQueue() {
		queue = new ActionQueue(database, MAX_ACTIONQUEUE_SIZE, this);
	}

	private void checkQueue() {
		if (queue == null) {
			getNewQueue();
		}
		if (queue.hasStopped()) {
			getNewQueue();
			queue.start();
		}
	}

	public void queue(DBAction action) {
		checkQueue();
		queue.add(action);
		setStatus(PROCESSING);
	}

	public synchronized void copyTo(ClusterMember secondary) {
		if (secondary == null) {
			return;
		}
		ActionQueue templateQ = this.queue;
		ActionQueue secondaryQ = secondary.queue;
		boolean templatePaused = templateQ.isPaused();
		boolean secondaryPaused = secondaryQ.isPaused();
		if (!templatePaused) {
			templateQ.pause();
		}
		if (!secondaryPaused) {
			secondaryQ.pause();
		}
		// DO THE JOB!!!
		secondaryQ.addAll(templateQ);
		// Now clean up
		if (!secondaryPaused) {
			secondaryQ.unpause();
		}
		if (!templatePaused) {
			templateQ.unpause();
		}
	}
}
