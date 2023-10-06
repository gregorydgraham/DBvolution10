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
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueue;
import nz.co.gregs.dbvolution.internal.cluster.SynchronisationAction;

/**
 *
 * @author gregorygraham
 */
public class ClusterMember implements AutoCloseable {

	private final ClusterDetails details;
	private final DatabaseList list;
	private final DBDatabase database;
	private final ActionQueue queue;
	private Status status = Status.PAUSED;
	private Integer quarantineCount = 0;

	public ClusterMember(ClusterDetails details, DatabaseList list, DBDatabase database) {
		this.details = details;
		this.list = list;
		this.database = database;
		this.queue = new ActionQueue(database, 10000, this);
	}

	public synchronized Status getStatus() {
		return status;
	}

	public synchronized final void setStatus(Status status) {
		Status oldStatus = this.status;
		this.status = status;
		switch (status) {
			case DEAD:
				stop();
				break;
			case PAUSED:
				stop();
				break;
			case PROCESSING:
				start();
				break;
			case QUARANTINED:
				incrementQuarantineCount();
				stop();
				break;
			case READY:
				resetQuarantineCount();
				break;
			case UNKNOWN:
				stop();
				break;
		}
		if (!oldStatus.equals(this.status)) {
			System.out.println("MEMBER: " + database.getLabel() + " NOW STATUS " + status);
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

	public ActionQueue getQueue() {
		return queue;
	}

	void stop() {
		queue.stopReader();
	}

	@Override
	public void close() {
		queue.stopReader();

	}

	public synchronized void resetQuarantineCount() {
		quarantineCount = 0;
	}

	public synchronized final void start() {
		status = PAUSED;
		queue.stopReader();
		queue.clear();
		queue.add(new SynchronisationAction(details, database));
		status = PROCESSING;
		queue.startReader();
	}

	public void notifyADatabaseIsReady() {
		if (status.equals(PROCESSING)) {
			setStatus(READY);
			list.notifyADatabaseIsReady();
		}
	}

	public void quarantineDatabase(DBDatabase database, SQLException ex) {
		setStatus(QUARANTINED);

	}
}
