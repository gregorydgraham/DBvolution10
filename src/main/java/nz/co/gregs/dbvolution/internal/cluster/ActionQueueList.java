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
import java.util.HashMap;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;

/**
 *
 * @author gregorygraham
 */
public class ActionQueueList implements Serializable {

	private static final long serialVersionUID = 1L;

	private final HashMap<DBDatabase, ActionQueue> queues = new HashMap<>(2);
	private final ClusterDetails clusterDetails;

	public ActionQueueList(ClusterDetails clusterDetails) {
		this.clusterDetails = clusterDetails;
	}

	public synchronized ActionQueue add(DBDatabase db) {
		final ActionQueue actionQueue = new ActionQueue(db, clusterDetails, 100000);
		actionQueue.start();
		queues.put(db, actionQueue);
		return actionQueue;
	}

	public synchronized ActionQueue remove(DBDatabase db) {
		ActionQueue found = queues.get(db);
		if (found != null) {
			found.stop();
			queues.remove(db);
		}
		return found;
	}

	public synchronized ActionQueue remove(ActionQueue q) {
		queues.remove(q.getDatabase());
		return q;
	}

	public synchronized void clear() {
		queues.forEach((db, queue) -> {
			queue.stop();
		});
		queues.clear();
	}

	public void enqueue(DBDatabase database, DBAction action) {
		final ActionQueue queue = queues.get(database);
		if (queue != null) {
			queue.add(action);
		}
	}
}
