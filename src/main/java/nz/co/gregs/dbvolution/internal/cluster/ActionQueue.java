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
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;

/**
 *
 * @author gregorygraham
 */
public class ActionQueue {

	private final DBDatabase database;
	private final int maxSize;

	private final BlockingQueue<ActionMessage> blockingQueue;
	public boolean keepRunning = true;
	private final QueueReader reader;
	private final Thread readerThread;

	public ActionQueue(DBDatabase database, ClusterDetails clusterDetails, int maxSize) {
		this.database = database;
		this.maxSize = maxSize;

		blockingQueue = new LinkedBlockingDeque<>(this.maxSize);
		reader = new QueueReader(database, clusterDetails, this);
		readerThread = new Thread(reader);
	}
	
	public void start(){
		readerThread.start();
	}

	public void add(ActionMessage message) throws InterruptedException {
		synchronized (blockingQueue) {
			blockingQueue.put(message);
		}
	}

	public void add(DBAction action) {
		ActionMessage value = new ActionMessage(action);
		try {
			System.out.println("ENQUEUING:" + action.getIntent() + " ON " + database.getLabel());
			blockingQueue.put(value);
		} catch (InterruptedException e) {
			System.out.println("ENQUEUE FAILED: " + e.getMessage());
		}
	}

	public ActionMessage remove() throws InterruptedException {
		return blockingQueue.poll(1, TimeUnit.SECONDS);
	}

	public boolean isEmpty() {
		return blockingQueue.isEmpty();
	}

	public void stop() {
		this.keepRunning = false;
		reader.stop();
	}

	public DBDatabase getDatabase() {
		return database;
	}
}
