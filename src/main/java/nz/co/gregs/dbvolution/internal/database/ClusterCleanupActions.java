/*
 * Copyright 2021 Gregory Graham.
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

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import org.apache.commons.logging.Log;

/**
 * Cleans up the cluster's databases after the cluster exits scope.
 *
 * <p>
 * Removes all databases from the cluster without terminating them and shutdown
 * all cluster processes.
 *
 * <p>
 * Dismantling the cluster is only needed in a small number of scenarios, mostly
 * testing.
 *
 * <p>
 * Dismantling the cluster ends all threads, removes all databases, and removes
 * the authoritative database configuration.
 *
 * <p>
 * This process is similar to {@link DBDatabaseCluster#stop()
 * } but does not stop or dismantle the individual databases.
 */
public class ClusterCleanupActions implements Runnable, Serializable {

	private static final long serialVersionUID = 1L;

	private final ClusterDetails details;
	private final Log log;
	private final ExecutorService actionThreadPool;

	public ClusterCleanupActions(ClusterDetails details, Log log, ExecutorService actionThreadPool) {
		this.details = details;
		this.log = log;
		this.actionThreadPool = actionThreadPool;
	}

	@Override
	public void run() {
		log.debug("CLEANING UP CLUSTER...");
		actionThreadPool.shutdown();
		try {
			details.removeAllDatabases();
		} catch (SQLException ex) {
			Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

}
