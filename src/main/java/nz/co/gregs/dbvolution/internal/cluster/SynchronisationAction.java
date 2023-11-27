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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.OnlyOneDatabaseInClusterException;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.internal.database.ClusterMemberList;

/**
 *
 * @author gregorygraham
 */
public class SynchronisationAction extends DBAction {

	private static final long serialVersionUID = 1L;
	private final DBDatabase target;
	private final ClusterDetails cluster;
	private static final Logger LOG = Logger.getLogger(SynchronisationAction.class.getName());
	private final ClusterMemberList members;

	public <R extends DBRow> SynchronisationAction(ClusterDetails details, DBDatabase target) {
		super(null, QueryIntention.SYNCHRONISE_WITH_CLUSTER);
		this.cluster = details;
		this.target = target;
		this.members = cluster.getMembers();
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		DBActionList actions = new DBActionList();

		DBDatabase template = null;
		final String secondaryLabel = target.getLabel();
		final String clusterLabel = cluster.getClusterLabel();
		LOG.log(Level.FINEST, "{0} SYNCHRONISING: {1}", new Object[]{clusterLabel, secondaryLabel});
		// we need to unpause the template no matter what happens so use a finally clause
		try {
			template = members.getTemplateDatabase(target);
			if (template != null) {
				// Check that we're not synchronising the reference database
				if (!template.getSettings().equals(target.getSettings())) {
					LOG.log(Level.FINEST, "{0} CAN SYNCHRONISE: {1}", new Object[]{clusterLabel, secondaryLabel});
					// TODO change to use a queue of tables so we can re-try tables that require another table to exist
					for (DBRow table : cluster.getRequiredAndTrackedTables()) {
						final String tableName = table.getTableName();
							LOG.log(Level.FINEST, "{0} CHECKING TABLE: {1}", new Object[]{clusterLabel, tableName});
							// make sure the table exists in the cluster already
							if (template.tableExists(table)) {
								LOG.log(Level.FINEST, "{0} INCLUDES TABLE: {1}", new Object[]{clusterLabel, tableName});
								// Make sure it exists in the new database
								if (target.tableExists(table) == true) {
									LOG.log(Level.FINEST, "{0} REMOVING DATA FROM {1}: {2}", new Object[]{clusterLabel, secondaryLabel, tableName});
									actions.add(new DBDropTableIfExists(table));
									LOG.log(Level.FINEST, "{0} REMOVED DATA FROM {1}: {2}", new Object[]{clusterLabel, secondaryLabel, tableName});
								}
								LOG.log(Level.FINEST, "{0} CREATING ON {1}: {2}", new Object[]{clusterLabel, secondaryLabel, tableName});
								actions.add(new DBCreateTable(table, true));
								LOG.log(Level.FINEST, "{0} CREATED ON {1}: {2}", new Object[]{clusterLabel, secondaryLabel, tableName});
								// Check that the table has data
								final DBTable<DBRow> primaryTable = template.getDBTable(table);
								try {
									final Long primaryTableCount = primaryTable.count();
									try {
										if (primaryTableCount > 0) {
											final DBTable<DBRow> primaryData = primaryTable.setBlankQueryAllowed(true).setTimeoutToForever();
											// Check that the new database has data
											LOG.log(Level.FINEST, "{0} CLUSTER FILLING TABLE ON {1}:{2}", new Object[]{clusterLabel, secondaryLabel, tableName});
											List<DBRow> allRows = primaryData.getAllRows();
											LOG.log(Level.FINEST, "{0} CLUSTER FILLING TABLE ON {1}:{2} with {3} rows", new Object[]{clusterLabel, secondaryLabel, tableName, allRows.size()});
											actions.add(new DBBulkInsert(allRows));
											LOG.log(Level.FINEST, "{0} FILLED TABLE ON {1}:{2}", new Object[]{clusterLabel, secondaryLabel, tableName});
										}
									} catch (SQLException exceptionGettingData) {
										LOG.log(Level.WARNING, "FAIL TO RETREIVE TABLE DATA: {0} - {1}", new Object[]{tableName, exceptionGettingData.getLocalizedMessage()});
										LOG.log(Level.WARNING, "SKIPPING TABLE: {0} - {1}", new Object[]{tableName, exceptionGettingData.getLocalizedMessage()});
										// lets just skip this table since it seems to be broken
									}
								} catch (SQLException exceptionCountingPrimaryTable) {
									LOG.log(Level.WARNING, "FAILED TO COUNT TABLE: {0} - {1}", new Object[]{tableName, exceptionCountingPrimaryTable.getLocalizedMessage()});
									LOG.log(Level.WARNING, "SKIPPING TABLE: {0} - {1}", new Object[]{tableName, exceptionCountingPrimaryTable.getLocalizedMessage()});
									// lets just skip this table since it seems to be broken
								}
							}
						LOG.log(Level.FINEST, "{0} FINISHED WITH TABLE: {1}", new Object[]{clusterLabel, tableName});
					}
					cluster.queueAction(target, actions);
					// the structure is done so copy the buffered actions 
					cluster.addTemplateActionQueueToSecondary(template, target);
					// Successfully synchronised the new database :)
				}
			}
		} catch (OnlyOneDatabaseInClusterException except) {
			System.out.println("SYNCH COMPLETE: "+secondaryLabel+" is the only database in this cluster");
//			 must be the first database
//			 let it proceed without doing anything
		} finally {
			releaseTemplateDatabase(template);
		}
		System.out.println("Completed synchronisation action");
		return actions;
	}

	@Override
	public String toString() {
		return getIntent() + " " + cluster.getClusterLabel();
	}

	public synchronized void releaseTemplateDatabase(DBDatabase primary) throws NoAvailableDatabaseException {
		if (primary != null) {
				System.out.println("RELEASING TEMPLATE: " + primary.getLabel());
				members.setProcessing(primary);
				System.out.println("RELEASED TEMPLATE: " + primary.getLabel());
		}
	}
}
