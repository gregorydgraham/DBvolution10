/*
 * Copyright 2020 Gregory Graham.
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
package nz.co.gregs.dbvolution.databases;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.databases.settingsbuilders.SettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;

/**
 *
 * @author gregorygraham
 */
public class ClusteredDatabase extends DBDatabase {

	private static final long serialVersionUID = 1L;
	private DBDatabase internalDatabase;

	public ClusteredDatabase(DBDatabase database) throws SQLException {
		super();
		this.internalDatabase = database;
		setHasCreatedRequiredTables(true);
		initDatabase(
				database
						.getURLInterpreter()
						.fromSettings(database.getSettings())
						.setDefinition(
								new ClusteredDefinition(database.getDefinition())
						)
		);
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		internalDatabase.addDatabaseSpecificFeatures(statement);
	}

	@Override
	public SettingsBuilder<?, ?> getURLInterpreter() {
		return internalDatabase.getURLInterpreter();
	}

	@Override
	public Integer getDefaultPort() {
		return internalDatabase.getDefaultPort();
	}

	@Override
	public ClusteredDatabase clone() throws CloneNotSupportedException {
		return (ClusteredDatabase) super.clone();
	}

	@Override
	public DBDatabase.ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		return internalDatabase.addFeatureToFixException(exp, intent);
	}

	@Override
	public <R extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, R tableRow) throws SQLException {
		internalDatabase.dropAnyAssociatedDatabaseObjects(dbStatement, tableRow);
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		super.dropTableNoExceptions(tableRow);
	}

	@Override
	public Connection getConnectionFromDriverManager() throws SQLException {
		return internalDatabase.getConnectionFromDriverManager();
	}

	@Override
	public boolean supportsMicrosecondPrecision() {
		return internalDatabase.supportsMicrosecondPrecision();
	}

	@Override
	public boolean supportsNanosecondPrecision() {
		return internalDatabase.supportsNanosecondPrecision();
	}

	@Override
	public boolean isMemoryDatabase() {
		return internalDatabase.isMemoryDatabase();
	}

	@Override
	public void close() {
		try {
			internalDatabase.close();
		} catch (Exception exp) {
			exp.printStackTrace();
		}
		try {
			super.close();
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}

	@Override
	public synchronized void stop() {
		System.out.println("STOPPING INTERNAL DATABASE \"" + internalDatabase.getLabel() + "\"...");
		try {
			internalDatabase.stop();
		} catch (Exception exp) {
			exp.printStackTrace();
		}
		System.out.println("STOPPED INTERNAL DATABASE...");
		System.out.println("STOPPING CLUSTERED DATABASE.");
		try {
			super.stop();
		} catch (Exception exp) {
			exp.printStackTrace();
		}
		System.out.println("STOPPED CLUSTERED DATABASE.");
	}

	public void stopClustering() {
		System.out.println("CLUSTERED DATABASE: stopping clustering");
		super.stop();
	}

	public DBDatabase getInternalDatabase() {
		return internalDatabase;
	}

	@Override
	public boolean tableExists(DBRow table) throws SQLException {
		return internalDatabase.tableExists(table);
	}

	@Override
	public synchronized void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		internalDatabase.createTable(newTableRow, includeForeignKeyClauses);
	}

	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return query.query(this.internalDatabase);
	}

	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		return action.execute(this.internalDatabase);
	}
}
