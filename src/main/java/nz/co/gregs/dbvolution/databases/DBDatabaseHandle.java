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

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.SettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.transactions.DBTransaction;

/**
 * A DBDatabaseHandle makes it easy to switch between databases.
 *
 * <p>
 * This is intended to be useful for "superuser" applications rather than
 * everyday data entry or reporting.</p>
 *
 * <p>
 * DBDatabaseHandle is a simple wrapper on a DBDatabase, with the addition of
 * the
 * {@link DBDatabaseHandle#setDatabase(nz.co.gregs.dbvolution.databases.DBDatabase) setDatabase method}.
 * This allows the underlying database to be changed for all references to this
 * object.</p>
 *
 * <p>
 * This allows for an application to be used with several databases while being
 * designed for a unitary database.
 *
 * @author gregorygraham
 */
public class DBDatabaseHandle extends DBDatabase {

	private static final long serialVersionUID = 1L;

	private DBDatabase wrappedDatabase;

	public DBDatabaseHandle() {
		super();
		try {
			wrappedDatabase = H2MemoryDB.createANewRandomDatabase();
		} catch (SQLException ex) {
			Logger.getLogger(DBDatabaseHandle.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public DBDatabaseHandle(SettingsBuilder<?, ?> settings) throws SQLException, Exception {
		wrappedDatabase = settings.getDBDatabase();
	}

	public DBDatabaseHandle(DBDatabase db) {
		wrappedDatabase = wrappedDatabase;
	}

	public synchronized DBDatabaseHandle setDatabase(DBDatabase db) {
		wrappedDatabase = wrappedDatabase;
		return this;
	}

	/**
	 * Used By Subclasses To Inject Datatypes, Functions, Etc Into the Database.
	 *
	 * @param statement the statement to use when adding features, DO NOT CLOSE
	 * THIS STATEMENT .
	 * @
	 * throws ExceptionDuringDatabaseFeatureSetup database exceptions may occur
	 * @see PostgresDB
	 * @see H2DB
	 * @see SQLiteDB
	 * @see OracleDB
	 * @see MSSQLServerDB
	 * @see MySQLDB
	 *
	 */
	public void addDatabaseSpecificFeatures(final Statement stmt) throws ExceptionDuringDatabaseFeatureSetup {
	}

	/**
	 * Clones the DBDatabase
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a clone of the database.
	 * @throws java.lang.CloneNotSupportedException
	 * java.lang.CloneNotSupportedException
	 *
	 */
	public DBDatabaseHandle clone() throws CloneNotSupportedException {
		return new DBDatabaseHandle(wrappedDatabase.clone());
	}

	/**
	 * Used to add features in a just-in-time manner.
	 *
	 * <p>
	 * During a statement the database may throw an exception because a feature
	 * has not yet been added. Use this method to parse the exception and install
	 * the required feature.
	 *
	 * <p>
	 * The statement will be automatically run after this method exits.
	 *
	 * @param exp the exception throw by the database that may need fixing
	 * @param intent the intention of the query or DDL when the exception was
	 * thrown
	 * @return the preferred response to the exception
	 * @throws SQLException accessing the database may cause exceptions
	 */
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		return wrappedDatabase.addFeatureToFixException(exp, intent);
	}

	public boolean isMemoryDatabase() {
		return wrappedDatabase.isMemoryDatabase();
	}

	/**
	 * Returns the port number usually assign to instances of this database.
	 *
	 * <p>
	 * There is no guarantee that the particular database instance uses this port,
	 * check with your DBA.</p>
	 *
	 * @return the port number commonly used by this type of database
	 */
	public Integer getDefaultPort() {
		return wrappedDatabase.getDefaultPort();
	}

	public SettingsBuilder<?, ?> getURLInterpreter() {
		return wrappedDatabase.getURLInterpreter();
	}

	public void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		wrappedDatabase.setDefinitionBasedOnConnectionMetaData(clientInfo, metaData);
	}

	public <TR extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement,
			TR tableRow) throws SQLException {
		wrappedDatabase.dropAnyAssociatedDatabaseObjects(dbStatement, tableRow);
	}

	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		dropTableNoExceptions(tableRow);
	}

	public Connection getConnectionFromDriverManager() throws SQLException {
		return wrappedDatabase.getConnectionFromDriverManager();
	}

	public boolean supportsMicrosecondPrecision() {
		return wrappedDatabase.supportsMicrosecondPrecision();
	}

	public boolean supportsNanosecondPrecision() {
		return wrappedDatabase.supportsNanosecondPrecision();
	}

	public void stop() {
		super.stop();
		wrappedDatabase.stop();
	}

	public boolean tableExists(DBRow table) throws SQLException {
		return wrappedDatabase.tableExists(table);
	}

	public void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTable(newTableRow, includeForeignKeyClauses);
	}

	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.executeDBQuery(query);
	}

	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		return wrappedDatabase.executeDBAction(action);
	}

	public synchronized DBDefinition getDefinition() throws NoAvailableDatabaseException {
		return wrappedDatabase.getDefinition();
	}

}
