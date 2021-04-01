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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
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
interface DBDatabaseInterface {

	public DBDefinition getDefinition() throws NoAvailableDatabaseException;

	/**
	 * Used By Subclasses To Inject Datatypes, Functions, Etc Into the Database.
	 *
	 * @param statement the statement to use when adding features, DO NOT CLOSE
	 * THIS STATEMENT.
	 * @throws ExceptionDuringDatabaseFeatureSetup database exceptions may occur
	 * @see PostgresDB
	 * @see H2DB
	 * @see SQLiteDB
	 * @see OracleDB
	 * @see MSSQLServerDB
	 * @see MySQLDB
	 */
	void addDatabaseSpecificFeatures(final Statement stmt) throws ExceptionDuringDatabaseFeatureSetup;

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
	DBDatabaseInterface clone() throws CloneNotSupportedException;

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
	DBDatabase.ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception;

	boolean isMemoryDatabase();

	/**
	 * Returns the port number usually assign to instances of this database.
	 *
	 * <p>
	 * There is no guarantee that the particular database instance uses this port,
	 * check with your DBA.</p>
	 *
	 * @return the port number commonly used by this type of database
	 */
	Integer getDefaultPort();

	SettingsBuilder<?, ?> getURLInterpreter();

	void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData);

	<TR extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, TR tableRow) throws SQLException;

	<TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException;

	Connection getConnectionFromDriverManager() throws SQLException;

	boolean supportsMicrosecondPrecision();

	boolean supportsNanosecondPrecision();

	void stop();

	public boolean tableExists(DBRow table) throws SQLException;

	void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException;

	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException;
	
	public void handleErrorDuringExecutingSQL(DBDatabase suspectDatabase, Throwable sqlException, String sqlString);

}
