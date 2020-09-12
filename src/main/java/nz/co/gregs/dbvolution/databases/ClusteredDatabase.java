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
import nz.co.gregs.dbvolution.DBMigration;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryInsert;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRecursiveQuery;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.SettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfDatabaseException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToDropDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.transactions.DBTransaction;

/**
 *
 * @author gregorygraham
 */
public class ClusteredDatabase extends DBDatabase {

	private static final long serialVersionUID = 1L;
	private final DBDatabase internalDatabase;
	private final ClusteredDefinition internalDefinition;

	public ClusteredDatabase(DBDatabase database) {
		this.internalDatabase = database;
		this.internalDefinition = new ClusteredDefinition(internalDatabase.getDefinition());
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		internalDatabase.addDatabaseSpecificFeatures(statement);
	}

	@Override
	protected SettingsBuilder<?, ?> getURLInterpreter() {
		return internalDatabase.getURLInterpreter();
	}

	@Override
	public Integer getDefaultPort() {
		return internalDatabase.getDefaultPort();
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
	public String toString() {
		return internalDatabase.toString();
	}

	@Override
	DBTransactionStatement getDBTransactionStatement() throws SQLException {
		return internalDatabase.getDBTransactionStatement();
	}

	@Override
	protected DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return internalDatabase.getLowLevelStatement();
	}

	@Override
	public synchronized DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return internalDatabase.getConnection();
	}

	@Override
	protected DBActionList updateAnyway(List<DBRow> rows) throws SQLException {
		return internalDatabase.updateAnyway(rows);
	}

	@Override
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return internalDatabase.get(exampleRow);
	}

	@Override
	public <R extends DBRow> long getCount(R exampleRow) throws SQLException, AccidentalCartesianJoinException {
		return internalDatabase.getCount(exampleRow);
	}

	@Override
	public long getCount(DBRow example, DBRow... examples) throws SQLException, AccidentalCartesianJoinException {
		return internalDatabase.getCount(example, examples);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.getByExample(exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.get(expectedNumberOfRows, exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.getByExample(expectedNumberOfRows, exampleRow);
	}

	@Override
	public List<DBQueryRow> get(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.get(row, rows);
	}

	@Override
	public List<DBQueryRow> getByExamples(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.getByExamples(row, rows);
	}

	@Override
	public void print(List<?> rows) {
		internalDatabase.print(rows);
	}

	@Override
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow row, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.get(expectedNumberOfRows, row, rows);
	}

	@Override
	public synchronized <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, ExceptionThrownDuringTransaction {
		return internalDatabase.doTransaction(dbTransaction, commit);
	}

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction {
		return internalDatabase.doTransaction(dbTransaction);
	}

	@Override
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return internalDatabase.doReadOnlyTransaction(dbTransaction);
	}

	@Override
	public DBActionList implement(DBScript script) throws Exception {
		return internalDatabase.implement(script);
	}

	@Override
	public DBActionList test(DBScript script) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return internalDatabase.test(script);
	}

	@Override
	public synchronized String getDriverName() {
		return internalDatabase.getDriverName();
	}

	@Override
	protected synchronized void setDriverName(String driver) {
		internalDatabase.setDriverName(driver);
	}

	@Override
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return internalDatabase.getDBTable(example);
	}

	@Override
	public DBQuery getDBQuery(DBRow example) {
		return internalDatabase.getDBQuery(example);
	}

	@Override
	public DBQuery getDBQuery() {
		return internalDatabase.getDBQuery();
	}

	@Override
	public DBQuery getDBQuery(DBRow example, DBRow... examples) {
		return internalDatabase.getDBQuery(example, examples);
	}

	@Override
	public DBQuery getDBQuery(Collection<DBRow> examples) {
		return internalDatabase.getDBQuery(examples);
	}

	@Override
	public synchronized void setPrintSQLBeforeExecuting(boolean b) {
		internalDatabase.setPrintSQLBeforeExecuting(b);
	}

	@Override
	public boolean isPrintSQLBeforeExecuting() {
		return internalDatabase.isPrintSQLBeforeExecuting();
	}

	@Override
	public void printSQLIfRequested(String sqlString) {
		internalDatabase.printSQLIfRequested(sqlString);
	}

	@Override
	synchronized void printSQLIfRequested(String sqlString, PrintStream out) {
		internalDatabase.printSQLIfRequested(sqlString, out);
	}

	@Override
	public void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		internalDatabase.createTableNoExceptions(includeForeignKeyClauses, newTable);
	}

	@Override
	public void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		internalDatabase.createTableNoExceptions(newTable);
	}

	@Override
	public void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		internalDatabase.createTablesNoExceptions(includeForeignKeyClauses, newTables);
	}

	@Override
	public void createTablesNoExceptions(DBRow... newTables) {
		internalDatabase.createTablesNoExceptions(newTables);
	}

	@Override
	public void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		internalDatabase.createTablesWithForeignKeysNoExceptions(newTables);
	}

	@Override
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		internalDatabase.createTable(newTableRow);
	}

	@Override
	public void createOrUpdateTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		internalDatabase.createOrUpdateTable(newTableRow);
	}

	@Override
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		internalDatabase.createTableWithForeignKeys(newTableRow);
	}

	@Override
	public synchronized void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		internalDatabase.createForeignKeyConstraints(newTableRow);
	}

	@Override
	public synchronized void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		internalDatabase.removeForeignKeyConstraints(newTableRow);
	}

	@Override
	public synchronized void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		internalDatabase.createIndexesOnAllFields(newTableRow);
	}

	@Override
	public synchronized void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		internalDatabase.dropTable(tableRow);
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		internalDatabase.dropTableNoExceptions(tableRow);
	}

	@Override
	public <TR extends DBRow> void dropTableIfExists(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, SQLException {
		internalDatabase.dropTableIfExists(tableRow);
	}

	@Override
	public synchronized DBDefinition getDefinition() throws NoAvailableDatabaseException {
		return internalDefinition;
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) throws NoAvailableDatabaseException {
		return internalDatabase.willCreateBlankQuery(row);
	}

	@Override
	public synchronized void dropDatabase(boolean doIt) throws AccidentalDroppingOfDatabaseException, UnableToDropDatabaseException, SQLException, AutoCommitActionDuringTransactionException, ExceptionThrownDuringTransaction {
		internalDatabase.dropDatabase(doIt);
	}

	@Override
	public synchronized void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfDatabaseException, SQLException, ExceptionThrownDuringTransaction {
		internalDatabase.dropDatabase(databaseName, doIt);
	}

	@Override
	public synchronized void setDatabaseName(String databaseName) {
		internalDatabase.setDatabaseName(databaseName);
	}

	@Override
	public synchronized boolean batchSQLStatementsWhenPossible() {
		return internalDatabase.batchSQLStatementsWhenPossible();
	}

	@Override
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		internalDatabase.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
	}

	@Override
	protected synchronized void preventDDLDuringTransaction(String message) throws AutoCommitActionDuringTransactionException {
		internalDatabase.preventDDLDuringTransaction(message);
	}

	@Override
	public synchronized void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		internalDatabase.preventDroppingOfTables(droppingTablesIsAMistake);
	}

	@Override
	protected synchronized boolean getPreventAccidentalDroppingOfTables() {
		return internalDatabase.getPreventAccidentalDroppingOfTables();
	}

	@Override
	public synchronized void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		internalDatabase.preventDroppingOfDatabases(justLeaveThisAtTrue);
	}

	@Override
	public synchronized boolean getPreventAccidentalDroppingOfDatabases() {
		return internalDatabase.getPreventAccidentalDroppingOfDatabases();
	}

	@Override
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.get(report, examples);
	}

	@Override
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.getAllRows(report, examples);
	}

	@Override
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return internalDatabase.getRows(report, examples);
	}

	@Override
	protected Connection getConnectionFromDriverManager() throws SQLException {
		return internalDatabase.getConnectionFromDriverManager();
	}

	@Override
	protected <R extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, R tableRow) throws SQLException {
		internalDatabase.dropAnyAssociatedDatabaseObjects(dbStatement, tableRow);
	}

	@Override
	public synchronized void unusedConnection(DBConnection connection) throws SQLException {
		internalDatabase.unusedConnection(connection);
	}

	@Override
	protected boolean supportsPooledConnections() {
		return internalDatabase.supportsPooledConnections();
	}

	@Override
	public synchronized void discardConnection(DBConnection connection) {
		internalDatabase.discardConnection(connection);
	}

	@Override
	protected ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		return internalDatabase.addFeatureToFixException(exp, intent);
	}

	@Override
	public <T extends DBRow> DBRecursiveQuery<T> getDBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow, T dbRow) {
		return internalDatabase.getDBRecursiveQuery(query, keyToFollow, dbRow);
	}

	@Override
	public boolean isDBDatabaseCluster() {
		return internalDatabase.isDBDatabaseCluster();
	}

	@Override
	public void setLastException(Exception except) {
		internalDatabase.setLastException(except);
	}

	@Override
	public Exception getLastException() {
		return internalDatabase.getLastException();
	}

	@Override
	protected void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		internalDatabase.setDefinitionBasedOnConnectionMetaData(clientInfo, metaData);
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
	public boolean supportsDifferenceBetweenNullAndEmptyString() {
		return getDefinition().canProduceNullStrings();
	}

	@Override
	public <K extends DBRow> DBQueryInsert<K> getDBQueryInsert(K mapper) {
		return internalDatabase.getDBQueryInsert(mapper);
	}

	@Override
	public <K extends DBRow> DBMigration<K> getDBMigration(K mapper) {
		return internalDatabase.getDBMigration(mapper);
	}

	@Override
	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		return internalDatabase.executeDBAction(action);
	}

	/* TODO Probably need to change this to use this database rather than the internal one*/
	@Override
	public String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException {
		return internalDatabase.getSQLForDBQuery(query);
	}

	@Override
	public boolean tableExists(DBRow table) throws SQLException {
		return internalDatabase.tableExists(table);
	}

	@Override
	boolean tableExists(Class<? extends DBRow> tab) throws SQLException {
		return internalDatabase.tableExists(tab);
	}

	@Override
	public void updateTableToMatchDBRow(DBRow table) throws SQLException {
		internalDatabase.updateTableToMatchDBRow(table);
	}

	@Override
	public DatabaseConnectionSettings getSettings() {
		return internalDatabase.getSettings();
	}

	@Override
	protected void setSettings(DatabaseConnectionSettings newSettings) {
		internalDatabase.setSettings(newSettings);
	}

	@Override
	protected void startServerIfRequired() {
		internalDatabase.startServerIfRequired();
	}

	@Override
	public boolean isMemoryDatabase() {
		return internalDatabase.isMemoryDatabase();
	}

	@Override
	public synchronized void stop() {
		System.out.println("STOPPING INTERNAL DATABASE \""+internalDatabase.getLabel()+"\"...");
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

	@Override
	public boolean getPrintSQLBeforeExecuting() {
		return internalDatabase.getPrintSQLBeforeExecuting();
	}

	@Override
	public boolean getBatchSQLStatementsWhenPossible() {
		return internalDatabase.getBatchSQLStatementsWhenPossible();
	}

	@Override
	public void backupToDBDatabase(DBDatabase backupDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		internalDatabase.backupToDBDatabase(backupDatabase);
	}

	public void stopClustering() {
		System.out.println("CLUSTERED DATABASE: stopping clustering");
		super.stop();
	}

	public DBDatabase getInternalDatabase() {
		return internalDatabase;
	}
}
