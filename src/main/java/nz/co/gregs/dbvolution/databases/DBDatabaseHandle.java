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

	@Override
	public void close() {
		wrappedDatabase.close();
	}

	@Override
	public String toString() {
		return wrappedDatabase.toString();
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return wrappedDatabase.clone();
	}

	@Override
	public synchronized int hashCode() {
		return wrappedDatabase.hashCode();
	}

	@Override
	public synchronized boolean equals(Object obj) {
		return wrappedDatabase.equals(obj);
	}

	@Override
	DBTransactionStatement getDBTransactionStatement() throws SQLException {
		return wrappedDatabase.getDBTransactionStatement();
	}

	@Override
	public DBStatement getDBStatement() throws SQLException {
		return wrappedDatabase.getDBStatement();
	}

	@Override
	protected DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return wrappedDatabase.getLowLevelStatement();
	}

	@Override
	public synchronized DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return wrappedDatabase.getConnection();
	}

	@Override
	protected DBActionList updateAnyway(List<DBRow> rows) throws SQLException {
		return wrappedDatabase.updateAnyway(rows);
	}

	@Override
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return wrappedDatabase.get(exampleRow);
	}

	@Override
	public <R extends DBRow> long getCount(R exampleRow) throws SQLException, AccidentalCartesianJoinException {
		return wrappedDatabase.getCount(exampleRow);
	}

	@Override
	public long getCount(DBRow example, DBRow... examples) throws SQLException, AccidentalCartesianJoinException {
		return wrappedDatabase.getCount(example, examples);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getByExample(exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(expectedNumberOfRows, exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getByExample(expectedNumberOfRows, exampleRow);
	}

	@Override
	public List<DBQueryRow> get(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(row, rows);
	}

	@Override
	public List<DBQueryRow> getByExamples(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getByExamples(row, rows);
	}

	@Override
	public void print(List<?> rows) {
		wrappedDatabase.print(rows);
	}

	@Override
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow row, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(expectedNumberOfRows, row, rows);
	}

	@Override
	public synchronized <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, ExceptionThrownDuringTransaction {
		return wrappedDatabase.doTransaction(dbTransaction, commit);
	}

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction {
		return wrappedDatabase.doTransaction(dbTransaction);
	}

	@Override
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return wrappedDatabase.doReadOnlyTransaction(dbTransaction);
	}

	@Override
	public DBActionList implement(DBScript script) throws Exception {
		return wrappedDatabase.implement(script);
	}

	@Override
	public DBActionList test(DBScript script) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return wrappedDatabase.test(script);
	}

	@Override
	public synchronized String getDriverName() {
		return wrappedDatabase.getDriverName();
	}

	@Override
	protected synchronized void setDriverName(String driver) {
		wrappedDatabase.setDriverName(driver);
	}

	@Override
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return wrappedDatabase.getDBTable(example);
	}

	@Override
	public DBQuery getDBQuery(DBRow example) {
		return wrappedDatabase.getDBQuery(example);
	}

	@Override
	public DBQuery getDBQuery() {
		return wrappedDatabase.getDBQuery();
	}

	@Override
	public DBQuery getDBQuery(DBRow example, DBRow... examples) {
		return wrappedDatabase.getDBQuery(example, examples);
	}

	@Override
	public DBQuery getDBQuery(Collection<DBRow> examples) {
		return wrappedDatabase.getDBQuery(examples);
	}

	@Override
	public synchronized void setPrintSQLBeforeExecuting(boolean b) {
		wrappedDatabase.setPrintSQLBeforeExecuting(b);
	}

	@Override
	public boolean isPrintSQLBeforeExecuting() {
		return wrappedDatabase.isPrintSQLBeforeExecuting();
	}

	@Override
	public void printSQLIfRequested(String sqlString) {
		wrappedDatabase.printSQLIfRequested(sqlString);
	}

	@Override
	synchronized void printSQLIfRequested(String sqlString, PrintStream out) {
		wrappedDatabase.printSQLIfRequested(sqlString, out);
	}

	@Override
	public void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTableNoExceptions(includeForeignKeyClauses, newTable);
	}

	@Override
	public void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTableNoExceptions(newTable);
	}

	@Override
	public void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		wrappedDatabase.createTablesNoExceptions(includeForeignKeyClauses, newTables);
	}

	@Override
	public void createTablesNoExceptions(DBRow... newTables) {
		wrappedDatabase.createTablesNoExceptions(newTables);
	}

	@Override
	public void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		wrappedDatabase.createTablesWithForeignKeysNoExceptions(newTables);
	}

	@Override
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTable(newTableRow);
	}

	@Override
	public void createOrUpdateTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createOrUpdateTable(newTableRow);
	}

	@Override
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTableWithForeignKeys(newTableRow);
	}

	@Override
	public synchronized void createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTable(newTableRow, includeForeignKeyClauses);
	}

	@Override
	public synchronized void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		wrappedDatabase.createForeignKeyConstraints(newTableRow);
	}

	@Override
	public synchronized void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		wrappedDatabase.removeForeignKeyConstraints(newTableRow);
	}

	@Override
	public synchronized void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		wrappedDatabase.createIndexesOnAllFields(newTableRow);
	}

	@Override
	public synchronized void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		wrappedDatabase.dropTable(tableRow);
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.dropTableNoExceptions(tableRow);
	}

	@Override
	public <TR extends DBRow> void dropTableIfExists(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, SQLException {
		wrappedDatabase.dropTableIfExists(tableRow);
	}

	@Override
	public synchronized DBDefinition getDefinition() throws NoAvailableDatabaseException {
		return wrappedDatabase.getDefinition();
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) throws NoAvailableDatabaseException {
		return wrappedDatabase.willCreateBlankQuery(row);
	}

	@Override
	public synchronized void dropDatabase(boolean doIt) throws AccidentalDroppingOfDatabaseException, UnableToDropDatabaseException, SQLException, AutoCommitActionDuringTransactionException, ExceptionThrownDuringTransaction {
		wrappedDatabase.dropDatabase(doIt);
	}

	@Override
	public synchronized void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfDatabaseException, SQLException, ExceptionThrownDuringTransaction {
		wrappedDatabase.dropDatabase(databaseName, doIt);
	}

	@Override
	public synchronized void setDatabaseName(String databaseName) {
		wrappedDatabase.setDatabaseName(databaseName);
	}

	@Override
	public synchronized boolean batchSQLStatementsWhenPossible() {
		return wrappedDatabase.batchSQLStatementsWhenPossible();
	}

	@Override
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		wrappedDatabase.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
	}

	@Override
	protected synchronized void preventDDLDuringTransaction(String message) throws AutoCommitActionDuringTransactionException {
		wrappedDatabase.preventDDLDuringTransaction(message);
	}

	@Override
	public synchronized void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		wrappedDatabase.preventDroppingOfTables(droppingTablesIsAMistake);
	}

	@Override
	protected synchronized boolean getPreventAccidentalDroppingOfTables() {
		return wrappedDatabase.getPreventAccidentalDroppingOfTables();
	}

	@Override
	public synchronized void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		wrappedDatabase.preventDroppingOfDatabases(justLeaveThisAtTrue);
	}

	@Override
	public synchronized boolean getPreventAccidentalDroppingOfDatabases() {
		return wrappedDatabase.getPreventAccidentalDroppingOfDatabases();
	}

	@Override
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(report, examples);
	}

	@Override
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getAllRows(report, examples);
	}

	@Override
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getRows(report, examples);
	}

	@Override
	public Connection getConnectionFromDriverManager() throws SQLException {
		return wrappedDatabase.getConnectionFromDriverManager();
	}

	@Override
	public <R extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, R tableRow) throws SQLException {
		wrappedDatabase.dropAnyAssociatedDatabaseObjects(dbStatement, tableRow);
	}

	@Override
	public synchronized void unusedConnection(DBConnection connection) throws SQLException {
		wrappedDatabase.unusedConnection(connection);
	}

	@Override
	protected boolean supportsPooledConnections() {
		return wrappedDatabase.supportsPooledConnections();
	}

	@Override
	public synchronized void discardConnection(DBConnection connection) {
		wrappedDatabase.discardConnection(connection);
	}

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		return wrappedDatabase.addFeatureToFixException(exp, intent);
	}

	@Override
	public <T extends DBRow> DBRecursiveQuery<T> getDBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow, T dbRow) {
		return wrappedDatabase.getDBRecursiveQuery(query, keyToFollow, dbRow);
	}

	@Override
	public boolean isDBDatabaseCluster() {
		return wrappedDatabase.isDBDatabaseCluster();
	}

	@Override
	public void setLastException(Exception except) {
		wrappedDatabase.setLastException(except);
	}

	@Override
	public Exception getLastException() {
		return wrappedDatabase.getLastException();
	}

	@Override
	public void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		wrappedDatabase.setDefinitionBasedOnConnectionMetaData(clientInfo, metaData);
	}

	@Override
	public boolean supportsMicrosecondPrecision() {
		return wrappedDatabase.supportsMicrosecondPrecision();
	}

	@Override
	public boolean supportsNanosecondPrecision() {
		return wrappedDatabase.supportsNanosecondPrecision();
	}

	@Override
	public boolean supportsDifferenceBetweenNullAndEmptyString() {
		return wrappedDatabase.supportsDifferenceBetweenNullAndEmptyString();
	}

	@Override
	protected boolean hasCreatedRequiredTables() {
		return wrappedDatabase.hasCreatedRequiredTables();
	}

	@Override
	public <K extends DBRow> DBQueryInsert<K> getDBQueryInsert(K mapper) {
		return wrappedDatabase.getDBQueryInsert(mapper);
	}

	@Override
	public <K extends DBRow> DBMigration<K> getDBMigration(K mapper) {
		return wrappedDatabase.getDBMigration(mapper);
	}

	@Override
	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		return wrappedDatabase.executeDBAction(action);
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.executeDBQuery(query);
	}

	@Override
	public String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException {
		return wrappedDatabase.getSQLForDBQuery(query);
	}

	@Override
	public boolean tableExists(DBRow table) throws SQLException {
		return wrappedDatabase.tableExists(table);
	}

	@Override
	boolean tableExists(Class<? extends DBRow> tab) throws SQLException {
		return wrappedDatabase.tableExists(tab);
	}

	@Override
	public void updateTableToMatchDBRow(DBRow table) throws SQLException {
		wrappedDatabase.updateTableToMatchDBRow(table);
	}

	@Override
	public Integer getDefaultPort() {
		return wrappedDatabase.getDefaultPort();
	}

	@Override
	public DatabaseConnectionSettings getSettings() {
		return wrappedDatabase.getSettings();
	}

	@Override
	protected void setSettings(DatabaseConnectionSettings newSettings) {
		wrappedDatabase.setSettings(newSettings);
	}

	@Override
	protected void startServerIfRequired() {
		wrappedDatabase.startServerIfRequired();
	}

	@Override
	public boolean isMemoryDatabase() {
		return wrappedDatabase.isMemoryDatabase();
	}

	@Override
	public synchronized void stop() {
		wrappedDatabase.stop();
	}

	@Override
	public boolean getPrintSQLBeforeExecuting() {
		return wrappedDatabase.getPrintSQLBeforeExecuting();
	}

	@Override
	public boolean getBatchSQLStatementsWhenPossible() {
		return wrappedDatabase.getBatchSQLStatementsWhenPossible();
	}

	@Override
	public void backupToDBDatabase(DBDatabase backupDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		wrappedDatabase.backupToDBDatabase(backupDatabase);
	}

	@Override
	protected ScheduledExecutorService getRegularThreadPool() {
		return wrappedDatabase.getRegularThreadPool();
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement stmt) throws ExceptionDuringDatabaseFeatureSetup {
		addDatabaseSpecificFeatures(stmt);
	}

	@Override
	public SettingsBuilder<?, ?> getURLInterpreter() {
		return wrappedDatabase.getURLInterpreter();
	}

}
