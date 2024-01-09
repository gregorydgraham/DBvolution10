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
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.DBDatabaseImplementation.ResponseToException;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.metadata.DBDatabaseMetaData;
import nz.co.gregs.dbvolution.databases.settingsbuilders.SettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.databases.metadata.Options;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.dbvolution.transactions.DBTransaction;

/**
 * A DBDatabaseHandle makes it easy to switch between databases.
 *
 * <p>
 * This is intended to be useful for "superuser" applications rather than
 * everyday data entry or reporting.
 *
 * <p>
 * DBDatabaseHandle is a simple wrapper on a DBDatabase, with the addition of
 * the
 * {@link DBDatabaseHandle#setDatabase(nz.co.gregs.dbvolution.databases.DBDatabase) setDatabase method}.
 * This allows the underlying database to be changed for all references to this
 * object.
 *
 * <p>
 * This allows for an application to be used with several databases while being
 * designed for a unitary database.
 *
 * @author gregorygraham
 */
public class DBDatabaseHandle implements DBDatabase {

	private static final long serialVersionUID = 1L;

	private DBDatabase wrappedDatabase;

	@Override
	public void setPreventAccidentalDeletingAllRowsFromTable(boolean b) {
		wrappedDatabase.setPreventAccidentalDeletingAllRowsFromTable(b);
	}

	@Override
	public void createOrUpdateTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createOrUpdateTable(newTableRow);
	}

	@Override
	public void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTableNoExceptions(newTable);
	}

	@Override
	public void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTableNoExceptions(includeForeignKeyClauses, newTable);
	}

	@Override
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTableWithForeignKeys(newTableRow);
	}

	@Override
	public void createTablesNoExceptions(DBRow... newTables) {
		wrappedDatabase.createTablesNoExceptions(newTables);
	}

	@Override
	public void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		wrappedDatabase.createTablesNoExceptions(includeForeignKeyClauses, newTables);
	}

	@Override
	public void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		wrappedDatabase.createTablesWithForeignKeysNoExceptions(newTables);
	}

	@Override
	public void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfDatabaseException, SQLException, ExceptionThrownDuringTransaction {
		wrappedDatabase.dropDatabase(databaseName, doIt);
	}

	@Override
	public void dropDatabase(boolean doIt) throws AccidentalDroppingOfDatabaseException, UnableToDropDatabaseException, SQLException, AutoCommitActionDuringTransactionException, ExceptionThrownDuringTransaction {
		wrappedDatabase.dropDatabase(doIt);
	}

	@Override
	public DBActionList dropTableIfExists(DBRow tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, SQLException {
		return wrappedDatabase.dropTableIfExists(tableRow);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getByExample(expectedNumberOfRows, exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getByExample(exampleRow);
	}

	@Override
	public List<DBQueryRow> getByExamples(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getByExamples(row, rows);
	}

	@Override
	public <T extends DBRow> DBRecursiveQuery<T> getDBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow, T dbRow) {
		return wrappedDatabase.getDBRecursiveQuery(query, keyToFollow, dbRow);
	}

	@Override
	public DBActionList update(Collection<? extends DBRow> listOfRowsToUpdate) throws SQLException {
		return wrappedDatabase.update(listOfRowsToUpdate);
	}

	@Override
	public DBActionList update(DBRow... rows) throws SQLException {
		return wrappedDatabase.update(rows);
	}

	@Override
	public DBActionList implement(DBScript script) throws Exception {
		return wrappedDatabase.implement(script);
	}

	@Override
	public Instant getCurrentInstant() throws UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, SQLException {
		return wrappedDatabase.getCurrentInstant();
	}

	@Override
	public LocalDateTime getCurrentLocalDatetime() throws SQLException {
		return wrappedDatabase.getCurrentLocalDatetime();
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
	public <K extends DBRow> DBQueryInsert<K> getDBQueryInsert(K mapper) {
		return wrappedDatabase.getDBQueryInsert(mapper);
	}

	@Override
	public void setLastException(Throwable except) {
		wrappedDatabase.setLastException(except);
	}

	@Override
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.createTable(newTableRow);
	}

	@Override
	public boolean getPrintSQLBeforeExecuting() {
		return wrappedDatabase.getPrintSQLBeforeExecuting();
	}

	@Override
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return wrappedDatabase.getDBTable(example);
	}

	@Override
	public DBActionList insert(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException {
		return wrappedDatabase.insert(listOfRowsToInsert);
	}

	@Override
	public DBActionList insert(DBRow row) throws SQLException {
		return wrappedDatabase.insert(row);
	}

	@Override
	public DBActionList insert(DBRow... listOfRowsToInsert) throws SQLException {
		return wrappedDatabase.insert(listOfRowsToInsert);
	}

	@Override
	public DBActionList insertOrUpdate(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException {
		return wrappedDatabase.insertOrUpdate(listOfRowsToInsert);
	}

	@Override
	public DBActionList insertOrUpdate(DBRow... rows) throws SQLException {
		return wrappedDatabase.insertOrUpdate(rows);
	}

	@Override
	public DBActionList insertOrUpdate(DBRow row) throws SQLException {
		return wrappedDatabase.insertOrUpdate(row);
	}

	@Override
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(report, examples);
	}

	@Override
	public List<DBQueryRow> get(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(row, rows);
	}

	@Override
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow row, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(expectedNumberOfRows, row, rows);
	}

	@Override
	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.get(expectedNumberOfRows, exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return wrappedDatabase.get(exampleRow);
	}

	@Override
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getAllRows(report, examples);
	}

	@Override
	public DBActionList delete(Collection<? extends DBRow> list) throws SQLException {
		return wrappedDatabase.delete(list);
	}

	@Override
	public DBActionList delete(DBRow... rows) throws SQLException {
		return wrappedDatabase.delete(rows);
	}

	@Override
	public DBDatabase copy() {
		return new DBDatabaseHandle(wrappedDatabase.copy());
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
	public boolean supportsDifferenceBetweenNullAndEmptyString() {
		return wrappedDatabase.supportsDifferenceBetweenNullAndEmptyString();
	}

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction {
		return wrappedDatabase.doTransaction(dbTransaction);
	}

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, ExceptionThrownDuringTransaction {
		return wrappedDatabase.doTransaction(dbTransaction, commit);
	}

	@Override
	public DBActionList test(DBScript script) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return wrappedDatabase.test(script);
	}

	@Override
	public void unusedConnection(DBConnection connection) throws SQLException {
		wrappedDatabase.unusedConnection(connection);
	}

	@Override
	public void discardConnection(DBConnection connection) {
		wrappedDatabase.discardConnection(connection);
	}

	@Override
	public DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return wrappedDatabase.getConnection();
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
	public void printSQLIfRequested(String sqlString, PrintStream out) {
		wrappedDatabase.printSQLIfRequested(sqlString, out);
	}

	@Override
	public void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		wrappedDatabase.preventDroppingOfDatabases(justLeaveThisAtTrue);
	}

	@Override
	public void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		wrappedDatabase.preventDroppingOfTables(droppingTablesIsAMistake);
	}

	@Override
	public void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		wrappedDatabase.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
	}

	@Override
	public boolean getBatchSQLStatementsWhenPossible() {
		return wrappedDatabase.getBatchSQLStatementsWhenPossible();
	}

	@Override
	public boolean batchSQLStatementsWhenPossible() {
		return wrappedDatabase.batchSQLStatementsWhenPossible();
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) throws NoAvailableDatabaseException {
		return wrappedDatabase.willCreateBlankQuery(row);
	}

	@Override
	public void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		wrappedDatabase.createIndexesOnAllFields(newTableRow);
	}

	@Override
	public void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		wrappedDatabase.removeForeignKeyConstraints(newTableRow);
	}

	@Override
	public void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		wrappedDatabase.createForeignKeyConstraints(newTableRow);
	}

	@Override
	public void updateTableToMatchDBRow(DBRow table) throws SQLException {
		wrappedDatabase.updateTableToMatchDBRow(table);
	}

	@Override
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return wrappedDatabase.doReadOnlyTransaction(dbTransaction);
	}

	@Override
	public DBTransactionStatement getDBTransactionStatement() throws SQLException {
		return wrappedDatabase.getDBTransactionStatement();
	}

	@Override
	public void commitTransaction() throws SQLException {
		wrappedDatabase.commitTransaction();
	}

	@Override
	public void rollbackTransaction() throws SQLException {
		wrappedDatabase.rollbackTransaction();
	}

	@Override
	public <V> IncompleteTransaction<V> doTransactionWithoutCompleting(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction {
		return wrappedDatabase.doTransactionWithoutCompleting(dbTransaction);
	}

	@Override
	public void setQuietExceptionsPreference(boolean b) {
		wrappedDatabase.setQuietExceptionsPreference(b);
	}

	@Override
	public boolean getQuietExceptionsPreference() {
		return wrappedDatabase.getQuietExceptionsPreference();
	}

	@Override
	public String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException {
		return wrappedDatabase.getSQLForDBQuery(query);
	}

	@Override
	public DBStatement getDBStatement() throws SQLException {
		return wrappedDatabase.getDBStatement();
	}

	@Override
	public void setPrintSQLBeforeExecuting(boolean b) {
		wrappedDatabase.setPrintSQLBeforeExecuting(b);
	}

	@Override
	public String getHost() {
		return wrappedDatabase.getHost();
	}

	@Override
	public String getJdbcURL() {
		return wrappedDatabase.getJdbcURL();
	}

	@Override
	public String getLabel() {
		return wrappedDatabase.getLabel();
	}

	@Override
	public String getPassword() {
		return wrappedDatabase.getPassword();
	}

	@Override
	public String getPort() {
		return wrappedDatabase.getPort();
	}

	@Override
	public String getSchema() {
		return wrappedDatabase.getSchema();
	}

	@Override
	public String getUsername() {
		return wrappedDatabase.getUsername();
	}

	@Override
	public String getDatabaseInstance() {
		return wrappedDatabase.getDatabaseInstance();
	}

	@Override
	public String getDatabaseName() {
		return wrappedDatabase.getDatabaseName();
	}

	@Override
	public String getDriverName() {
		return wrappedDatabase.getDriverName();
	}

	@Override
	public Map<String, String> getExtras() {
		return wrappedDatabase.getExtras();
	}

	@Override
	public DatabaseConnectionSettings getSettings() {
		return wrappedDatabase.getSettings();
	}

	@Override
	public DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
		return wrappedDatabase.getSettingsFromJDBCURL(jdbcURL);
	}

	@Override
	public String getUrlFromSettings(DatabaseConnectionSettings oldSettings) {
		return wrappedDatabase.getUrlFromSettings(oldSettings);
	}

	@Override
	public DataSource getDataSource() {
		return wrappedDatabase.getDataSource();
	}

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
		wrappedDatabase = db;
	}

	public synchronized DBDatabaseHandle setDatabase(DBDatabase db) {
		wrappedDatabase = db;
		return this;
	}

	/**
	 * Used By Subclasses To Inject Datatypes, Functions, Etc Into the Database.
	 *
	 * @param stmt the statement to use when adding features, DO NOT CLOSE THIS
	 * STATEMENT .
	 * @throws ExceptionDuringDatabaseFeatureSetup database exceptions may occur
	 * @see PostgresDB
	 * @see H2DB
	 * @see SQLiteDB
	 * @see OracleDB
	 * @see MSSQLServerDB
	 * @see MySQLDB
	 *
	 */
	@Override
	public void addDatabaseSpecificFeatures(final Statement stmt) throws ExceptionDuringDatabaseFeatureSetup {
	}

	/**
	 * Clones the DBDatabase
	 *
	 *
	 * @return a clone of the database.
	 * @throws java.lang.CloneNotSupportedException
	 * java.lang.CloneNotSupportedException
	 *
	 */
	@Override
	public DBDatabaseHandle clone() throws CloneNotSupportedException {
		return new DBDatabaseHandle(wrappedDatabase.clone());
	}

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent, StatementDetails details) throws Exception {
		return wrappedDatabase.addFeatureToFixException(exp, intent, details);
	}

	@Override
	public boolean isMemoryDatabase() {
		return wrappedDatabase.isMemoryDatabase();
	}

	/**
	 * Returns the port number usually assign to instances of this database.
	 *
	 * <p>
	 * There is no guarantee that the particular database instance uses this port,
	 * check with your DBA.
	 *
	 * @return the port number commonly used by this type of database
	 */
	@Override
	public Integer getDefaultPort() {
		return wrappedDatabase.getDefaultPort();
	}

	@Override
	public SettingsBuilder<?, ?> getURLInterpreter() {
		return wrappedDatabase.getURLInterpreter();
	}

	@Override
	public void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		wrappedDatabase.setDefinitionBasedOnConnectionMetaData(clientInfo, metaData);
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		wrappedDatabase.dropTableNoExceptions(tableRow);
	}

	@Override
	public Connection getConnectionFromDriverManager() throws SQLException {
		return wrappedDatabase.getConnectionFromDriverManager();
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
	public void stop() {
		wrappedDatabase.stop();
	}

	@Override
	public boolean tableExists(DBRow table) throws SQLException {
		return wrappedDatabase.tableExists(table);
	}

	@Override
	public List<DBAction> createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
		return wrappedDatabase.createTable(newTableRow, includeForeignKeyClauses);
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.executeDBQuery(query);
	}

	@Override
	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		return wrappedDatabase.executeDBAction(action);
	}

	@Override
	public synchronized DBDefinition getDefinition() throws NoAvailableDatabaseException {
		return wrappedDatabase.getDefinition();
	}

	@Override
	public void deleteAllRowsFromTable(DBRow table) throws SQLException {
		wrappedDatabase.deleteAllRowsFromTable(table);
	}

	@Override
	public List<DBAction> dropTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		return wrappedDatabase.dropTable(newTableRow);
	}

	@Override
	public void handleErrorDuringExecutingSQL(DBDatabase suspectDatabase, Throwable sqlException, String sqlString) {
		wrappedDatabase.handleErrorDuringExecutingSQL(suspectDatabase, sqlException, sqlString);
	}

	@Override
	public boolean supportsGeometryTypesFullyInSchema() {
		return wrappedDatabase.supportsGeometryTypesFullyInSchema();
	}

	@Override
	public void print(List<?> rows) {
		wrappedDatabase.print(rows);
	}

	@Override
	public <MAPPER extends DBRow> DBMigration<MAPPER> getDBMigration(MAPPER mapper) {
		return wrappedDatabase.getDBMigration(mapper);
	}

	@Override
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return wrappedDatabase.getRows(report, examples);
	}

	@Override
	public <R extends DBRow> long getCount(R exampleRow) throws SQLException, AccidentalCartesianJoinException {
		return wrappedDatabase.getCount(exampleRow);
	}

	@Override
	public DBDatabaseMetaData getDBDatabaseMetaData(Options options) throws SQLException {
		return wrappedDatabase.getDBDatabaseMetaData(options);
	}

	@Override
	public SettingsBuilder<?, ?> getSettingsBuilder() {
		return this.wrappedDatabase.getSettingsBuilder();
	}

	@Override
	public void close() {
		this.wrappedDatabase.close();
	}

}
