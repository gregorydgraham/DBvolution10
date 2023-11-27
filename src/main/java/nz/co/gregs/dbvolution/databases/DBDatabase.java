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
import java.io.Serializable;
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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.metadata.DBDatabaseMetaData;
import nz.co.gregs.dbvolution.databases.settingsbuilders.SettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.databases.metadata.Options;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.dbvolution.transactions.DBTransaction;

/**
 *
 * @author gregorygraham
 */
public interface DBDatabase extends Serializable {

	public DBDefinition getDefinition() throws NoAvailableDatabaseException;

	/**
	 * Used By Subclasses To Inject Datatypes, Functions, Etc Into the Database.
	 *
	 * @param stmt the statement to use when adding features, DO NOT CLOSE THIS
	 * STATEMENT.
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
	 * @return a clone of the database.
	 * @throws java.lang.CloneNotSupportedException
	 * java.lang.CloneNotSupportedException
	 *
	 */
	DBDatabase clone() throws CloneNotSupportedException;

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
	 * @param details the details for the statement
	 * @return the preferred response to the exception
	 * @throws SQLException accessing the database may cause exceptions
	 */
	DBDatabaseImplementation.ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent, StatementDetails details) throws Exception;

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

	<TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException;

	Connection getConnectionFromDriverManager() throws SQLException;

	boolean supportsMicrosecondPrecision();

	boolean supportsNanosecondPrecision();

	void stop();

	public boolean tableExists(DBRow table) throws SQLException;

	List<DBAction> createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException;

	List<DBAction> dropTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException;

	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException;

	public void handleErrorDuringExecutingSQL(DBDatabase suspectDatabase, Throwable sqlException, String sqlString);

	void deleteAllRowsFromTable(DBRow table) throws SQLException;

	boolean supportsGeometryTypesFullyInSchema();

	String getHost();

	/**
	 * Returns the JDBC URL used by this instance, if one has been specified.
	 *
	 * @return the jdbcURL
	 */
	String getJdbcURL();

	/**
	 * A label for the database for reference within an application.
	 *
	 * <p>
	 * This label has no effect on the actual database connection.
	 *
	 * @return the internal label of this database
	 */
	String getLabel();

	/**
	 * Returns the password specified.
	 *
	 * @return the password
	 */
	String getPassword();

	String getPort();

	String getSchema();

	/**
	 * Returns the username specified for this DBDatabase instance.
	 *
	 * @return the username
	 */
	String getUsername();

	String getDatabaseInstance();

	/**
	 * Returns the database name if one was supplied.
	 *
	 * @return the database name
	 */
	String getDatabaseName();

	/**
	 * Returns the name of the JDBC driver class used by this DBDatabase instance.
	 *
	 * @return the driverName
	 */
	String getDriverName();

	Map<String, String> getExtras();

	DatabaseConnectionSettings getSettings();

	DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL);

	String getUrlFromSettings(DatabaseConnectionSettings oldSettings);

	DataSource getDataSource();

	/**
	 *
	 * @param justLeaveThisAtTrue	justLeaveThisAtTrue
	 */
	/*
	 * The lack of documentation is by design: you shouldn't be using this method.
	If you must use it, maybe you're the DBA or something, this only works for one call of dropDatabase().
	It is automatically reset to TRUE after every use to avoid accidental use.
	Also note that there is a race condition between the setting of this and your call to dropDatabase().  If other code
	calls dropDatabase() somewhere else, it may get there before you do, so just never use this, OK?
	 */
	void preventDroppingOfDatabases(boolean justLeaveThisAtTrue);

	/**
	 *
	 * @param droppingTablesIsAMistake just leave it at TRUE.
	 */
	/*
	 * The lack of documentation is by design: you probably shouldn't be using this method.
	If you must use it, maybe you're the DBA or something, this only works for one call of dropTable().
	It is automatically reset to TRUE after every use to avoid accidental use.
	Also note that there is a race condition between the setting of this and your call to dropTable().  If other code
	calls dropTable() somewhere else, it may get there before you do, so just never use this, OK?
	 */
	void preventDroppingOfTables(boolean droppingTablesIsAMistake);

	/**
	 * Sets whether this DBDatabase will attempt to batch multiple SQL commands.
	 *
	 * <p>
	 * It is possible to execute several SQL statements in one instruction, and
	 * generally DBvolution attempts to do that when handed several actions at
	 * once.
	 * <p>
	 * However sometimes this is inappropriate and this method can help with those
	 * times.
	 *
	 * @param batchSQLStatementsWhenPossible TRUE if this instance will try to
	 * batch SQL statements, FALSE otherwise
	 */
	void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible);

	boolean getBatchSQLStatementsWhenPossible();

	/**
	 * Returns whether this DBDatabase will attempt to batch multiple SQL
	 * commands.
	 *
	 * <p>
	 * It is possible to execute several SQL statements in one instruction, and
	 * generally DBvolution attempts to do that when handed several actions at
	 * once.
	 * <p>
	 * However sometimes this is inappropriate and this method can help with those
	 * times.
	 *
	 * @return TRUE if this instance will try to batch SQL statements, FALSE
	 * otherwise
	 */
	boolean batchSQLStatementsWhenPossible();

	/**
	 * Returns whether or not the example has any specified criteria.See
	 * {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.databases.definitions.DBDefinition) willCreateBlankQuery}
	 * on DBRow.
	 *
	 *
	 * @param row row
	 * @return TRUE if the specified row has no specified criteria, FALSE
	 * otherwise
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown when a cluster cannot service requests
	 */
	boolean willCreateBlankQuery(DBRow row) throws NoAvailableDatabaseException;

	/**
	 * Adds Database Indexes To All Fields Of This Table.
	 *
	 * <p>
	 * Use this method to add indexes to all the columns of the table. This is
	 * only necessary once and should really be performed by a DBA.
	 *
	 * <p>
	 * Adding indexes can improve response time for queries, but has consequences
	 * for storage and insertion time. However in a small database the query
	 * improvement will far out weigh the down sides and this is a recommend route
	 * to improvements.
	 *
	 * <p>
	 * As usual, your mileage may vary and consult a DBA if trouble persists.
	 *
	 * @param newTableRow the data model's version of the table that needs indexes
	 * @throws SQLException database exceptions
	 */
	/*TODONE: convert to use DBAction to improve cluster implementation */
	void createIndexesOnAllFields(DBRow newTableRow) throws SQLException;

	/**
	 * Drops All Foreign Key Constraints From The Supplied Table, does not affect
	 * &#64;DBForeignKey.
	 *
	 * <p>
	 * Generates and executes the required SQL to remove all foreign key
	 * constraints on this table defined within the database.
	 *
	 * <p>
	 * This methods is supplied as an inverse to
	 * {@link #createForeignKeyConstraints(nz.co.gregs.dbvolution.DBRow)}.
	 *
	 * <p>
	 * If a pair of tables have foreign keys constraints to each other it may be
	 * necessary to remove the constraints to successfully insert some rows.
	 * DBvolution cannot to protect you using this situation, however this method
	 * will remove some of the problem.
	 *
	 * @param newTableRow the data models version of the table that needs FKs
	 * removed
	 * @throws SQLException database exceptions
	 */
	/*TODONE: convert to use DBAction to improve cluster implementation */
	void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException;

	/**
	 * Adds actual foreign key constraints to the database table represented by
	 * the supplied DBRow.
	 *
	 * <p>
	 * While database theory stipulates that foreign keys should be represented by
	 * a constraint on the table, this is not part of the industry standard.
	 * DBvolution allows for the creation of these constraints through this
	 * method.
	 *
	 * <p>
	 * All databases support FK constraints, and they provide useful checks.
	 * However they are the last possible check, represent an inadequate
	 * protection, and can cause considerable difficulties at surprising times. I
	 * recommend against them.
	 *
	 * <p>
	 * Note: SQLite does not support adding Foreign Keys to existing tables.
	 *
	 * @param newTableRow the table that needs foreign key constraints
	 * @throws SQLException the database has had an issue.
	 */
	/*TODONE: convert to use DBAction to improve cluster implementation */
	void createForeignKeyConstraints(DBRow newTableRow) throws SQLException;

	/**
	 * Uses the supplied DBRow to update the existing database table by creating
	 * the table, if necessary, or adding any columns that are missing.
	 *
	 * @param table the database table representation that is correct
	 * @throws java.sql.SQLException database errors
	 */
	void updateTableToMatchDBRow(DBRow table) throws SQLException;

	/**
	 * Convenience method to test a DBScript on this database
	 *
	 * equivalent to script.test(this);
	 *
	 * @param script the script to executed and rollback
	 * @return a DBActionList provided by the script
	 * @throws java.sql.SQLException database errors
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception using the transaction
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown when a cluster cannot service requests
	 */
	default DBActionList test(DBScript script) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException {
		return script.test(this);
	}

	/**
	 * Performs the transaction on this database without making changes.
	 *
	 * <p>
	 * If there is an exception of any kind the transaction is rolled back and no
	 * changes are made.
	 *
	 * <p>
	 * If no exception occurs, the transaction is still rolled back and no changes
	 * are made
	 *
	 * @param <V> the return type of the transaction
	 * @param dbTransaction the transaction to execute
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception using the transaction
	 * @see DBTransaction
	 */
	<V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction, NoAvailableDatabaseException;

	DBTransactionStatement getDBTransactionStatement() throws SQLException;

	void commitTransaction() throws SQLException;

	void rollbackTransaction() throws SQLException;

	<V> IncompleteTransaction<V> doTransactionWithoutCompleting(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction;

	void setQuietExceptionsPreference(boolean b);

	boolean getQuietExceptionsPreference();

	String getSQLForDBQuery(DBQueryable query) throws NoAvailableDatabaseException;

	/**
	 * Retrieve the DBStatement used internally.
	 *
	 * <p>
	 * DBStatement is the internal version of {@code java.sql.Statement}
	 *
	 * <p>
	 * However you will not need a DBStatement to use DBvolution. Your path lies
	 * elsewhere.
	 *
	 * @return the DBStatement to be used: either a new one, or the current
	 * transaction statement.
	 * @throws java.sql.SQLException interacts with the database layer.
	 */
	DBStatement getDBStatement() throws SQLException;

	/**
	 * Enables the printing of all SQL to System.out before the SQL is executed.
	 *
	 * @param b TRUE to print SQL before execution, FALSE otherwise.
	 */
	void setPrintSQLBeforeExecuting(boolean b);

	/**
	 * Called by internal methods that are about to execute SQL so the SQL can be
	 * printed.
	 *
	 * @param sqlString the raw SQL to print
	 */
	void printSQLIfRequested(String sqlString);

	void printSQLIfRequested(String sqlString, PrintStream out);

	/**
	 * Used by DBStatement to release the connection back into the connection
	 * pool.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param connection connection
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	void unusedConnection(DBConnection connection) throws SQLException;

	/**
	 * Removes a connection from the available pool.
	 *
	 * You'll not need to use this unless you're replacing DBvolution's database
	 * connection handling.
	 *
	 * @param connection the JDBC connection to be removed
	 */
	void discardConnection(DBConnection connection);

	/**
	 * Retrieve the Connection used internally.
	 *
	 * <p>
	 * However you will not need a Connection to use DBvolution. Your path lies
	 * elsewhere.
	 *
	 * @return the Connection to be used.
	 * @throws java.sql.SQLException interacts with the database layer
	 * @throws UnableToCreateDatabaseConnectionException thrown when there is an
	 * issue connecting
	 * @throws UnableToFindJDBCDriver may be thrown if the JDBCDriver is not on
	 * the class path. DBvolution includes several JDBCDrivers already but Oracle
	 * and MS SQLserver, in particular, need to be added to the path if you wish
	 * to work with those databases.
	 */
	DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException;

	/**
	 * Indicates whether SQL will be printed before it is executed.
	 *
	 * @return the printSQLBeforeExecuting
	 */
	boolean isPrintSQLBeforeExecuting();

	/**
	 * Returns a DBTable instance for the DBRow example.
	 *
	 * <p>
	 * See {@link DBTable DBTable} for more details.
	 *
	 * <p>
	 * Please be aware that DBtable doesn't assume the example's criteria are
	 * important. Use
	 * {@link DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow) getRowsByExample}
	 * to use the criteria on the DBRow.
	 *
	 * @param <R> the table affected
	 * @param example the example row to use in the query
	 * @return a DBTable instance for the example provided
	 */
	<R extends DBRow> DBTable<R> getDBTable(R example);

	/**
	 *
	 * Inserts DBRows and Lists of DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a List of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	DBActionList insert(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException;

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param row a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	DBActionList insert(DBRow row) throws SQLException;

	/**
	 *
	 * Inserts DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	DBActionList insert(DBRow... listOfRowsToInsert) throws SQLException;

	/**
	 *
	 * Inserts DBRows and Lists of DBRows into the correct tables automatically
	 *
	 * @param listOfRowsToInsert a List of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	DBActionList insertOrUpdate(Collection<? extends DBRow> listOfRowsToInsert) throws SQLException;

	DBActionList insertOrUpdate(DBRow row) throws SQLException;

	DBActionList insertOrUpdate(DBRow... rows) throws SQLException;

	/**
	 * Get The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows using the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> DBReport type
	 * @param report report
	 * @param examples examples
	 * @return A List of instances of the supplied report using the database 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	<A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * @param row the first example
	 * @param rows the examples of the rows required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
	List<DBQueryRow> get(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 *
	 * creates a query and fetches the rows automatically, based on the examples
	 * given
	 *
	 * Will throw a {@link UnexpectedNumberOfRowsException} if the number of rows
	 * found is different from the number expected. See {@link DBQuery#getAllRows(long)
	 * } for further details.
	 *
	 * @param expectedNumberOfRows the number of rows required
	 * @param row the first example of the tables required
	 * @param rows examples of the tables required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException thrown when the retrieved row count
	 * is wrong
	 * @see DBQuery
	 * @see DBQuery#getAllRows(long)
	 */
	List<DBQueryRow> get(Long expectedNumberOfRows, DBRow row, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See {@link DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow, long)}
	 *
	 * @param <R> the table affected
	 * @param expectedNumberOfRows throw an exception and abort if this number is
	 * not matched
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException the exception thrown if the number
	 * of rows is wrong
	 */
	<R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See
	 * {@link nz.co.gregs.dbvolution.DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow)}
	 *
	 * @param <R> the row affected
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 */
	<R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException;

	/**
	 * Get All The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Provides convenient access to the using DBReport with a blank query.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows using the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> the DBReport to be derived using the database data.
	 * @param report the report to be produced
	 * @param examples DBRow subclasses that provide extra criteria
	 * @return A list of the DBreports generated
	 * @throws SQLException database exceptions
	 */
	<A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 *
	 * Deletes Lists of DBRows using the correct tables automatically
	 *
	 * @param list a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	DBActionList delete(Collection<? extends DBRow> list) throws SQLException;

	/**
	 *
	 * Deletes DBRows using the correct tables automatically
	 *
	 * @param rows a list of DBRows
	 * @return a DBActionList of all the actions performed
	 * @throws SQLException database exceptions
	 */
	DBActionList delete(DBRow... rows) throws SQLException;

	DBDatabase copy();

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param example the example rows that are required in the query
	 * @return a DBQuery with the examples as required tables
	 */
	DBQuery getDBQuery(DBRow example);

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.This is
	 * the easiest way to create DBQueries, and indeed queries.
	 *
	 * @return a DBQuery with the examples as required tables
	 */
	DBQuery getDBQuery();

	boolean supportsDifferenceBetweenNullAndEmptyString();

	/**
	 * Performs the transaction on this database.
	 *
	 * <p>
	 * If there is an exception of any kind the transaction is rolled back and no
	 * changes are made.
	 *
	 * <p>
	 * Otherwise the transaction is committed and changes are made permanent
	 *
	 * @param <V> the return type of the transaction
	 * @param dbTransaction the transaction to execute
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception using the transaction
	 * @see DBTransaction
	 */
	<V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction;

	/**
	 *
	 * Convenience method to simplify switching using READONLY to COMMITTED
	 * transaction
	 *
	 * @param <V> the return type of the transaction, can be anything
	 * @param dbTransaction the transaction to execute
	 * @param commit commit=true or rollback=false.
	 * @return the object returned by the transaction
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * an encapsulated exception using the transaction
	 * @see DBTransaction
	 * @see
	 * DBDatabase#doTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 * @see
	 * DBDatabase#doReadOnlyTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 */
	<V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, ExceptionThrownDuringTransaction;

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param example the first example row that is required in the query
	 * @param examples the example rows that are required in the query
	 * @return a DBQuery with the examples as required tables
	 */
	DBQuery getDBQuery(DBRow example, DBRow... examples);

	/**
	 * Creates a new DBQuery object with the examples added as
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[]) required} tables.
	 *
	 * This is the easiest way to create DBQueries, and indeed queries.
	 *
	 * @param examples the example rows that are required in the query
	 * @return a DBQuery with the examples as required tables
	 */
	DBQuery getDBQuery(final Collection<DBRow> examples);

	<K extends DBRow> DBQueryInsert<K> getDBQueryInsert(K mapper);

	void setLastException(Throwable except);

	/**
	 * Creates a table on the database based on the DBRow.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates an actual table on
	 * the database using the default data types supplied by the fields of the
	 * DBRow.
	 *
	 * @param newTableRow the table to create
	 * @throws SQLException database exceptions
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException;

	boolean getPrintSQLBeforeExecuting();

	/**
	 * Creates or updates a table on the database based on the DBRow.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates an actual table on
	 * the database using the default data types supplied by the fields of the
	 * DBRow.
	 *
	 * @param newTableRow the table to create
	 * @throws SQLException database exceptions
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	void createOrUpdateTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException;

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Foreign key constraints are NOT created.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param newTable the table to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException;

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param includeForeignKeyClauses should explicit FK references be created in
	 * the database?
	 * @param newTable the table to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException;

	/**
	 * Creates a table on the database based on the DBRow, and creates the
	 * required database foreign key constraints.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates an actual table on
	 * the database using the default data types supplied by the fields of the
	 * DBRow.
	 *
	 * <p>
	 * DBvolution does not require actual foreign keys constraints to exist in the
	 * database but there are some advantages in terms of data integrity and
	 * schema transparency.
	 *
	 * <p>
	 * Unfortunately there are also problems caused by creating foreign key
	 * constraints: insertion order sensitivity for instance.
	 *
	 * <p>
	 * Personally I prefer the foreign keys to exist, however database constraints
	 * have been described as the "ambulance at the bottom of the cliff" so you
	 * might be better off without them.
	 *
	 * @param newTableRow table
	 * @throws SQLException database exceptions
	 *
	 */
	void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException;

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Foreign key constraints are NOT created.
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param newTables the tables to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	void createTablesNoExceptions(DBRow... newTables);

	/**
	 * Creates tables on the database based on the DBRows.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRows.
	 *
	 * @param includeForeignKeyClauses should explicit FK references be created in
	 * the database?
	 * @param newTables the tables to create
	 * @throws AutoCommitActionDuringTransactionException thrown if this action is
	 * used during a DBTransaction or DBScript
	 */
	void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables);

	/**
	 * Creates tables on the database based on the DBRows, and creates the
	 * required database foreign key constraints.
	 *
	 * <p>
	 * Implemented to facilitate testing, this method creates actual tables on the
	 * database using the default data types supplied by the fields of the DBRow.
	 *
	 * <p>
	 * DBvolution does not require actual foreign keys constraints to exist in the
	 * database but there are some advantages in terms of data integrity and
	 * schema transparency.
	 *
	 * <p>
	 * Unfortunately there are also problems caused by creating foreign key
	 * constraints: insertion order sensitivity for instance.
	 *
	 * <p>
	 * Personally I prefer the foreign keys to exist, however database constraints
	 * have been described as the "ambulance at the bottom of the cliff" so you
	 * might be better off without them.
	 *
	 * @param newTables table
	 *
	 */
	void createTablesWithForeignKeysNoExceptions(DBRow... newTables);

	/**
	 * The worst idea EVAH.
	 *
	 * <p>
	 * Do NOT Use This.
	 *
	 * @param databaseName the database to be permanently and completely
	 * destroyed.
	 * @param doIt don't do it.
	 * @throws AccidentalDroppingOfDatabaseException Terrible!
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * If you're lucky...
	 * @throws java.sql.SQLException database errors
	 */
	void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfDatabaseException, SQLException, ExceptionThrownDuringTransaction;

	/**
	 * The worst idea EVAH.
	 *
	 * <p>
	 * Do NOT Use This.
	 *
	 * @param doIt don't do it.
	 * @throws AccidentalDroppingOfDatabaseException See?
	 * @throws UnableToDropDatabaseException Terrible!
	 * @throws nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction
	 * If You Lucky.
	 */
	void dropDatabase(boolean doIt) throws AccidentalDroppingOfDatabaseException, UnableToDropDatabaseException, SQLException, AutoCommitActionDuringTransactionException, ExceptionThrownDuringTransaction;

	/**
	 * Drops a table from the database.
	 *
	 * <p>
	 * The easy way to drop a table that might not exist. Will still throw a
	 * AutoCommitActionDuringTransactionException if you use it during a
	 * transaction or AccidentalDroppingOfTableException if dropping tables is
	 * being prevented by DBvolution.
	 * <p>
	 * An even worse idea than {@link #dropTable(nz.co.gregs.dbvolution.DBRow)}
	 * <p>
	 * In General NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Seriously NEVER USE THIS METHOD.
	 *
	 * <p>
	 * Your DBA will murder you.
	 *
	 * @param tableRow the database table to drop permanently
	 * @return the actions performed during the action
	 * @throws java.sql.SQLException database errors
	 */
	DBActionList dropTableIfExists(DBRow tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, SQLException;

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See {@link DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow, long)}
	 *
	 * @param <R> the table affected
	 * @param expectedNumberOfRows the number of rows required
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws UnexpectedNumberOfRowsException the exception thrown when the
	 * number of rows is not correct
	 * @throws nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException
	 * thrown if a cluster is unable to service requests.
	 */
	<R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the selected rows as a list
	 *
	 * <p>
	 * See
	 * {@link nz.co.gregs.dbvolution.DBTable#getRowsByExample(nz.co.gregs.dbvolution.DBRow)}
	 *
	 * @param <R> the table affected
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 */
	<R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 * creates a query and fetches the rows automatically, based on the examples
	 * given.
	 *
	 * @param row the first table
	 * @param rows the example rows for the tables required
	 * @return a list of DBQueryRows relating to the selected rows
	 * @throws SQLException database exceptions
	 * @see DBQuery
	 * @see DBQuery#getAllRows()
	 */
	List<DBQueryRow> getByExamples(DBRow row, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException;

	/**
	 * Create a DBRecursiveQuery based on the query and foreign key supplied.
	 *
	 * <p>
	 * DBRecursiveQuery uses the query to create the first rows of the recursive
	 * query. This can be any query and contain any tables. However it must
	 * contain the table T and the foreign key must be a recursive foreign key
	 * (FK) to and using table T.
	 *
	 * <p>
	 * After the priming query has been created the FK supplied will be followed
	 * repeatedly. The FK must be contained in one of the tables of the priming
	 * query and it must reference the same table, that is to say it must be a
	 * recursive foreign key.
	 *
	 * <p>
	 * The FK will be repeatedly followed until the root node is reached (an
	 * ascending query) or the leaf nodes have been reached (a descending query).
	 * A root node is defined as a row with a null value in the FK. A leaf node is
	 * a row that has no FKs referencing it.
	 *
	 * <p>
	 * While it is possible to define a root node in other ways only the above
	 * definition is currently supported.
	 *
	 * @param <T> the DBRow produced by this recursive query
	 * @param query the priming query
	 * @param keyToFollow the FK to follow
	 * @param dbRow required to define T
	 * @return a recursive query
	 * @throws ColumnProvidedMustBeAForeignKey Only FKs please
	 * @throws ForeignKeyDoesNotReferenceATableInTheQuery Only FKs actually in the
	 * query please
	 * @throws ForeignKeyIsNotRecursiveException the FK must be in _and_ reference
	 * T
	 */
	<T extends DBRow> DBRecursiveQuery<T> getDBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow, T dbRow);

	/**
	 *
	 * Updates Lists of DBRows in the correct tables automatically.
	 *
	 * Updated rows are marked as updated, and can be used as though they have
	 * been freshly retrieved from the database.
	 *
	 * @param listOfRowsToUpdate a List of DBRows
	 * @return a DBActionList of the actions performed on the database
	 * @throws SQLException database exceptions
	 */
	DBActionList update(Collection<? extends DBRow> listOfRowsToUpdate) throws SQLException;

	/**
	 *
	 * Updates DBRows and Lists of DBRows in the correct tables automatically.
	 *
	 * Updated rows are marked as updated, and can be used as though they have
	 * been freshly retrieved from the database.
	 *
	 * @param rows a list of DBRows
	 * @return a DBActionList of the actions performed on the database
	 * @throws SQLException database exceptions
	 */
	DBActionList update(DBRow... rows) throws SQLException;

	/**
	 * Convenience method to implement a DBScript on this database
	 *
	 * equivalent to script.implement(this);
	 *
	 * @param script the script to execute and commit
	 * @return a DBActionList provided by the script
	 * @throws Exception any exception can be thrown by a DBScript
	 */
	DBActionList implement(DBScript script) throws Exception;

	Instant getCurrentInstant() throws UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, SQLException;

	/**
	 *
	 * Convenience method to print the rows using get(DBRow...)
	 *
	 * @param rows lists of DBRows, DBReports, or DBQueryRows
	 */
	default void print(List<?> rows) {
		if (rows != null) {
			for (Object row : rows) {
				if (row != null) {
					System.out.println(row.toString());
				} else {
					System.out.println("null");
				}
			}
		}
	}

	LocalDateTime getCurrentLocalDatetime() throws SQLException;

	/**
	 * Creates a DBmigration that will do a conversion from one or more database
	 * tables to another database table.
	 *
	 * <p>
	 * The mapper class should be an extension of a intended DBRow class that has
	 * instances of the DBRows required in the query, constraints, and mappings to
	 * fill the required columns set in the initialization clause. </p>
	 *
	 * <p>
	 * See DBMigrationTest for examples.</p>
	 *
	 * @param <MAPPER> the DBRow extension that maps fields of internal DBRows to
	 * all the fields of it's superclass.
	 * @param mapper a class that can be used to map one or more database tables
	 * to a single table.
	 * @return a DBQueryInsert for the mapper class
	 */
	default <MAPPER extends DBRow> DBMigration<MAPPER> getDBMigration(MAPPER mapper) {
		return DBMigration.using(this, mapper);
	}

	/**
	 * Get The Rows For The Supplied DBReport Constrained By The Examples.
	 *
	 * <p>
	 * Calls the
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) DBReport getRows method}.
	 *
	 * Retrieves a list of report rows using the database using the constraints
	 * supplied by the report and the examples supplied.
	 *
	 * @param <A> DBReport type
	 * @param report report
	 * @param examples examples
	 * @return A List of instances of the supplied report using the database 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	default <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		return DBReport.getRows(this, report, examples);
	}

	/**
	 *
	 * Automatically selects the correct table based on the example supplied and
	 * returns the number of rows found based on the example.
	 *
	 * <p>
	 * See {@link nz.co.gregs.dbvolution.DBTable#count()}
	 *
	 * @param <R> the row affected
	 * @param exampleRow the example
	 * @return a list of the selected rows
	 * @throws SQLException database exceptions
	 * @throws AccidentalCartesianJoinException Thrown when a query will create a
	 * Cartesian Join and cartesian joins have not been explicitly permitted.
	 */
	default <R extends DBRow> long getCount(R exampleRow) throws SQLException, AccidentalCartesianJoinException {
		DBTable<R> dbTable = getDBTable(exampleRow).setBlankQueryAllowed(true);
		return dbTable.count();
	}

	void setPreventAccidentalDeletingAllRowsFromTable(boolean b);

	public DBDatabaseMetaData getDBDatabaseMetaData(Options options) throws SQLException;

	public SettingsBuilder<?,?> getSettingsBuilder();

}
