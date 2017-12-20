/*
 * Copyright 2017 gregorygraham.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBExecutable;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.databases.definitions.ClusterDatabaseDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.reflection.DataModel;
import nz.co.gregs.dbvolution.transactions.DBTransaction;

/**
 *
 * @author gregorygraham
 */
public class DBDatabaseCluster extends DBDatabase {

	private static final long serialVersionUID = 1l;

	private final List<DBDatabase> allDatabases = new ArrayList<>();
	private final List<DBDatabase> addedDatabases = new ArrayList<>();
	private final List<DBDatabase> readyDatabases = new ArrayList<>();
	private final DBStatementCluster clusterStatement;
	private final Map<DBDatabase, Queue<DBExecutable>> queuedActions = new HashMap<>(0);
	private Set<DBRow> requiredTables;

	public DBDatabaseCluster(DBDatabase... databases) throws SQLException {
		super();
		setDefinition(new ClusterDatabaseDefinition());
		clusterStatement = new DBStatementCluster(this);
		this.addedDatabases.addAll(Arrays.asList(databases));
		this.allDatabases.addAll(Arrays.asList(databases));
		synchronizeSecondaryDatabases();
	}

	/**
	 * Appends the specified element to the end of this list (optional operation).
	 *
	 * <p>
	 * Lists that support this operation may place limitations on what elements
	 * may be added to this list. In particular, some lists will refuse to add
	 * null elements, and others will impose restrictions on the type of elements
	 * that may be added. List classes should clearly specify in their
	 * documentation any restrictions on what elements may be added.
	 *
	 * @param database element to be appended to this list
	 * @return <tt>true</tt> (as specified by {@link Collection#add})
	 * @throws UnsupportedOperationException if the <tt>add</tt> operation is not
	 * supported by this list
	 * @throws ClassCastException if the class of the specified element prevents
	 * it from being added to this list
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * @throws IllegalArgumentException if some property of this element prevents
	 * it from being added to this list
	 */
	public boolean addDatabase(DBDatabase database) throws SQLException {
		addedDatabases.add(database);
		boolean add = allDatabases.add(database);
		synchronizeAddedDatabases();
		return add;
	}

	public synchronized DBDatabase[] getDatabases() {
		return readyDatabases.toArray(new DBDatabase[]{});
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present (optional operation). If this list does not contain the element,
	 * it is unchanged. More formally, removes the element with the lowest index
	 * <tt>i</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
	 * (if such an element exists). Returns <tt>true</tt> if this list contained
	 * the specified element (or equivalently, if this list changed as a result of
	 * the call).
	 *
	 * @param databases DBDatabases to be removed from this list, if present
	 * @return <tt>true</tt> if this list contained the specified element
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the <tt>remove</tt> operation is
	 * not supported by this list
	 */
	public boolean removeDatabases(List<DBDatabase> databases) {
		return removeDatabases(databases.toArray(new DBDatabase[]{}));
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present (optional operation). If this list does not contain the element,
	 * it is unchanged. More formally, removes the element with the lowest index
	 * <tt>i</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
	 * (if such an element exists). Returns <tt>true</tt> if this list contained
	 * the specified element (or equivalently, if this list changed as a result of
	 * the call).
	 *
	 * @param databases DBDatabases to be removed from this list, if present
	 * @return <tt>true</tt> if this list contained the specified element
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the <tt>remove</tt> operation is
	 * not supported by this list
	 */
	public boolean removeDatabases(DBDatabase... databases) {
		for (DBDatabase database : databases) {
			queuedActions.remove(database);
			allDatabases.remove(database);
		}
		return true;
	}

	/**
	 * Returns a single random database that is ready for queries
	 *
	 * @return a ready database
	 */
	public DBDatabase getReadyDatabase() {
		Random rand = new Random();
		DBDatabase randomElement = readyDatabases.get(rand.nextInt(readyDatabases.size()));
		return randomElement;
	}

	@Override
	public void addFeatureToFixException(Exception exp) throws Exception {
		throw new UnsupportedOperationException("DBDatabase.addFeatureToFixException(Exception) should not be called");
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		throw new UnsupportedOperationException("DBDatabase.addDatabaseSpecificFeatures(Statement) should not be called");
	}

	@Override
	public synchronized void discardConnection(Connection connection) {
		throw new UnsupportedOperationException("DBDatabase.discardConnection() should not be called");
	}

	@Override
	public synchronized void unusedConnection(Connection connection) throws SQLException {
		throw new UnsupportedOperationException("DBDatabase.unusedConnection() should not be called");
	}

	@Override
	public Connection getConnectionFromDriverManager() throws SQLException {
		throw new UnsupportedOperationException("DBDatabase.getConnectionFromDriverManager() should not be called");
	}

	@Override
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException {
		return getReadyDatabase().getRows(report, examples);
	}

	@Override
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException {
		return getReadyDatabase().getAllRows(report, examples);
	}

	@Override
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException {
		return getReadyDatabase().get(report, examples);
	}

	@Override
	public void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		for (DBDatabase next : readyDatabases) {
			next.preventDroppingOfDatabases(justLeaveThisAtTrue);
		}
	}

	@Override
	public void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		for (DBDatabase next : readyDatabases) {
			next.preventDroppingOfTables(droppingTablesIsAMistake);
		}
	}

	@Override
	public void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		for (DBDatabase next : readyDatabases) {
			next.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		}
	}

	@Override
	public boolean batchSQLStatementsWhenPossible() {
		boolean result = true;
		for (DBDatabase next : readyDatabases) {
			result &= next.batchSQLStatementsWhenPossible();
		}
		return result;
	}

	@Override
	public void dropDatabase(String databaseName, boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.dropDatabase(databaseName, doIt);
		}
	}

	@Override
	public void dropDatabase(boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.dropDatabase(doIt);
		}
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) {
		return getReadyDatabase().willCreateBlankQuery(row);
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.dropTableNoExceptions(tableRow);
		}
	}

	@Override
	public void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		for (DBDatabase next : readyDatabases) {
			next.dropTable(tableRow);
		}
	}

	@Override
	public void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		for (DBDatabase next : readyDatabases) {
			next.createIndexesOnAllFields(newTableRow);
		}
	}

	@Override
	public void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		for (DBDatabase next : readyDatabases) {
			next.removeForeignKeyConstraints(newTableRow);
		}
	}

	@Override
	public void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		for (DBDatabase next : readyDatabases) {
			next.createForeignKeyConstraints(newTableRow);
		}
	}

	@Override
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTableWithForeignKeys(newTableRow);
		}
	}

	@Override
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTable(newTableRow);
		}
	}

	@Override
	public void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		for (DBDatabase next : readyDatabases) {
			next.createTablesWithForeignKeysNoExceptions(newTables);
		}
	}

	@Override
	public void createTablesNoExceptions(DBRow... newTables) {
		for (DBDatabase next : readyDatabases) {
			next.createTablesNoExceptions(newTables);
		}
	}

	@Override
	public void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		for (DBDatabase next : readyDatabases) {
			next.createTablesNoExceptions(includeForeignKeyClauses, newTables);
		}
	}

	@Override
	public void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTableNoExceptions(newTable);
		}
	}

	@Override
	public void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTableNoExceptions(includeForeignKeyClauses, newTable);
		}
	}

	@Override
	public DBActionList test(DBScript script) throws Exception {
		return getReadyDatabase().test(script);
	}

	@Override
	public DBActionList implement(DBScript script) throws Exception {
		DBActionList actions = new DBActionList();
		try {
			for (DBDatabase next : readyDatabases) {
				actions = next.implement(script);
			}
			commitAll();
		} catch (Exception exc) {
			rollbackAll(exc);
		}
		return actions;
	}

	@Override
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
		return getReadyDatabase().doReadOnlyTransaction(dbTransaction);
	}

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, Exception {
		V result = null;
		boolean rollbackAll = false;
		List<DBDatabase> transactionDatabases = new ArrayList<>();
		try {
			for (DBDatabase database : readyDatabases) {
				DBDatabase db;
				synchronized (database) {
					db = database.clone();
				}
				transactionDatabases.add(db);
				V returnValues = null;
				db.transactionStatement = db.getDBTransactionStatement();
				try {
					db.isInATransaction = true;
					db.transactionConnection = db.transactionStatement.getConnection();
					db.transactionConnection.setAutoCommit(false);
					try {
						returnValues = dbTransaction.doTransaction(db);
						if (!commit) {
							try {
								db.transactionConnection.rollback();
								LOG.info("Transaction Successful: ROLLBACK Performed");
							} catch (SQLException rollbackFailed) {
								LOG.warn("ROLLBACK FAILED: CONTINUING REGARDLESS: " + rollbackFailed.getLocalizedMessage());
								discardConnection(db.transactionConnection);
							}
						}
					} catch (Exception ex) {
						try {
							LOG.warn("Exception Occurred: Attempting ROLLBACK - " + ex.getMessage(), ex);
							if (!explicitCommitActionRequired) {
								db.transactionConnection.rollback();
								LOG.warn("Exception Occurred: ROLLBACK Succeeded!");
							}
						} catch (SQLException excp) {
							LOG.warn("Exception Occurred During Rollback: " + ex.getMessage(), excp);
						}
						throw ex;
					}
				} finally {
				}
				result = returnValues;

			}
		} catch (Exception exc) {
			rollbackAll = true;
		} finally {
			for (DBDatabase db : transactionDatabases) {
				if (commit) {
					if (rollbackAll) {
						db.transactionConnection.rollback();
					} else {
						db.transactionConnection.commit();
					}
				}
				db.isInATransaction = false;
				db.transactionStatement.transactionFinished();
				db.discardConnection(db.transactionConnection);
				db.transactionConnection = null;
				db.transactionStatement = null;
			}
		}
		return result;
	}

	@Override
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException {
		return getReadyDatabase().get(expectedNumberOfRows, rows);
	}

	@Override
	public List<DBQueryRow> getByExamples(DBRow... rows) throws SQLException {
		return getReadyDatabase().getByExamples(rows);
	}

	@Override
	public List<DBQueryRow> get(DBRow... rows) throws SQLException {
		return getReadyDatabase().get(rows);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException {
		return getReadyDatabase().getByExample(expectedNumberOfRows, exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException {
		return getReadyDatabase().get(expectedNumberOfRows, exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException {
		return getReadyDatabase().getByExample(exampleRow);
	}

	@Override
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException {
		return getReadyDatabase().get(exampleRow);
	}

	@Override
	public Connection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		throw new UnsupportedOperationException("DBDatabase.getConnection should not be used.");
	}

	@Override
	protected DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return clusterStatement;
	}

	@Override
	public synchronized boolean equals(Object obj) {
		if (obj instanceof DBDatabaseCluster) {
			DBDatabaseCluster otherDB = (DBDatabaseCluster) obj;
			return allDatabases.equals(otherDB.allDatabases)
					&& addedDatabases.equals(otherDB.addedDatabases)
					&& readyDatabases.equals(otherDB.readyDatabases);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	private void synchronizeSecondaryDatabases() throws SQLException {
		DBDatabase[] addedDBs;
		synchronized (addedDatabases) {
			addedDBs = addedDatabases.toArray(new DBDatabase[]{});
		}
		for (DBDatabase db : addedDBs) {
			addedDatabases.remove(db);

			//Do The Synchronising...
			synchronizeSecondaryDatabase(db);
		}
	}

	private void commitAll() throws SQLException {
		for (DBDatabase db : readyDatabases) {
			db.doCommit();
		}
	}

	private void rollbackAll(Exception exc) throws SQLException {
		for (DBDatabase db : readyDatabases) {
			db.doRollback();
		}
	}

	@Override
	public DBActionList executeDBAction(DBExecutable action) throws SQLException {
		addActionToQueue(action);
		DBActionList actionsPerformed = new DBActionList();
		for (DBDatabase next : readyDatabases) {
			actionsPerformed = next.executeDBAction(action);
			removeActionFromQueue(next, action);
		}
		return actionsPerformed;
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException {
		DBQueryable actionsPerformed = this.getReadyDatabase().executeDBQuery(query);
		return actionsPerformed;
	}

	synchronized ArrayList<DBStatement> getDBStatements() throws SQLException {
		ArrayList<DBStatement> arrayList = new ArrayList<>();
		for (DBDatabase db : readyDatabases) {
			arrayList.add(db.getDBStatement());
		}
		return arrayList;
	}

	@Override
	public DBDefinition getDefinition() {
		return getReadyDatabase().getDefinition();
	}

	public DBDatabase getPrimaryDatabase() {
		if (readyDatabases.size() > 0) {
			return readyDatabases.get(0);
		} else {
			return allDatabases.get(0);
		}
	}

	@Override
	public void setPrintSQLBeforeExecuting(boolean b) {
		for (DBDatabase db : allDatabases) {
			db.setPrintSQLBeforeExecuting(b);
		}
	}

	private synchronized void addActionToQueue(DBExecutable action) {
		for (DBDatabase db : allDatabases) {
			queuedActions.get(db).add(action);
		}
	}

	private synchronized void removeActionFromQueue(DBDatabase database, DBExecutable action) {
		try {
			final Queue<DBExecutable> db = queuedActions.get(database);
			if (db != null) {
				db.remove();
			}
		} catch (Exception exc) {
			LOG.info("DBDatabaseCluster", exc);
		}
	}

	private void synchronizeSecondaryDatabase(DBDatabase secondary) throws SQLException {

		// Get some sort of lock
		DBDatabase primary = getPrimaryDatabase();
		synchronized (this) {
			// Create a action queue for the new database
			queuedActions.put(secondary, new LinkedBlockingQueue<DBExecutable>());
			// Check that we're not synchronising the reference database
			if (!primary.equals(secondary)) {
				if (requiredTables == null) {
					requiredTables = DataModel.getRequiredTables();
				}
				for (DBRow table : requiredTables) {
					if (true) {
						if (primary.tableExists(table)) {
							final DBTable<DBRow> primaryTable = primary.getDBTable(table);
							final DBTable<DBRow> secondaryTable = secondary.getDBTable(table);
							final Long primaryTableCount = primaryTable.count();
							final Long secondaryTableCount = secondaryTable.count();
							if (primaryTableCount > 0) {
								final DBTable<DBRow> primaryData = primaryTable.setBlankQueryAllowed(true);
								if (secondaryTableCount == 0) {
									List<DBRow> allRows = primaryData.getAllRows();
									secondaryTable.insert(allRows);
								} else if (!secondaryTableCount.equals(primaryTableCount)) {
									secondary.delete(secondaryTable.setBlankQueryAllowed(true).getAllRows());
									List<DBRow> allRows = primaryData.getAllRows();
									secondary.insert(allRows);
								}
							}
						}
					}
				}
			}
		}
		synchronizeActions(secondary);
		secondary.setExplicitCommitAction(true);
		readyDatabases.add(secondary);
	}

	private synchronized void synchronizeActions(DBDatabase db) throws SQLException {
		Queue<DBExecutable> queue = queuedActions.get(db);
		while (!queue.isEmpty()) {
			DBExecutable action = queue.remove();
			db.executeDBAction(action);
		}
	}

	private synchronized void synchronizeAddedDatabases() throws SQLException {
		for (DBDatabase addedDatabase : addedDatabases) {
			synchronizeSecondaryDatabase(addedDatabase);
		}
	}

	@Override
	public synchronized boolean tableExists(DBRow table) throws SQLException {
		boolean tableExists = true;
		for (DBDatabase readyDatabase : readyDatabases) {
			tableExists &= readyDatabase.tableExists(table);
		}
		return tableExists;
	}

}
