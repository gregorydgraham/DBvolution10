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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.databases.definitions.ClusterDatabaseDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.reflection.DataModel;
import nz.co.gregs.dbvolution.transactions.DBTransaction;

/**
 * Creates a database cluster programmatically.
 *
 * <p>
 * Clustering provides several benefits: automatic replication, reduced server
 * load on individual servers, improved server failure tolerance, and, with a
 * little programming, dynamic server replacement.</p>
 *
 * <p>
 * Please note that this class is not required to use database clusters provided
 * by database vendors. Use the normal DBDatabase subclass for those
 * vendors.</p>
 *
 * <p>
 * DBDatabaseCluster collects together several databases and ensures that all
 * actions are performed on all databases. This ensures that all databases stay
 * in synch and allows queries to be distributed to any database and produce the
 * same results. Different databases can be any supported database, for instance
 * the DBvolutionDemo application uses H2 and SQLite.</p>
 *
 * <p>
 * Upon creation, known tables and data are synchronized, the first database in
 * the cluster being used as the template. Added databases are synchronized
 * before being used</p>
 *
 * <p>
 * Automatically generated keys are still supported with a slight change: the
 * key will be generated in the first database and used as a literal value in
 * all other databases.
 *
 * @author gregorygraham
 */
public class DBDatabaseCluster extends DBDatabase {

	private static final long serialVersionUID = 1l;

	private final List<DBDatabase> allDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> addedDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private final List<DBDatabase> readyDatabases = Collections.synchronizedList(new ArrayList<DBDatabase>(0));
	private transient final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
	private transient final DBStatementCluster clusterStatement;
	private transient final Map<DBDatabase, Queue<DBAction>> queuedActions = Collections.synchronizedMap(new HashMap<DBDatabase, Queue<DBAction>>(0));
	private transient final ExecutorService threadPool = Executors.newCachedThreadPool();

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
	 * @return <tt>true</tt> if the database has been added to the cluster.
	 * @throws java.sql.SQLException
	 * @throws UnsupportedOperationException if the <tt>add</tt> operation is not
	 * supported by this list
	 * @throws ClassCastException if the class of the specified element prevents
	 * it from being added to this list
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * @throws IllegalArgumentException if some property of this element prevents
	 * it from being added to this list
	 */
	public synchronized boolean addDatabase(DBDatabase database) throws SQLException {
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
	public synchronized boolean removeDatabases(List<DBDatabase> databases) {
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
	public synchronized boolean removeDatabases(DBDatabase... databases) {
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
	public synchronized DBDatabase getReadyDatabase() {
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
	public synchronized void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		for (DBDatabase next : readyDatabases) {
			next.preventDroppingOfDatabases(justLeaveThisAtTrue);
		}
	}

	@Override
	public synchronized void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		for (DBDatabase next : readyDatabases) {
			next.preventDroppingOfTables(droppingTablesIsAMistake);
		}
	}

	@Override
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		for (DBDatabase next : readyDatabases) {
			next.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		}
	}

	@Override
	public synchronized boolean batchSQLStatementsWhenPossible() {
		boolean result = true;
		for (DBDatabase next : readyDatabases) {
			result &= next.batchSQLStatementsWhenPossible();
		}
		return result;
	}

	@Override
	public synchronized void dropDatabase(String databaseName, boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.dropDatabase(databaseName, doIt);
		}
	}

	@Override
	public synchronized void dropDatabase(boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.dropDatabase(doIt);
		}
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) {
		return getReadyDatabase().willCreateBlankQuery(row);
	}

	@Override
	public synchronized <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.dropTableNoExceptions(tableRow);
		}
	}

	@Override
	public synchronized void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		for (DBDatabase next : readyDatabases) {
			next.dropTable(tableRow);
		}
	}

	@Override
	public synchronized void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		for (DBDatabase next : readyDatabases) {
			next.createIndexesOnAllFields(newTableRow);
		}
	}

	@Override
	public synchronized void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		for (DBDatabase next : readyDatabases) {
			next.removeForeignKeyConstraints(newTableRow);
		}
	}

	@Override
	public synchronized void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		for (DBDatabase next : readyDatabases) {
			next.createForeignKeyConstraints(newTableRow);
		}
	}

	@Override
	public synchronized void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTableWithForeignKeys(newTableRow);
		}
	}

	@Override
	public synchronized void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTable(newTableRow);
		}
	}

	@Override
	public synchronized void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		for (DBDatabase next : readyDatabases) {
			next.createTablesWithForeignKeysNoExceptions(newTables);
		}
	}

	@Override
	public synchronized void createTablesNoExceptions(DBRow... newTables) {
		for (DBDatabase next : readyDatabases) {
			next.createTablesNoExceptions(newTables);
		}
	}

	@Override
	public synchronized void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		for (DBDatabase next : readyDatabases) {
			next.createTablesNoExceptions(includeForeignKeyClauses, newTables);
		}
	}

	@Override
	public synchronized void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTableNoExceptions(newTable);
		}
	}

	@Override
	public synchronized void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		for (DBDatabase next : readyDatabases) {
			next.createTableNoExceptions(includeForeignKeyClauses, newTable);
		}
	}

	@Override
	public DBActionList test(DBScript script) throws Exception {
		return getReadyDatabase().test(script);
	}

	@Override
	public synchronized DBActionList implement(DBScript script) throws Exception {
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
	public synchronized <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, Exception {
		V result = null;
		boolean rollbackAll = false;
		List<DBDatabase> transactionDatabases = new ArrayList<>();
		try {
			for (DBDatabase database : readyDatabases) {
				DBDatabase db;
//				synchronized (database) {
				db = database.clone();
//				}
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

	private synchronized void commitAll() throws SQLException {
		for (DBDatabase db : readyDatabases) {
			db.doCommit();
		}
	}

	private synchronized void rollbackAll(Exception exc) throws SQLException {
		for (DBDatabase db : readyDatabases) {
			db.doRollback();
		}
	}

	@Override
	public synchronized DBActionList executeDBAction(DBAction action) throws SQLException {
		addActionToQueue(action);
		List<ActionTask> tasks = new ArrayList<ActionTask>();
		DBActionList actionsPerformed;
		for (DBDatabase next : readyDatabases) {
			tasks.add(new ActionTask(next, action));
			removeActionFromQueue(next, action);
		}
		try {
			threadPool.invokeAll(tasks);
		} catch (InterruptedException ex) {
			Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
			throw new DBRuntimeException("Unable To Run Actions", ex);
		}
		actionsPerformed = tasks.get(0).getActionList();
		return actionsPerformed;
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException {
		final DBDatabase readyDatabase = getReadyDatabase();
		DBQueryable actionsPerformed = readyDatabase.executeDBQuery(query);
		return actionsPerformed;
	}

	@Override
	public String getSQLForDBQuery(DBQueryable query) {
		return this.getReadyDatabase().getSQLForDBQuery(query);
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

	public synchronized DBDatabase getPrimaryDatabase() {
		if (readyDatabases.size() > 0) {
			return readyDatabases.get(0);
		} else {
			return allDatabases.get(0);
		}
	}

	@Override
	public synchronized void setPrintSQLBeforeExecuting(boolean b) {
		for (DBDatabase db : allDatabases) {
			db.setPrintSQLBeforeExecuting(b);
		}
	}

	private synchronized void addActionToQueue(DBAction action) {
		for (DBDatabase db : allDatabases) {
			queuedActions.get(db).add(action);
		}
	}

	private synchronized void removeActionFromQueue(DBDatabase database, DBAction action) {
		final Queue<DBAction> db = queuedActions.get(database);
		if (db != null) {
			db.remove(action);
		}
	}

	private synchronized void synchronizeSecondaryDatabase(DBDatabase secondary) throws SQLException {

		// Get some sort of lock
		DBDatabase primary = getPrimaryDatabase();
		synchronized (this) {
			// Create a action queue for the new database
			queuedActions.put(secondary, new LinkedBlockingQueue<DBAction>());
			// Check that we're not synchronising the reference database
			if (!primary.equals(secondary)) {
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
		Queue<DBAction> queue = queuedActions.get(db);
		while (!queue.isEmpty()) {
			DBAction action = queue.remove();
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
			final boolean tableExists1 = readyDatabase.tableExists(table);
			tableExists &= tableExists1;
		}
		return tableExists;
	}

	private static class ActionTask implements Callable<DBActionList> {

		private final DBDatabase database;
		private final DBAction action;
		private final DBActionList actionList = new DBActionList();

		public ActionTask(DBDatabase db, DBAction action) {
			this.database = db;
			this.action = action;
		}

		@Override
		public synchronized DBActionList call() throws Exception {
			actionList.clear();
			actionList.addAll(database.executeDBAction(action));
			return actionList;
		}

		public synchronized DBActionList getActionList() {
			final DBActionList newList = new DBActionList();
			newList.addAll(actionList);
			return newList;
		}
	}

}
