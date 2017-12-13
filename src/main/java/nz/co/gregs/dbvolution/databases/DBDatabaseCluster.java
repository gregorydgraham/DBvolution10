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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBExecutable;
import nz.co.gregs.dbvolution.databases.definitions.ClusterDatabaseDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
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

	public DBDatabaseCluster(DBDatabase... databases) throws SQLException {
		super();
		setDefinition(new ClusterDatabaseDefinition());
		clusterStatement = new DBStatementCluster(this);
		this.addedDatabases.addAll(Arrays.asList(databases));
		this.allDatabases.addAll(Arrays.asList(databases));
		synchroniseAddedDatabases();
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
	public boolean addDatabase(DBDatabase database) {
		addedDatabases.add(database);
		synchroniseAddedDatabases();
		return allDatabases.add(database);
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
	 * @param database DBDatabase to be removed from this list, if present
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
	public boolean removeDatabases(DBDatabase database) {
		return allDatabases.remove(database);
	}

	/**
	 * Returns a single random database that is ready for queries
	 *
	 * @return a ready database
	 */
	private DBDatabase getReadyDatabase() {
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
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.preventDroppingOfDatabases(justLeaveThisAtTrue);
		}
	}

	@Override
	public void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.preventDroppingOfTables(droppingTablesIsAMistake);
		}
	}

	@Override
	public void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		}
	}

	@Override
	public boolean batchSQLStatementsWhenPossible() {
		boolean result = true;
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			result &= next.batchSQLStatementsWhenPossible();
		}
		return result;
	}

	@Override
	public void dropDatabase(String databaseName, boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.dropDatabase(databaseName, doIt);
		}
	}

	@Override
	public void dropDatabase(boolean doIt) throws Exception, UnsupportedOperationException, AutoCommitActionDuringTransactionException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.dropDatabase(doIt);
		}
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) {
		return getReadyDatabase().willCreateBlankQuery(row);
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.dropTableNoExceptions(tableRow);
		}
	}

	@Override
	public void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.dropTable(tableRow);
		}
	}

	@Override
	public void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.createIndexesOnAllFields(newTableRow);
		}
	}

	@Override
	public void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
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
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.createTableWithForeignKeys(newTableRow);
		}
	}

	@Override
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.createTable(newTableRow);
		}
	}

	@Override
	public void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.createTablesWithForeignKeysNoExceptions(newTables);
		}
	}

	@Override
	public void createTablesNoExceptions(DBRow... newTables) {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.createTablesNoExceptions(newTables);
		}
	}

	@Override
	public void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.createTablesNoExceptions(includeForeignKeyClauses, newTables);
		}
	}

	@Override
	public void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.createTableNoExceptions(newTable);
		}
	}

	@Override
	public void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
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
			for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
				DBDatabase next = iterator.next();
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

//	@Override
//	public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
//		V result = null;
//		try {
//			for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
//				DBDatabase next = iterator.next();
//				result = next.doTransaction(dbTransaction);
//			}
//			commitAll();
//		} catch (Exception exc) {
//			rollbackAll(exc);
//		}
//		return result;
//	}

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, Exception {
		V result = null;
		boolean rollbackAll = false;
		List<DBDatabase> transactionDatabases = new ArrayList<>();
		try {
			for (DBDatabase database : readyDatabases) {
//				result = next.doTransaction(dbTransaction, commit);
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

	private void synchroniseAddedDatabases() {
		DBDatabase[] addedDBs;
		synchronized (addedDatabases) {
			addedDBs = addedDatabases.toArray(new DBDatabase[]{});
		}
		for (DBDatabase db : addedDBs) {
			addedDatabases.remove(db);
			//Do The Synchronising...

			db.setExplicitCommitAction(true);
			//Mark the database as ready
			synchronized (readyDatabases) {
				readyDatabases.add(db);
			}
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
		DBActionList actionsPerformed = new DBActionList();
		for (DBDatabase next : readyDatabases) {
			actionsPerformed = action.execute(next);
		}
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
		return readyDatabases.get(0);
	}

	@Override
	public void setPrintSQLBeforeExecuting(boolean b) {
		for (DBDatabase db : allDatabases) {
			db.setPrintSQLBeforeExecuting(b);
		}
	}
}
