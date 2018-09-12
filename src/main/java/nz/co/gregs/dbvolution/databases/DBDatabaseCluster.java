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

import java.lang.reflect.InvocationTargetException;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
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

	protected final ClusterDetails details;
	private transient final ExecutorService threadPool;
	private final transient DBStatementCluster clusterStatement;

	public DBDatabaseCluster() {
		super();
		clusterStatement = new DBStatementCluster(this);
		details = new ClusterDetails();
		threadPool = Executors.newCachedThreadPool();
	}

	public DBDatabaseCluster(DBDatabase... databases) throws SQLException {
		this();
		details.addAll(databases);
		setDefinition(new ClusterDatabaseDefinition());
		synchronizeSecondaryDatabases();
	}

	public DBDatabaseCluster(DatabaseConnectionSettings... settings) throws SQLException, InvocationTargetException, IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, NoSuchMethodException, ClassNotFoundException {
		this();
		setDefinition(new ClusterDatabaseDefinition());
		for (DatabaseConnectionSettings setting : settings) {
			this.addDatabase(setting.createDBDatabase());
		}
	}

	public synchronized DBStatement getClusterStatement() {
		return clusterStatement;
	}

	/**
	 * Adds a new database to this cluster.
	 *
	 * <p>
	 * The database will be synchronized and then made available for use.</p>
	 * <p>
	 * This is the non-blocking version of {@link #addDatabaseAndWait(nz.co.gregs.dbvolution.databases.DBDatabase)
	 * }.</p>
	 *
	 * @param database element to be appended to this list
	 * @return <tt>true</tt> if the database has been added to the cluster.
	 * @throws java.sql.SQLException
	 */
	public final synchronized boolean addDatabase(DBDatabase database) throws SQLException {
		boolean add = details.add(database);
		synchronizeAddedDatabases(false);
		return add;
	}

	/**
	 * Adds a new database to this cluster.
	 *
	 * <p>
	 * The database will be synchronized and then made available for use.</p>
	 *
	 * <p>
	 * This is the blocking version of {@link #addDatabase(nz.co.gregs.dbvolution.databases.DBDatabase)
	 * }</p>
	 *
	 * @param database element to be appended to this list
	 * @return <tt>true</tt> if the database has been added to the cluster.
	 * @throws java.sql.SQLException
	 */
	public synchronized boolean addDatabaseAndWait(DBDatabase database) throws SQLException {
		boolean add = details.add(database);
		synchronizeAddedDatabases(true);
		return add;
	}

	public synchronized DBDatabase[] getDatabases() {
		return details.getAllDatabases();
	}

	public Status getDatabaseStatus(DBDatabase db) {
		return details.getStatusOf(db);
	}

	/**
	 * Adds the database to the cluster, synchronizes it, and then removes it.
	 *
	 * @param backupDatabase
	 * @throws SQLException
	 * @throws UnableToRemoveLastDatabaseFromClusterException
	 */
	public void backupToDBDatabase(DBDatabase backupDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		this.addDatabaseAndWait(backupDatabase);
		removeDatabase(backupDatabase);
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
	 * @throws UnableToRemoveLastDatabaseFromClusterException
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the <tt>quarantineDatabase</tt>
	 * operation is not supported by this list
	 */
	public synchronized boolean removeDatabases(List<DBDatabase> databases) throws UnableToRemoveLastDatabaseFromClusterException {
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
	 * @throws UnableToRemoveLastDatabaseFromClusterException
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the <tt>quarantineDatabase</tt>
	 * operation is not supported by this list
	 */
	public synchronized boolean removeDatabases(DBDatabase... databases) throws UnableToRemoveLastDatabaseFromClusterException {
		for (DBDatabase database : databases) {
			removeDatabase(database);
		}
		return true;
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
	 * @throws UnableToRemoveLastDatabaseFromClusterException
	 * @throws ClassCastException if the type of the specified element is
	 * incompatible with this list
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws NullPointerException if the specified element is null and this list
	 * does not permit null elements
	 * (<a href="Collection.html#optional-restrictions">optional</a>)
	 * @throws UnsupportedOperationException if the <tt>quarantineDatabase</tt>
	 * operation is not supported by this list
	 */
	public boolean removeDatabase(DBDatabase database) throws UnableToRemoveLastDatabaseFromClusterException {
		return details.quarantineDatabase(database);
	}

	/**
	 * Returns a single random database that is ready for queries
	 *
	 * @return a ready database
	 */
	public DBDatabase getReadyDatabase() throws NoAvailableDatabaseException {
		return details.getReadyDatabase();
	}

	@Override
	public ResponseToException addFeatureToFixException(Exception exp) throws Exception {
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
	public synchronized void preventDroppingOfDatabases(boolean justLeaveThisAtTrue) {
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.preventDroppingOfDatabases(justLeaveThisAtTrue);
		}
	}

	@Override
	public synchronized void preventDroppingOfTables(boolean droppingTablesIsAMistake) {
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.preventDroppingOfTables(droppingTablesIsAMistake);
		}
	}

	@Override
	public synchronized void setBatchSQLStatementsWhenPossible(boolean batchSQLStatementsWhenPossible) {
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			next.setBatchSQLStatementsWhenPossible(batchSQLStatementsWhenPossible);
		}
	}

	@Override
	public synchronized boolean batchSQLStatementsWhenPossible() {
		boolean result = true;
		DBDatabase[] dbs = details.getReadyDatabases();
		for (DBDatabase next : dbs) {
			result &= next.batchSQLStatementsWhenPossible();
		}
		return result;
	}

	@Override
	public synchronized void dropDatabase(String databaseName, boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				try {
					next.dropDatabase(databaseName, doIt);
					finished = true;
				} catch (UnsupportedOperationException | SQLException | AutoCommitActionDuringTransactionException | ExceptionThrownDuringTransaction e) {
					handleExceptionDuringQuery(e, next);
				}
			}
		} while (!finished);
	}

	@Override
	public void dropDatabase(boolean doIt) throws UnsupportedOperationException, AutoCommitActionDuringTransactionException, SQLException, UnableToRemoveLastDatabaseFromClusterException {
		boolean finished = false;
		int tried = 0;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						tried++;
						next.dropDatabase(doIt);
						finished = true;
					} catch (UnsupportedOperationException | SQLException | AutoCommitActionDuringTransactionException | ExceptionThrownDuringTransaction e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (tried < 20 && !finished);
	}

	@Override
	public boolean willCreateBlankQuery(DBRow row) {
		return getReadyDatabase().willCreateBlankQuery(row);
	}

	@Override
	public synchronized <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException, UnableToRemoveLastDatabaseFromClusterException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.dropTableNoExceptions(tableRow);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public void dropTable(DBRow tableRow) throws SQLException, AutoCommitActionDuringTransactionException, AccidentalDroppingOfTableException, UnableToRemoveLastDatabaseFromClusterException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.dropTable(tableRow);
						finished = true;
					} catch (SQLException e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createIndexesOnAllFields(DBRow newTableRow) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createIndexesOnAllFields(newTableRow);
						finished = true;
					} catch (SQLException e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void removeForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.removeForeignKeyConstraints(newTableRow);
						finished = true;
					} catch (SQLException e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createForeignKeyConstraints(DBRow newTableRow) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createForeignKeyConstraints(newTableRow);
						finished = true;
					} catch (SQLException e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createTableWithForeignKeys(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createTableWithForeignKeys(newTableRow);
						finished = true;
					} catch (SQLException e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (!finished);
	}

	@Override
	public void createTable(DBRow newTableRow) throws SQLException, AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.createTable(newTableRow);
						finished = true;
					} catch (SQLException e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTablesWithForeignKeysNoExceptions(DBRow... newTables) {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTablesWithForeignKeysNoExceptions(newTables);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTablesNoExceptions(DBRow... newTables) {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTablesNoExceptions(newTables);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTablesNoExceptions(boolean includeForeignKeyClauses, DBRow... newTables) {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTablesNoExceptions(includeForeignKeyClauses, newTables);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTableNoExceptions(DBRow newTable) throws AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTableNoExceptions(newTable);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public synchronized void createTableNoExceptions(boolean includeForeignKeyClauses, DBRow newTable) throws AutoCommitActionDuringTransactionException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					next.createTableNoExceptions(includeForeignKeyClauses, newTable);
					finished = true;
				}
			}
		} while (!finished);
	}

	@Override
	public void updateTableToMatchDBRow(DBRow table) throws SQLException {
		boolean finished = false;
		do {
			DBDatabase[] dbs = details.getReadyDatabases();
			for (DBDatabase next : dbs) {
				synchronized (next) {
					try {
						next.updateTableToMatchDBRow(table);
						finished = true;
					} catch (SQLException e) {
						handleExceptionDuringQuery(e, next);
					}
				}
			}
		} while (!finished);
	}

	@Override
	public DBActionList test(DBScript script) throws SQLException, ExceptionThrownDuringTransaction {
		final DBDatabase readyDatabase = getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.test(script);
		}
	}

	@Override
	public <V> V doReadOnlyTransaction(DBTransaction<V> dbTransaction) throws SQLException, ExceptionThrownDuringTransaction {
		final DBDatabase readyDatabase = getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.doReadOnlyTransaction(dbTransaction);
		}
	}

	@Override
	public synchronized <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException {
		V result = null;
		boolean rollbackAll = false;
		List<DBDatabase> transactionDatabases = new ArrayList<>();
		try {
			final DBDatabase[] readyDatabases = details.getReadyDatabases();
			for (DBDatabase database : readyDatabases) {
				synchronized (database) {
					DBDatabase db;
					db = database.clone();
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
								} catch (SQLException rollbackFailed) {
									discardConnection(db.transactionConnection);
								}
							}
						} catch (ExceptionThrownDuringTransaction ex) {
							try {
								if (!explicitCommitActionRequired) {
									db.transactionConnection.rollback();
								}
							} catch (SQLException excp) {
								LOG.warn("Exception Occurred During Rollback: " + ex.getMessage());
							}
							throw ex;
						}
					} finally {
					}
					result = returnValues;
				}
			}
		} catch (Exception exc) {
			rollbackAll = true;
		} finally {
			for (DBDatabase db : transactionDatabases) {
				synchronized (db) {
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
		}
		return result;
	}

	@Override
	public <A extends DBReport> List<A> getRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.getRows(report, examples);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<>(0);
	}

	@Override
	public <A extends DBReport> List<A> getAllRows(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.getAllRows(report, examples);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<>(0);
	}

	@Override
	public <A extends DBReport> List<A> get(A report, DBRow... examples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.get(report, examples);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<>(0);
	}

	@Override
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.get(expectedNumberOfRows, rows);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<>(0);
	}

	@Override
	public List<DBQueryRow> getByExamples(DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.getByExamples(rows);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<>(0);
	}

	@Override
	public List<DBQueryRow> get(DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.get(rows);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<>(0);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.getByExample(expectedNumberOfRows, exampleRow);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<R>(0);
	}

	@Override
	public <R extends DBRow> List<R> get(Long expectedNumberOfRows, R exampleRow) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.get(expectedNumberOfRows, exampleRow);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<R>(0);
	}

	@Override
	public <R extends DBRow> List<R> getByExample(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.getByExample(exampleRow);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<R>(0);
	}

	@Override
	public <R extends DBRow> List<R> get(R exampleRow) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBDatabase readyDatabase;
		boolean finished = false;
		do {
			readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					return readyDatabase.get(exampleRow);
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		} while (!finished);
		return new ArrayList<R>(0);
	}

	@Override
	public Connection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		throw new UnsupportedOperationException("DBDatabase.getConnection should not be used.");
	}

	@Override
	protected DBStatement getLowLevelStatement() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException {
		return getClusterStatement();
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	private void synchronizeSecondaryDatabases() throws SQLException {
		DBDatabase[] addedDBs;
		addedDBs = details.getUnsynchronizedDatabases();
		for (DBDatabase db : addedDBs) {
			details.synchronizingDatabase(db);

			//Do The Synchronising...
			synchronizeSecondaryDatabase(db);
		}
	}

	@Override
	public synchronized DBActionList executeDBAction(DBAction action) throws SQLException {
		addActionToQueue(action);
		List<ActionTask> tasks = new ArrayList<ActionTask>();
		DBActionList actionsPerformed = new DBActionList();
		try {
			DBDatabase readyDatabase = getReadyDatabase();
			boolean finished = false;
			do {
				try {
					if (action.requiresRunOnIndividualDatabaseBeforeCluster()) {
						// Because of autoincrement PKs we need to execute on one database first
						actionsPerformed = new ActionTask(this, readyDatabase, action).call();
						removeActionFromQueue(readyDatabase, action);
						finished = true;
					} else {
						finished = true;
					}
				} catch (SQLException e) {
					handleExceptionDuringAction(e, readyDatabase);
				}
			} while (!finished && size() > 1);
			final DBDatabase[] readyDatabases = details.getReadyDatabases();
			// Now execute on all the other databases
			for (DBDatabase next : readyDatabases) {
				if (action.runOnDatabaseDuringCluster(readyDatabase, next)) {
					final ActionTask task = new ActionTask(this, next, action);
					tasks.add(task);
					removeActionFromQueue(next, action);
				}
			}
			threadPool.invokeAll(tasks);
		} catch (InterruptedException ex) {
			Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
			throw new DBRuntimeException("Unable To Run Actions", ex);
		}
		if (actionsPerformed.isEmpty()) {
			actionsPerformed = tasks.get(0).getActionList();
		}
		return actionsPerformed;
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, UnableToRemoveLastDatabaseFromClusterException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQueryable actionsPerformed = query;
		boolean finished = false;
		while (!finished) {
			final DBDatabase readyDatabase = getReadyDatabase();
			synchronized (readyDatabase) {
				try {
					actionsPerformed = readyDatabase.executeDBQuery(query);
					finished = true;
				} catch (SQLException e) {
					handleExceptionDuringQuery(e, readyDatabase);
				}
			}
		}
		return actionsPerformed;
	}

	private static ArrayList<Class<? extends Exception>> okExceptions
			= new ArrayList<Class<? extends Exception>>() {
		private static final long serialVersionUID = 1l;

		{
			add(UnexpectedNumberOfRowsException.class);
			add(AutoCommitActionDuringTransactionException.class);
			add(AccidentalDroppingOfTableException.class);
			add(CloneNotSupportedException.class);
		}
	};

	private void handleExceptionDuringQuery(Exception e, final DBDatabase readyDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		if (!okExceptions.contains(e.getClass())) {
			if (size() == 1) {
				if (e instanceof SQLException) {
					throw (SQLException) e;
				} else {
					throw new SQLException(e);
				}
			} else {
				removeDatabase(readyDatabase);
			}
		}
	}

	private void handleExceptionDuringAction(Exception e, final DBDatabase readyDatabase) throws SQLException, UnableToRemoveLastDatabaseFromClusterException {
		if (size() == 1) {
			if (e instanceof SQLException) {
				throw (SQLException) e;
			} else {
				throw new SQLException(e);
			}
		} else {
			removeDatabase(readyDatabase);
		}
	}

	@Override
	public String getSQLForDBQuery(DBQueryable query) {
		final DBDatabase readyDatabase = this.getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.getSQLForDBQuery(query);
		}
	}

	ArrayList<DBStatement> getDBStatements() throws SQLException {
		ArrayList<DBStatement> arrayList = new ArrayList<>();
		final DBDatabase[] readyDatabases = details.getReadyDatabases();
		for (DBDatabase db : readyDatabases) {
			synchronized (db) {
				arrayList.add(db.getDBStatement());
			}
		}
		return arrayList;
	}

	@Override
	public DBDefinition getDefinition() {
		final DBDatabase readyDatabase = getReadyDatabase();
		synchronized (readyDatabase) {
			return readyDatabase.getDefinition();
		}
	}

	@Override
	public void setPrintSQLBeforeExecuting(boolean b) {
		for (DBDatabase db : details.getAllDatabases()) {
			synchronized (db) {
				db.setPrintSQLBeforeExecuting(b);
			}
		}
	}

	private void addActionToQueue(DBAction action) {
		for (DBDatabase db : details.getAllDatabases()) {
			Queue<DBAction> queue = details.getActionQueue(db);
			queue.add(action);
		}
	}

	private void removeActionFromQueue(DBDatabase database, DBAction action) {
		final Queue<DBAction> queue = details.getActionQueue(database);
		synchronized (queue) {
			if (queue != null) {
				queue.remove(action);
			}
		}
	}

	private void synchronizeSecondaryDatabase(DBDatabase secondary) throws SQLException, NoAvailableDatabaseException, AccidentalCartesianJoinException, AccidentalBlankQueryException {

		DBDatabase template = null;
		try {
			template = getTemplateDatabase();
		} catch (NoAvailableDatabaseException except) {
			// must be the first database
		}
		if (template != null) {
			// Check that we're not synchronising the reference database
			if (!template.equals(secondary)) {
				final DBRow[] requiredTables = details.getRequiredTables();
				for (DBRow table : requiredTables) {
					if (true) {
						if (template.tableExists(table)) {
							// Make sure it exists in the new database
							if (secondary.tableExists(table) == false) {
								secondary.createTable(table);
							}
							// Check that the table has data
							final DBTable<DBRow> primaryTable = template.getDBTable(table);
							final DBTable<DBRow> secondaryTable = secondary.getDBTable(table);
							final Long primaryTableCount = primaryTable.count();
							final Long secondaryTableCount = secondaryTable.count();
							if (primaryTableCount > 0) {
								final DBTable<DBRow> primaryData = primaryTable.setBlankQueryAllowed(true);
								// Check that the new database has data
								if (secondaryTableCount == 0) {
									List<DBRow> allRows = primaryData.getAllRows();
									secondaryTable.insert(allRows);
								} else if (!secondaryTableCount.equals(primaryTableCount)) {
									// Something is different in the data so correct it
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
		releaseTemplateDatabase(template);
		synchronizeActions(secondary);
//		}
	}

	private synchronized void synchronizeActions(DBDatabase db) throws SQLException {
		if (db != null) {
			db.setExplicitCommitAction(false);
			Queue<DBAction> queue = details.getActionQueue(db);
			while (queue != null && !queue.isEmpty()) {
				DBAction action = queue.remove();
				db.executeDBAction(action);
			}
			db.setExplicitCommitAction(true);
			details.readyDatabase(db);
		}
	}

	private synchronized void synchronizeAddedDatabases(boolean blocking) throws SQLException {
		boolean block = blocking || (details.getReadyDatabases().length < 2);
		final DBDatabase[] dbs = details.getUnsynchronizedDatabases();
		for (DBDatabase addedDatabase : dbs) {
			SynchroniseTask task = new SynchroniseTask(this, addedDatabase) {
				@Override
				public Void synchronise(DBDatabaseCluster cluster, DBDatabase database) throws SQLException {
					cluster.synchronizeSecondaryDatabase(database);
					return null;
				}
			};
			if (block) {
				try {
					task.synchronise(this, addedDatabase);
				} catch (SQLException ex) {
					Logger.getLogger(DBDatabaseCluster.class.getName()).log(Level.SEVERE, null, ex);
				}
			} else {
				try {
					threadPool.submit(task);
				} catch (RejectedExecutionException ex) {
					task.synchronise(this, addedDatabase);
				}
			}
		}
	}

	@Override
	public synchronized boolean tableExists(DBRow table) throws SQLException {
		boolean tableExists = true;
		for (DBDatabase readyDatabase : details.getReadyDatabases()) {
			synchronized (readyDatabase) {
				final boolean tableExists1 = readyDatabase.tableExists(table);
				tableExists &= tableExists1;
			}
		}
		return tableExists;
	}

	/**
	 * Returns the number of ready databases.
	 *
	 * <p>
	 * The size of the cluster is dynamic as databases are added, removed, and
	 * synchronized but this method returns the size of the cluster in terms of
	 * active databases at this point in time.</p>
	 *
	 * <ul>
	 * <li>DBDatabaseClusters within this cluster count as 1 database each.</li>
	 * <li>Unsynchronized databases are not counted by this method.</li>
	 * </ul>.
	 *
	 * @return the number of ready database.
	 */
	public int size() {
		return details.getReadyDatabases().length;
	}

	private synchronized void releaseTemplateDatabase(DBDatabase primary) throws SQLException {
		synchronizeActions(primary);
	}

	private DBDatabase getTemplateDatabase() throws NoAvailableDatabaseException {
		return details.getTemplateDatabase();
	}

	public String getClusterStatus() {
		return "Active Databases: " + details.getReadyDatabases().length + " of " + details.getAllDatabases().length;
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	private static class ActionTask implements Callable<DBActionList> {

		private final DBDatabase database;
		private final DBAction action;
		private final DBDatabaseCluster cluster;
		private DBActionList actionList = new DBActionList();

		public ActionTask(DBDatabaseCluster cluster, DBDatabase db, DBAction action) {
			this.cluster = cluster;
			this.database = db;
			this.action = action;
		}

		@Override
		public DBActionList call() throws SQLException {
			try {
				DBActionList actions = database.executeDBAction(action);
				setActionList(actions);
				return getActionList();
			} catch (SQLException e) {
				cluster.handleExceptionDuringAction(e, database);
			}
			return getActionList();
		}

		public synchronized DBActionList getActionList() {
			final DBActionList newList = new DBActionList();
			newList.addAll(actionList);
			return newList;
		}

		private synchronized void setActionList(DBActionList actions) {
			this.actionList = actions;
		}
	}

	private static abstract class SynchroniseTask implements Callable<Void> {

		private final DBDatabaseCluster cluster;
		private final DBDatabase database;

		public SynchroniseTask(DBDatabaseCluster cluster, DBDatabase db) {
			this.cluster = cluster;
			this.database = db;
		}

		@Override
		public Void call() throws Exception {
			return synchronise(getCluster(), getDatabase());
		}

		/**
		 * @return the cluster
		 */
		public DBDatabaseCluster getCluster() {
			return cluster;
		}

		/**
		 * @return the database
		 */
		public DBDatabase getDatabase() {
			return database;
		}

		public abstract Void synchronise(DBDatabaseCluster cluster, DBDatabase database) throws SQLException;
	}

	public static enum Status {

		READY,
		UNSYNCHRONISED,
		PAUSED,
		DEAD,
		REJECTED,
		UNKNOWN,
		PROCESSING,
		QUARANTINED
	}
}
