/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.databases;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import nz.co.gregs.dbvolution.DBMigration;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryInsert;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBActionList;
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
public class FederatedDatabase extends DBDatabase {

	private static final long serialVersionUID = 1l;

	private final List<DBDatabase> allDatabases = new ArrayList<>();
	private final List<DBDatabase> addedDatabases = new ArrayList<>();
	private final List<DBDatabase> readyDatabases = new ArrayList<>();

	public FederatedDatabase(DBDatabase... databases) {
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

	public DBDatabase[] getDatabases(DBDatabase database) {
		return allDatabases.toArray(new DBDatabase[]{});
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
	public <K extends DBRow> DBMigration<K> getDBMigration(K mapper) {
		return super.getDBMigration(mapper);
	}

	@Override
	public <K extends DBRow> DBQueryInsert<K> getDBQueryInsert(K mapper) {
		return super.getDBQueryInsert(mapper);
	}

	@Override
	public void addFeatureToFixException(Exception exp) throws Exception {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.addFeatureToFixException(exp);
		}
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		super.addDatabaseSpecificFeatures(statement);
	}

	@Override
	public synchronized void discardConnection(Connection connection) {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.discardConnection(connection);
		}

	}

	@Override
	protected boolean supportsPooledConnections() {
		return super.supportsPooledConnections();
	}

	@Override
	public synchronized void unusedConnection(Connection connection) throws SQLException {
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
			next.unusedConnection(connection);
		}
	}

	@Override
	protected void setPassword(String password) {
		super.setPassword(password);
	}

	@Override
	protected void setUsername(String username) {
		super.setUsername(username);
	}

	@Override
	protected void setJdbcURL(String jdbcURL) {
		super.setJdbcURL(jdbcURL);
	}

	@Override
	public Connection getConnectionFromDriverManager() throws SQLException {
		return super.getConnectionFromDriverManager();
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
	public boolean supportsRightOuterJoinNatively() {
		return super.supportsRightOuterJoinNatively();
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return super.supportsFullOuterJoinNatively();
	}

	@Override
	public boolean supportsFullOuterJoin() {
		return super.supportsFullOuterJoin();
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
	protected void setDatabaseName(String databaseName) {
		super.setDatabaseName(databaseName);
	}

	@Override
	public String getDatabaseName() {
		return super.getDatabaseName();
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
	protected void setDefinition(DBDefinition defn) {
		;//this is a null operation for federated databases as each individual db defines it's own definition
	}

	@Override
	public DBDefinition getDefinition() {
		return new DBDefinition() {
			@Override
			public String getDateFormattedForQuery(Date date) {
				throw new UnsupportedOperationException("Not supported yet.");
			}

			@Override
			public String doDayOfWeekTransform(String dateSQL) {
				throw new UnsupportedOperationException("Not supported yet.");
			}
		};
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
		for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
			DBDatabase next = iterator.next();
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
	public void printSQLIfRequested(String sqlString) {
		super.printSQLIfRequested(sqlString);
	}

	@Override
	public boolean isPrintSQLBeforeExecuting() {
		return super.isPrintSQLBeforeExecuting();
	}

	@Override
	public void setPrintSQLBeforeExecuting(boolean b) {
		super.setPrintSQLBeforeExecuting(b);
	}

	@Override
	public DBQuery getDBQuery(List<DBRow> examples) {
		return super.getDBQuery(examples);
	}

	@Override
	public DBQuery getDBQuery(DBRow... examples) {
		return super.getDBQuery(examples);
	}

	@Override
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return super.getDBTable(example);
	}

	@Override
	public String getPassword() {
		return super.getPassword();
	}

	@Override
	public String getUsername() {
		return super.getUsername();
	}

	@Override
	public String getJdbcURL() {
		return super.getJdbcURL();
	}

	@Override
	protected void setDriverName(String driver) {
		super.setDriverName(driver);
	}

	@Override
	public String getDriverName() {
		return super.getDriverName();
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

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction) throws SQLException, Exception {
		V result = null;
		try {
			for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
				DBDatabase next = iterator.next();
				result = next.doTransaction(dbTransaction);
			}
			commitAll();
		} catch (Exception exc) {
			rollbackAll(exc);
		}
		return result;
	}

	@Override
	public <V> V doTransaction(DBTransaction<V> dbTransaction, Boolean commit) throws SQLException, Exception {
		V result = null;
		try {
			for (Iterator<DBDatabase> iterator = readyDatabases.iterator(); iterator.hasNext();) {
				DBDatabase next = iterator.next();
				result = next.doTransaction(dbTransaction, commit);
			}
			commitAll();
		} catch (Exception exc) {
			rollbackAll(exc);
		}
		return result;
	}

	@Override
	public List<DBQueryRow> get(Long expectedNumberOfRows, DBRow... rows) throws SQLException, UnexpectedNumberOfRowsException {
		return getReadyDatabase().get(expectedNumberOfRows, rows);
	}

	@Override
	public void print(List<?> rows) {
		super.print(rows);
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
		return super.getConnection();
	}

	@Override
	public synchronized boolean equals(Object obj) {
		if (obj instanceof FederatedDatabase) {
			FederatedDatabase otherDB = (FederatedDatabase) obj;
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
		for (Iterator<DBDatabase> iterator1 = addedDatabases.iterator(); iterator1.hasNext();) {
			DBDatabase db = iterator1.next();
			addedDatabases.remove(db);
			//Do The Synchronising...

			db.setExplicitCommitAction(true);
			//Mark the database as ready
			readyDatabases.add(db);
		}
	}

	private void commitAll() {
		for (Iterator<DBDatabase> iterator1 = readyDatabases.iterator(); iterator1.hasNext();) {
			DBDatabase db = iterator1.next();
			db.doCommit();
		}
	}

	private void rollbackAll(Exception exc) {
		for (Iterator<DBDatabase> iterator1 = readyDatabases.iterator(); iterator1.hasNext();) {
			DBDatabase db = iterator1.next();
			db.doRollback();
		}
	}

}
