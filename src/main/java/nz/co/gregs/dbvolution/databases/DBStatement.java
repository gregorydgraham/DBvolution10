
/*
 * Copyright 2013 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.databases;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Encapsulates the JDBC Connection and Statement classes.
 *
 * <p>
 * You should not need to create a DBStatement as they are managed by the
 * DBDatabase internally.
 *
 * <p>
 * DBStatement simplifies the JDBC interface by managing the connection and
 * statement simultaneously. When the statement is closed so is the connection
 * ensuring minimalist usage of the database.
 *
 * <p>
 * Mostly this is a thin wrapper around DBDatabase, Connection, and Statement
 * objects
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBStatement implements Statement {

	static final private Log LOG = LogFactory.getLog(DBStatement.class);

	private Statement internalStatement;
	private boolean batchHasEntries;
	final DBDatabase database;
	private final Connection connection;
	private boolean isClosed = false;

//	private static final List<DBStatement> DBSTATEMENT_REGISTER = new ArrayList<>();
//	private static final List<DBStatement> DBSTATEMENT_CLOSED_REGISTER = new ArrayList<>();

	/**
	 * Creates a statement object for the given DBDatabase and Connection.
	 *
	 * @param db the target database
	 * @param connection the connection to the database
	 */
	public DBStatement(DBDatabase db, Connection connection) {
		this.database = db;
		this.connection = connection;
//		this.internalStatement = connection.createStatement();
//		DBSTATEMENT_REGISTER.add(this);
	}

	/**
	 * Executes the given SQL statement, which returns a single ResultSet object.
	 *
	 * @param sql SQL
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a ResultSet
	 * @throws SQLException database exceptions
	 */
	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		final String logSQL = "EXECUTING QUERY: " + sql;
		database.printSQLIfRequested(logSQL);
//		LOG.debug(logSQL);
		ResultSet executeQuery = null;
		try {
			executeQuery = getInternalStatement().executeQuery(sql);
		} catch (SQLException exp) {
			try {
				executeQuery = addFeatureAndAttemptQueryAgain(exp, sql);
			} catch (SQLException ex) {
				throw ex;
			} catch (Exception ex) {
				throw new SQLException(ex);
			}
		}
		return executeQuery;
	}

	private ResultSet addFeatureAndAttemptQueryAgain(Exception exp, String sql) throws Exception {
		ResultSet executeQuery;
		checkForBrokenConnection(exp, sql);
		DBDatabase.ResponseToException nextAction = DBDatabase.ResponseToException.REQUERY;
		try {
			nextAction = database.addFeatureToFixException(exp);
		} catch (Exception ex) {
			Exception ex1 = exp;
			while (!ex1.getMessage().equals(ex.getMessage())) {
				database.addFeatureToFixException(ex);
			}
			throw new SQLException(exp);
		}
		if (nextAction.equals(DBDatabase.ResponseToException.REQUERY)) {
			try {
				executeQuery = getInternalStatement().executeQuery(sql);
				return executeQuery;
			} catch (SQLException exp2) {
				if (exp.getMessage().equals(exp2.getMessage())) {
					throw exp;
				} else {
					executeQuery = addFeatureAndAttemptQueryAgain(exp2, sql);
					return executeQuery;
				}
			}
		}
		return null;
	}

	/**
	 * Executes the given SQL statement, which may be an INSERT, UPDATE, or DELETE
	 * statement or an SQL statement that returns nothing, such as an SQL DDL
	 * statement.
	 *
	 * @param string	string
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int executeUpdate(String string) throws SQLException {
		int executeUpdate = getInternalStatement().executeUpdate(string);

		return executeUpdate;
	}

	/**
	 * Closes the Statement and the Connection.
	 *
	 * <p>
	 * Also informs the DBDatabase that there is one less connection to the
	 * database.
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void close() throws SQLException {
		isClosed = true;
		try {
			database.unusedConnection(getConnection());
		} catch (SQLException e) {
			// Someone please tell me how you are supposed to cope 
			// with an exception during the close method????????
			LOG.warn("Exception occurred during close(): " + e.getMessage(), e);
		}
		try {
			Statement statement = getInternalStatement();
//			System.out.println("CLOSING DBSTATEMENT");
			statement.close();
			setInternalStatement(null);
//			DBSTATEMENT_REGISTER.remove(this);
//			DBSTATEMENT_CLOSED_REGISTER.remove(this);
		} catch (SQLException e) {
			// Someone please tell me how you are supposed to cope 
			// with an exception during the close method????????
			LOG.warn("Exception occurred during close(): " + e.getMessage(), e);
		}
	}

	/**
	 * Retrieves the maximum number of bytes that can be returned for character
	 * and binary column values in a ResultSet object produced by this Statement
	 * object. This limit applies only to BINARY, VARBINARY, LONGVARBINARY, CHAR,
	 * VARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR and LONGVARCHAR columns. If the
	 * limit is exceeded, the excess data is silently discarded.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the current column size limit for columns storing character and
	 * binary values; zero means there is no limit. 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getMaxFieldSize() throws SQLException {
		return getInternalStatement().getMaxFieldSize();
	}

	/**
	 * Sets the limit for the maximum number of bytes that can be returned for
	 * character and binary column values in a ResultSet object produced by this
	 * Statement object. This limit applies only to BINARY, VARBINARY,
	 * LONGVARBINARY, CHAR, VARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR and LONGVARCHAR
	 * fields. If the limit is exceeded, the excess data is silently discarded.
	 * For maximum portability, use values greater than 256.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param i i
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setMaxFieldSize(int i) throws SQLException {
		getInternalStatement().setMaxFieldSize(i);
	}

	/**
	 * Retrieves the maximum number of rows that a ResultSet object produced by
	 * this Statement object can contain. If this limit is exceeded, the excess
	 * rows are silently dropped.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the current maximum number of rows for a <code>ResultSet</code>
	 * object produced by this <code>Statement</code> object; zero means there is
	 * no limit 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getMaxRows() throws SQLException {
		return getInternalStatement().getMaxRows();
	}

	/**
	 * Sets the limit for the maximum number of rows that any ResultSet object
	 * generated by this Statement object can contain to the given number. If the
	 * limit is exceeded, the excess rows are silently dropped.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param i i
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setMaxRows(int i) throws SQLException {
		getInternalStatement().setMaxRows(i);
	}

	/**
	 * Sets escape processing on or off. If escape scanning is on (the default),
	 * the driver will do escape substitution before sending the SQL statement to
	 * the database. Note: Since prepared statements have usually been parsed
	 * prior to making this call, disabling escape processing for
	 * PreparedStatements objects will have no effect.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param bln bln
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setEscapeProcessing(boolean bln) throws SQLException {
		getInternalStatement().setEscapeProcessing(bln);
	}

	/**
	 * Retrieves the number of seconds the driver will wait for a Statement object
	 * to execute. If the limit is exceeded, a SQLException is thrown.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the current query timeout limit in seconds; zero means there is no
	 * limit 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getQueryTimeout() throws SQLException {
		return getInternalStatement().getQueryTimeout();
	}

	/**
	 * Sets the number of seconds the driver will wait for a Statement object to
	 * execute to the given number of seconds. By default there is no limit on the
	 * amount of time allowed for a running statement to complete. If the limit is
	 * exceeded, an SQLTimeoutException is thrown. A JDBC driver must apply this
	 * limit to the execute, executeQuery and executeUpdate methods.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param i i
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setQueryTimeout(int i) throws SQLException {
		getInternalStatement().setQueryTimeout(i);
	}

	/**
	 * Cancels this Statement object if both the DBMS and driver support aborting
	 * an SQL statement. This method can be used by one thread to cancel a
	 * statement that is being executed by another thread.
	 *
	 * 1 Database exceptions may be thrown
	 *
	 */
	@Override
	public synchronized void cancel() throws SQLException {
		try {
			getInternalStatement().cancel();
			if (database.getDefinition().willCloseConnectionOnStatementCancel()) {
				replaceBrokenConnection();
			}
		} catch (SQLException | UnableToCreateDatabaseConnectionException | UnableToFindJDBCDriver ex) {
			//;Logger.getLogger(DBStatement.class.getName()).log(Level.INFO, "Cancel Threw An Exception", ex);
		}
	}

	/**
	 * Discards the current connection, replaces it, and creates a new statement.
	 *
	 * <p>
	 * Call this after canceling or closing a statement for a database that close
	 * the connection with the statement. Use {@link DBDefinition#willCloseConnectionOnStatementCancel()
	 * } to detect this situation.
	 *
	 * @throws SQLException exceptions from connecting to the database and
	 * creating a Statement.
	 * @throws UnableToCreateDatabaseConnectionException may be thrown if there is
	 * an issue with connecting.
	 * @throws UnableToFindJDBCDriver may be thrown if the JDBCDriver is not on
	 * the class path. DBvolution includes several JDBCDrivers already but Oracle
	 * and MS SQLserver, in particular, need to be added to the path if you wish
	 * to work with those databases.
	 */
	protected synchronized void replaceBrokenConnection() throws SQLException, UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver {
		database.discardConnection(connection);
	}

	/**
	 * Retrieves the first warning reported by calls on this Statement object.
	 * Subsequent Statement object warnings will be chained to this SQLWarning
	 * object.
	 *
	 * <p>
	 * The warning chain is automatically cleared each time a statement is
	 * (re)executed. This method may not be called on a closed Statement object;
	 * doing so will cause an SQLException to be thrown.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the first <code>SQLWarning</code> object or <code>null</code> if
	 * there are no warnings 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		return getInternalStatement().getWarnings();
	}

	/**
	 * Clears all the warnings reported on this Statement object. After a call to
	 * this method, the method getWarnings will return null until a new warning is
	 * reported for this Statement object.
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void clearWarnings() throws SQLException {
		getInternalStatement().clearWarnings();
	}

	/**
	 * Sets the SQL cursor name to the given String, which will be used by
	 * subsequent Statement object execute methods.
	 *
	 * <P>
	 * This name can then be used in SQL positioned update or delete statements to
	 * identify the current row in the ResultSet object generated by this
	 * statement. If the database does not support positioned update/delete, this
	 * method is a noop. To insure that a cursor has the proper isolation level to
	 * support updates, the cursor's SELECT statement should have the form SELECT
	 * FOR UPDATE. If FOR UPDATE is not present, positioned updates may fail.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param string string
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setCursorName(String string) throws SQLException {
		getInternalStatement().setCursorName(string);
	}

	/**
	 * Executes the given SQL statement, which may return multiple results.
	 *
	 * <p>
	 * In some (uncommon) situations, a single SQL statement may return multiple
	 * result sets and/or update counts. Normally you can ignore this unless you
	 * are (1) executing a stored procedure that you know may return multiple
	 * results or (2) you are dynamically executing an unknown SQL string.
	 *
	 * <p>
	 * The execute method executes an SQL statement and indicates the form of the
	 * first result. You must then use the methods getResultSet or getUpdateCount
	 * to retrieve the result, and getMoreResults to move to any subsequent
	 * result(s).
	 *
	 * @param sql	string
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return <code>TRUE</code> if the first result is a <code>ResultSet</code>
	 * object; <code>FALSE</code> if it is an update count or there are no results
	 * 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean execute(String sql) throws SQLException {
		final String logSQL = "EXECUTING: " + sql;
		database.printSQLIfRequested(logSQL);
		LOG.debug(logSQL);
		final boolean execute;
		try {
			execute = getInternalStatement().execute(sql);
		} catch (SQLException exp) {
//			exp.printStackTrace();
			return addFeatureAndAttemptExecuteAgain(exp, sql);
		}
		return execute;
	}

	private boolean addFeatureAndAttemptExecuteAgain(Exception exp, String sql) throws SQLException {
		boolean executeQuery;
		try {
			database.addFeatureToFixException(exp);
		} catch (Exception ex) {
//			ex.printStackTrace();
			throw new SQLException("Failed To Add Support For SQL: " + exp.getMessage() + " : Original Query: " + sql, ex);
		}
		try {
			executeQuery = getInternalStatement().execute(sql);
			return executeQuery;
		} catch (SQLException exp2) {
//			exp2.printStackTrace();
			if (!exp.getMessage().equals(exp2.getMessage())) {
				executeQuery = addFeatureAndAttemptExecuteAgain(exp2, sql);
				return executeQuery;
			} else {
				throw new SQLException(exp);
			}
		}
	}

	private boolean addFeatureAndAttemptExecuteAgain(Exception exp, String string, String[] strings) throws SQLException {
		boolean executeQuery;
		try {
			database.addFeatureToFixException(exp);
		} catch (Exception ex) {
			throw new SQLException("Failed To Add Support For SQL: " + exp.getMessage() + " : Original Query: " + string, ex);
		}
		try {
			executeQuery = getInternalStatement().execute(string, strings);
			return executeQuery;
		} catch (SQLException exp2) {
			if (!exp.getMessage().equals(exp2.getMessage())) {
				executeQuery = addFeatureAndAttemptExecuteAgain(exp2, string, strings);
				return executeQuery;
			} else {
				throw new SQLException(exp);
			}
		}
	}

	/**
	 * Retrieves the current result as a ResultSet object.
	 *
	 * <p>
	 * This method should be called only once per result.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the current result as a <code>ResultSet</code> object or
	 * <code>null</code> if the result is an update count or there are no more
	 * results 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public ResultSet getResultSet() throws SQLException {
		return getInternalStatement().getResultSet();
	}

	/**
	 * Retrieves the current result as an update count; if the result is a
	 * ResultSet object or there are no more results, -1 is returned.
	 * <p>
	 * This method should be called only once per result.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the current result as an update count; -1 if the current result is
	 * a <code>ResultSet</code> object or there are no more results. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getUpdateCount() throws SQLException {
		return getInternalStatement().getUpdateCount();
	}

	/**
	 * Moves to this Statement object's next result, returns true if it is a
	 * ResultSet object, and implicitly closes any current ResultSet object(s)
	 * obtained with the method getResultSet.
	 * <p>
	 * There are no more results when the following is true:
	 * <p>
	 * <code>
	 * // stmt is a Statement object ((stmt.getMoreResults() == false) &amp;&amp;
	 * (stmt.getUpdateCount() == -1))
	 * </code>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true if the next result is a ResultSet object; false if it is an
	 * update count or there are no more results 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean getMoreResults() throws SQLException {
		return getInternalStatement().getMoreResults();
	}

	/**
	 * Gives the driver a hint as to the direction in which rows will be processed
	 * in ResultSet objects created using this Statement object.
	 * <P>
	 * The default value is ResultSet.FETCH_FORWARD.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param i i
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setFetchDirection(int i) throws SQLException {
		getInternalStatement().setFetchDirection(i);
	}

	/**
	 * Retrieves the direction for fetching rows from database tables that is the
	 * default for result sets generated from this Statement object.
	 * <P>
	 * If this Statement object has not set a fetch direction by calling the
	 * method setFetchDirection, the return value is implementation-specific.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default fetch direction for result sets generated from this
	 * Statement object 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getFetchDirection() throws SQLException {
		return getInternalStatement().getFetchDirection();
	}

	/**
	 * Gives the JDBC driver a hint as to the number of rows that should be
	 * fetched from the database when more rows are needed for ResultSet objects
	 * generated by this Statement.
	 * <P>
	 * If the value specified is zero, then the hint is ignored. The default value
	 * is zero.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param i i
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setFetchSize(int i) throws SQLException {
		getInternalStatement().setFetchSize(i);
	}

	/**
	 * Retrieves the number of result set rows that is the default fetch size for
	 * ResultSet objects generated from this Statement object.
	 * <P>
	 * If this Statement object has not set a fetch size by calling the method
	 * setFetchSize, the return value is implementation-specific.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default fetch size for result sets generated from this
	 * Statement object
	 *
	 * 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getFetchSize() throws SQLException {
		return getInternalStatement().getFetchSize();
	}

	/**
	 * Retrieves the result set concurrency for ResultSet objects generated by
	 * this Statement object.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return either ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getResultSetConcurrency() throws SQLException {
		return getInternalStatement().getResultSetConcurrency();
	}

	/**
	 * Retrieves the result set type for ResultSet objects generated by this
	 * Statement object.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return one of ResultSet.TYPE_FORWARD_ONLY,
	 * ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getResultSetType() throws SQLException {
		return getInternalStatement().getResultSetType();
	}

	/**
	 * Adds the given SQL command to the current list of commands for this
	 * Statement object. The commands in this list can be executed as a batch by
	 * calling the method executeBatch.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param string string
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void addBatch(String string) throws SQLException {
		getInternalStatement().addBatch(string);
		setBatchHasEntries(true);
	}

	/**
	 * Empties this Statement object's current list of SQL commands.
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void clearBatch() throws SQLException {
		getInternalStatement().clearBatch();
		setBatchHasEntries(false);
	}

	/**
	 * Submits a batch of commands to the database for execution and if all
	 * commands execute successfully, returns an array of update counts.
	 * <P>
	 * The int elements of the array that is returned are ordered to correspond to
	 * the commands in the batch, which are ordered according to the order in
	 * which they were added to the batch. The elements in the array returned by
	 * the method executeBatch may be one of the following: <ol><li>A number
	 * greater than or equal to zero -- indicates that the command was processed
	 * successfully and is an update count giving the number of rows in the
	 * database that were affected by the command's execution </li><li>A value of
	 * SUCCESS_NO_INFO -- indicates that the command was processed successfully
	 * but that the number of rows affected is unknown </li><li>A value of
	 * EXECUTE_FAILED -- indicates that the command failed to execute successfully
	 * and occurs only if a driver continues to process commands after a command
	 * fails</li></ol><p>
	 * If one of the commands in a batch update fails to execute properly, this
	 * method throws a BatchUpdateException, and a JDBC driver may or may not
	 * continue to process the remaining commands in the batch. However, the
	 * driver's behavior must be consistent with a particular DBMS, either always
	 * continuing to process commands or never continuing to process commands. If
	 * the driver continues processing after a failure, the array returned by the
	 * method BatchUpdateException.getUpdateCounts will contain as many elements
	 * as there are commands in the batch, and at least one of the elements will
	 * be a value of EXECUTE_FAILED.
	 * <p>
	 * The possible implementations and return values have been modified in the
	 * Java 2 SDK, Standard Edition, version 1.3 to accommodate the option of
	 * continuing to process commands in a batch update after a
	 * BatchUpdateException object has been thrown.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an array of update counts containing one element for each command
	 * in the batch. The elements of the array are ordered according to the order
	 * in which commands were added to the batch. 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int[] executeBatch() throws SQLException {
		return getInternalStatement().executeBatch();
	}

	/**
	 * Retrieves the Connection object that produced this Statement object.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the connection that produced this statement 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	/**
	 * Moves to this Statement object's next result, deals with any current
	 * ResultSet object(s) according to the instructions specified by the given
	 * flag, and returns true if the next result is a ResultSet object.
	 * <p>
	 * There are no more results when the following is true:</p>
	 * <code>
	 *
	 * (statement.getMoreResults(current) == false)
	 * &amp;&amp; (statement.getUpdateCount() == -1))
	 * </code>
	 *
	 * @param i	i
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return true if the next result is a ResultSet object; false if it is an
	 * update count or there are no more results. 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean getMoreResults(int i) throws SQLException {
		return getInternalStatement().getMoreResults();
	}

	/**
	 * Retrieves any auto-generated keys created as a result of executing this
	 * Statement object.
	 * <p>
	 * If this Statement object did not generate any keys, an empty ResultSet
	 * object is returned.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a ResultSet object containing the auto-generated key(s) generated
	 * by the execution of this Statement object 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return getInternalStatement().getGeneratedKeys();
	}

	/**
	 * Executes the given SQL statement and signals the driver with the given flag
	 * about whether the auto-generated keys produced by this Statement object
	 * should be made available for retrieval.
	 * <P>
	 * The driver will ignore the flag if the SQL statement is not an INSERT
	 * statement, or an SQL statement able to return auto-generated keys (the list
	 * of such statements is vendor-specific).
	 *
	 * @param string string
	 * @param i i
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int executeUpdate(String string, int i) throws SQLException {
		return getInternalStatement().executeUpdate(string, i);
	}

	/**
	 * Executes the given SQL statement and signals the driver that the
	 * auto-generated keys indicated in the given array should be made available
	 * for retrieval.
	 * <P>
	 * This array contains the indexes of the columns in the target table that
	 * contain the auto-generated keys that should be made available. The driver
	 * will ignore the array if the SQL statement is not an INSERT statement, or
	 * an SQL statement able to return auto-generated keys (the list of such
	 * statements is vendor-specific).
	 *
	 * @param string string
	 * @param ints ints
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int executeUpdate(String string, int[] ints) throws SQLException {
		return getInternalStatement().executeUpdate(string, ints);
	}

	/**
	 * Executes the given SQL statement and signals the driver that the
	 * auto-generated keys indicated in the given array should be made available
	 * for retrieval.
	 * <p>
	 * This array contains the names of the columns in the target table that
	 * contain the auto-generated keys that should be made available. The driver
	 * will ignore the array if the SQL statement is not an INSERT statement, or
	 * an SQL statement able to return auto-generated keys (the list of such
	 * statements is vendor-specific).
	 *
	 * @param string string
	 * @param strings strings
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return either the row count for INSERT, UPDATE, or DELETE statements, or 0
	 * for SQL statements that return nothing 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int executeUpdate(String string, String[] strings) throws SQLException {
		final String logSQL = "EXECUTING UPDATE: " + string;
		database.printSQLIfRequested(logSQL);
		LOG.debug(logSQL);
		return getInternalStatement().executeUpdate(string, strings);
	}

	/**
	 * Executes the given SQL statement, which may return multiple results, and
	 * signals the driver that any auto-generated keys should be made available
	 * for retrieval.
	 * <P>
	 * The driver will ignore this signal if the SQL statement is not an INSERT
	 * statement, or an SQL statement able to return auto-generated keys (the list
	 * of such statements is vendor-specific). In some (uncommon) situations, a
	 * single SQL statement may return multiple result sets and/or update counts.
	 * Normally you can ignore this unless you are (1) executing a stored
	 * procedure that you know may return multiple results or (2) you are
	 * dynamically executing an unknown SQL string.
	 * <P>
	 * The execute method executes an SQL statement and indicates the form of the
	 * first result. You must then use the methods getResultSet or getUpdateCount
	 * to retrieve the result, and getMoreResults to move to any subsequent
	 * result(s).
	 *
	 * @param string string
	 * @param i i
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return true if the first result is a ResultSet object; false if it is an
	 * update count or there are no results. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean execute(String string, int i) throws SQLException {
		final String logSQL = "EXECUTING: " + string;
		database.printSQLIfRequested(logSQL);
		LOG.debug(logSQL);
		return getInternalStatement().execute(string, i);
	}

	/**
	 * Executes the given SQL statement, which may return multiple results, and
	 * signals the driver that the auto-generated keys indicated in the given
	 * array should be made available for retrieval.
	 * <P>
	 * This array contains the indexes of the columns in the target table that
	 * contain the auto-generated keys that should be made available. The driver
	 * will ignore the array if the SQL statement is not an INSERT statement, or
	 * an SQL statement able to return auto-generated keys (the list of such
	 * statements is vendor-specific). Under some (uncommon) situations, a single
	 * SQL statement may return multiple result sets and/or update counts.
	 * Normally you can ignore this unless you are (1) executing a stored
	 * procedure that you know may return multiple results or (2) you are
	 * dynamically executing an unknown SQL string.
	 * <P>
	 * The execute method executes an SQL statement and indicates the form of the
	 * first result. You must then use the methods getResultSet or getUpdateCount
	 * to retrieve the result, and getMoreResults to move to any subsequent
	 * result(s).
	 *
	 * @param string string
	 * @param ints ints
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return true if the first result is a ResultSet object; false if it is an
	 * update count or there are no results 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean execute(String string, int[] ints) throws SQLException {
		final String logSQL = "EXECUTING: " + string;
		database.printSQLIfRequested(logSQL);
		LOG.debug(logSQL);
		return getInternalStatement().execute(string, ints);
	}

	/**
	 * Executes the given SQL statement, which may return multiple results, and
	 * signals the driver that the auto-generated keys indicated in the given
	 * array should be made available for retrieval.
	 * <P>
	 * This array contains the names of the columns in the target table that
	 * contain the auto-generated keys that should be made available. The driver
	 * will ignore the array if the SQL statement is not an INSERT statement, or
	 * an SQL statement able to return auto-generated keys (the list of such
	 * statements is vendor-specific). In some (uncommon) situations, a single SQL
	 * statement may return multiple result sets and/or update counts. Normally
	 * you can ignore this unless you are (1) executing a stored procedure that
	 * you know may return multiple results or (2) you are dynamically executing
	 * an unknown SQL string.
	 * <P>
	 * The execute method executes an SQL statement and indicates the form of the
	 * first result. You must then use the methods getResultSet or getUpdateCount
	 * to retrieve the result, and getMoreResults to move to any subsequent
	 * result(s).
	 *
	 * @param string string
	 * @param strings strings
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return true if the next result is a ResultSet object; false if it is an
	 * update count or there are no more results 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean execute(String string, String[] strings) throws SQLException {
		final String logSQL = "EXECUTING: " + string;
		database.printSQLIfRequested(logSQL);
		LOG.debug(logSQL);
		try {
			return getInternalStatement().execute(string, strings);
		} catch (SQLException exp) {
			return addFeatureAndAttemptExecuteAgain(exp, string, strings);
		}
	}

	/**
	 * Retrieves the result set holdability for ResultSet objects generated by
	 * this Statement object.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return either ResultSet.HOLD_CURSORS_OVER_COMMIT or
	 * ResultSet.CLOSE_CURSORS_AT_COMMIT 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public int getResultSetHoldability() throws SQLException {
		return getInternalStatement().getResultSetHoldability();
	}

	/**
	 * Retrieves whether this Statement object has been closed. A Statement is
	 * closed if the method close has been called on it, or if it is automatically
	 * closed.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true if this Statement object is closed; false if it is still open
	 * 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean isClosed() throws SQLException {
		if (database.getDefinition().supportsStatementIsClosed()) {
			return getInternalStatement().isClosed();
		} else {
			return isClosed;
		}
	}

	/**
	 * Requests that a Statement be pooled or not pooled.
	 * <P>
	 * The value specified is a hint to the statement pool implementation
	 * indicating whether the applicaiton wants the statement to be pooled. It is
	 * up to the statement pool manager as to whether the hint is used. The
	 * poolable value of a statement is applicable to both internal statement
	 * caches implemented by the driver and external statement caches implemented
	 * by application servers and other applications.
	 * <P>
	 * By default, a Statement is not poolable when created, and a
	 * PreparedStatement and CallableStatement are poolable when created.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param bln bln
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void setPoolable(boolean bln) throws SQLException {
		getInternalStatement().setPoolable(bln);
	}

	/**
	 * Returns a value indicating whether the Statement is poolable or not.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true if the Statement is poolable; false otherwise
	 *
	 * 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean isPoolable() throws SQLException {
		return getInternalStatement().isPoolable();
	}

	/**
	 * Returns an object that implements the given interface to allow access to
	 * non-standard methods, or standard methods not exposed by the proxy.
	 *
	 * If the receiver implements the interface then the result is the receiver or
	 * a proxy for the receiver. If the receiver is a wrapper and the wrapped
	 * object implements the interface then the result is the wrapped object or a
	 * proxy for the wrapped object. Otherwise return the the result of calling
	 * <code>unwrap</code> recursively on the wrapped object or a proxy for that
	 * result. If the receiver is not a wrapper and does not implement the
	 * interface, then an <code>SQLException</code> is thrown.
	 *
	 * @param iface A Class defining an interface that the result must implement.
	 * @param <T> the required interface.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an object that implements the interface. May be a proxy for the
	 * actual implementing object. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return getInternalStatement().unwrap(iface);
	}

	/**
	 * Returns true if this either implements the interface argument or is
	 * directly or indirectly a wrapper for an object that does. Returns false
	 * otherwise. If this implements the interface then return true, else if this
	 * is a wrapper then return the result of recursively calling
	 * <code>isWrapperFor</code> on the wrapped object. If this does not implement
	 * the interface and is not a wrapper, return false. This method should be
	 * implemented as a low-cost operation compared to <code>unwrap</code> so that
	 * callers can use this method to avoid expensive <code>unwrap</code> calls
	 * that may fail. If this method returns true then calling <code>unwrap</code>
	 * with the same argument should succeed.
	 *
	 * @param iface a Class defining an interface.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return true if this implements the interface or directly or indirectly
	 * wraps an object that does.
	 * @throws java.sql.SQLException if an error occurs while determining whether
	 * this is a wrapper for an object with the given interface.
	 * @since 1.6
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return getInternalStatement().isWrapperFor(iface);
	}

	private void setBatchHasEntries(boolean b) {
		batchHasEntries = b;
	}

	/**
	 * Indicates that a batch has been added.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the batch has un-executed entries, otherwise FALSE.
	 */
	public boolean getBatchHasEntries() {
		return batchHasEntries;
	}

	/**
	 * Unsupported.
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public void closeOnCompletion() throws SQLException {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Unsupported.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return unsupported 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the internalStatement
	 * @throws java.sql.SQLException
	 */
	protected synchronized Statement getInternalStatement() throws SQLException {
		if (this.internalStatement == null) {
//			System.out.println("OPENING DBSTATEMENT");
			this.setInternalStatement(connection.createStatement());
		}
		return this.internalStatement;
	}

	/**
	 * @param realStatement the internalStatement to set
	 */
	protected synchronized void setInternalStatement(Statement realStatement) {
		this.internalStatement = realStatement;
	}

	private void checkForBrokenConnection(Exception exp, String sql) throws SQLException {
		final String message = exp.getMessage().toLowerCase();
		if (message.matches(".*connection.*broken.*")
				|| message.matches(".*connection.*closed.*")
				|| message.matches(".*statement.*broken.*")
				|| message.matches(".*statement.*closed.*")) {
			replaceBrokenConnection();
		}
	}
}
