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
import nz.co.gregs.dbvolution.DBDatabase;
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
 * @author Gregory Graham
 */
public class DBStatement implements Statement {

	static final private Log log = LogFactory.getLog(DBStatement.class);

	private Statement internalStatement;
	private boolean batchHasEntries;
	private final DBDatabase database;
	private final Connection connection;

	public DBStatement(DBDatabase db, Connection connection) throws SQLException {
		this.database = db;
		this.connection = connection;
		this.internalStatement = connection.createStatement();
	}

	@Override
	public ResultSet executeQuery(String string) throws SQLException {
		final String logSQL = "EXECUTING QUERY: " + string;
		database.printSQLIfRequested(logSQL);
		log.info(logSQL);
		return getInternalStatement().executeQuery(string);
	}

	@Override
	public int executeUpdate(String string) throws SQLException {
		return getInternalStatement().executeUpdate(string);
	}

	/**
	 * Closes the Statement and the Connection.
	 *
	 * <p>
	 * Also informs the DBDatabase that there is one less connection to the
	 * database.
	 *
	 * @throws SQLException
	 */
	@Override
	public void close() throws SQLException {
		try {
			getInternalStatement().close();
			getConnection().close();
			database.connectionClosed(getConnection());
		} catch (SQLException e) {
			// Someone please tell me how you are supposed to cope 
			// with an exception during the close method????????
			log.warn("Exception occurred during close(): " + e.getMessage(), e);
			throw e;
//            e.printStackTrace(System.err);
		}
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return getInternalStatement().getMaxFieldSize();
	}

	@Override
	public void setMaxFieldSize(int i) throws SQLException {
		getInternalStatement().setMaxFieldSize(i);
	}

	@Override
	public int getMaxRows() throws SQLException {
		return getInternalStatement().getMaxRows();
	}

	@Override
	public void setMaxRows(int i) throws SQLException {
		getInternalStatement().setMaxRows(i);
	}

	@Override
	public void setEscapeProcessing(boolean bln) throws SQLException {
		getInternalStatement().setEscapeProcessing(bln);
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return getInternalStatement().getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int i) throws SQLException {
		getInternalStatement().setQueryTimeout(i);
	}

	@Override
	public void cancel() throws SQLException {
		getInternalStatement().cancel();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return getInternalStatement().getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		getInternalStatement().clearWarnings();
	}

	@Override
	public void setCursorName(String string) throws SQLException {
		getInternalStatement().setCursorName(string);
	}

	@Override
	public boolean execute(String string) throws SQLException {
		final String logSQL = "EXECUTING: " + string;
		database.printSQLIfRequested(logSQL);
		log.info(logSQL);
		return getInternalStatement().execute(string);
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return getInternalStatement().getResultSet();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return getInternalStatement().getUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return getInternalStatement().getMoreResults();
	}

	@Override
	public void setFetchDirection(int i) throws SQLException {
		getInternalStatement().setFetchDirection(i);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return getInternalStatement().getFetchDirection();
	}

	@Override
	public void setFetchSize(int i) throws SQLException {
		getInternalStatement().setFetchSize(i);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return getInternalStatement().getFetchSize();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return getInternalStatement().getResultSetConcurrency();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return getInternalStatement().getResultSetType();
	}

	@Override
	public void addBatch(String string) throws SQLException {
		getInternalStatement().addBatch(string);
		setBatchHasEntries(true);
	}

	@Override
	public void clearBatch() throws SQLException {
		getInternalStatement().clearBatch();
		setBatchHasEntries(false);
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return getInternalStatement().executeBatch();
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean getMoreResults(int i) throws SQLException {
		return getInternalStatement().getMoreResults();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return getInternalStatement().getGeneratedKeys();
	}

	@Override
	public int executeUpdate(String string, int i) throws SQLException {
		return getInternalStatement().executeUpdate(string, i);
	}

	@Override
	public int executeUpdate(String string, int[] ints) throws SQLException {
		return getInternalStatement().executeUpdate(string, ints);
	}

	@Override
	public int executeUpdate(String string, String[] strings) throws SQLException {
		final String logSQL = "EXECUTING UPDATE: " + string;
		database.printSQLIfRequested(logSQL);
		log.info(logSQL);
		return getInternalStatement().executeUpdate(string, strings);
	}

	@Override
	public boolean execute(String string, int i) throws SQLException {
		final String logSQL = "EXECUTING: " + string;
		database.printSQLIfRequested(logSQL);
		log.info(logSQL);
		return getInternalStatement().execute(string, i);
	}

	@Override
	public boolean execute(String string, int[] ints) throws SQLException {
		final String logSQL = "EXECUTING: " + string;
		database.printSQLIfRequested(logSQL);
		log.info(logSQL);
		return getInternalStatement().execute(string, ints);
	}

	@Override
	public boolean execute(String string, String[] strings) throws SQLException {
		final String logSQL = "EXECUTING: " + string;
		database.printSQLIfRequested(logSQL);
		log.info(logSQL);
		return getInternalStatement().execute(string, strings);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return getInternalStatement().getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return getInternalStatement().isClosed();
	}

	@Override
	public void setPoolable(boolean bln) throws SQLException {
		getInternalStatement().setPoolable(bln);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return getInternalStatement().isPoolable();
	}

	@Override
	public <T> T unwrap(Class<T> type) throws SQLException {
		return getInternalStatement().unwrap(type);
	}

	@Override
	public boolean isWrapperFor(Class<?> type) throws SQLException {
		return getInternalStatement().isWrapperFor(type);
	}

	public void setBatchHasEntries(boolean b) {
		batchHasEntries = b;
	}

	public boolean getBatchHasEntries() {
		return batchHasEntries;
	}

	public void closeOnCompletion() throws SQLException {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	public boolean isCloseOnCompletion() throws SQLException {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * @return the internalStatement
	 */
	protected Statement getInternalStatement() {
		return internalStatement;
	}

	/**
	 * @param realStatement the internalStatement to set
	 */
	protected void setInternalStatement(Statement realStatement) {
		this.internalStatement = realStatement;
	}
}
