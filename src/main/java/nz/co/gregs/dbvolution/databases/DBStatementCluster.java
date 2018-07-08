/*
 * Copyright 2017 greg.
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Random;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;

public class DBStatementCluster extends DBStatement {

	private final DBDatabaseCluster databaseCluster;

	public DBStatementCluster(DBDatabaseCluster db) {
		super(db, new DBConnectionCluster(db));
		this.databaseCluster = db;
	}

	@Override
	public boolean getBatchHasEntries() {
		throw new UnsupportedOperationException("DBStatementCluster.getBatchHasEntries is not yet implemented.");
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	public boolean execute(String string, String[] strings) throws SQLException {
		boolean executed = true;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed &= next.execute(string, strings);
		}
		return executed;
	}

	@Override
	public boolean execute(String string, int[] ints) throws SQLException {
		boolean executed = true;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed &= next.execute(string, ints);
		}
		return executed;
	}

	@Override
	public boolean execute(String string, int i) throws SQLException {
		boolean executed = true;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed &= next.execute(string, i);
		}
		return executed;
	}

	@Override
	public int executeUpdate(String string, String[] strings) throws SQLException {
		int executed = 0;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed = Math.max(executed, next.executeUpdate(string, strings));
		}
		return executed;
	}

	@Override
	public int executeUpdate(String string, int[] ints) throws SQLException {
		int executed = 0;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed = Math.max(executed, next.executeUpdate(string, ints));
		}
		return executed;
	}

	@Override
	public int executeUpdate(String string, int i) throws SQLException {
		int executed = 0;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed = Math.max(executed, next.executeUpdate(string, i));
		}
		return executed;
	}

	private DBStatement getRandomStatement() throws SQLException {
		Random rand = new Random();
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		DBStatement randomElement = dbStatements.get(rand.nextInt(dbStatements.size()));
		return randomElement;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return getRandomStatement().getGeneratedKeys();
	}

	@Override
	public boolean getMoreResults(int i) throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getMoreResults is not yet implemented.");
	}

	@Override
	public Connection getConnection() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getConnection should not be used.");
	}

	@Override
	public int[] executeBatch() throws SQLException {
		int[] executed = new int[]{};
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed = next.executeBatch();
		}
		return executed;
	}

	@Override
	public void clearBatch() throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.clearBatch();
		}
	}

	@Override
	public void addBatch(String string) throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.addBatch(string);
		}
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getResultSetType should not be used.");
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getResultSetConcurrency should not be used.");
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getFetchSize should not be used.");
	}

	@Override
	public void setFetchSize(int i) throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.setFetchSize should not be used.");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getFetchDirection should not be used.");
	}

	@Override
	public void setFetchDirection(int i) throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.setFetchDirection should not be used.");
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getMoreResults should not be used.");
	}

	@Override
	public int getUpdateCount() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getUpdateCount should not be used.");
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getResultSet should not be used.");
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		boolean executed = true;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed &= next.execute(sql);
		}
		return executed;
	}

	@Override
	public void setCursorName(String string) throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.setCursorName should not be used.");
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.clearWarnings should not be used.");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new UnsupportedOperationException("DBStatementCluster.getWarnings should not be used.");
	}

	@Override
	protected synchronized void replaceBrokenConnection() throws SQLException, UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver {
		throw new UnsupportedOperationException("DBStatementCluster.getWarnings should not be used.");
	}

	@Override
	public synchronized void cancel() throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.cancel();
		}
	}

	@Override
	public void setQueryTimeout(int i) throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.setQueryTimeout(i);
		}
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return getRandomStatement().getQueryTimeout();
	}

	@Override
	public void setEscapeProcessing(boolean bln) throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.setEscapeProcessing(bln);
		}
	}

	@Override
	public void setMaxRows(int i) throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.setMaxRows(i);
		}
	}

	@Override
	public int getMaxRows() throws SQLException {
		return getRandomStatement().getMaxRows();
	}

	@Override
	public void setMaxFieldSize(int i) throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.setMaxRows(i);
		}
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return getRandomStatement().getMaxFieldSize();
	}

	@Override
	public void close() throws SQLException {
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			next.close();
		}
	}

	@Override
	public int executeUpdate(String string) throws SQLException {
		int executed = 0;
		ArrayList<DBStatement> dbStatements = databaseCluster.getDBStatements();
		for (DBStatement next : dbStatements) {
			executed &= next.executeUpdate(string);
		}
		return executed;
	}

	@Override
	public ResultSet executeQuery(String string) throws SQLException {
		return getRandomStatement().executeQuery(string);
	}

}
