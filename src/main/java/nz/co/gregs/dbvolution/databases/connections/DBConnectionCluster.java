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
package nz.co.gregs.dbvolution.databases.connections;

import nz.co.gregs.dbvolution.databases.*;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 *
 * @author greg
 */
public class DBConnectionCluster implements DBConnection {

	private final DBDatabaseCluster databaseCluster;
	
	public DBConnectionCluster(DBDatabaseCluster cluster) {
		this.databaseCluster = cluster;
	}

	@Override
	public DBStatement createDBStatement() throws SQLException {
		return databaseCluster.getDBStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareStatement(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareCall(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support nativeSQL(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setAutoCommit(boolean) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getAutoCommit() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void commit() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support commit() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void rollback() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support rollback() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void close() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support close() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support isClosed() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getMetaData() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setReadOnly() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support isReadOnly() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setCatalog() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getCatalog() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getCatalog() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setTransactionIsolation() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getTransactionIsolation() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getWarnings() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support clearWarnings() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support createStatment() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareStatement() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareCall() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getTypeMap() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setTypeMap() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setHoldability() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getHoldability() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setSavePoint() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setSavePoint(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support rollback(Savepoint) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support releaseSavepoint(Savepoint) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support createStatement(int, int, int) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareStatement(String, int, int, int) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareCall(String, int, int, int) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareStatement(String, int) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareStatement(String, int[]) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support prepareStatement(String, String[]) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support createClob() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support createBlob() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support createNclob() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support createSQLXML() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support isValid(int) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setClientInfo(String, String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setClientInfo(Properties) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getClientInfo(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getCientInfo() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support createArrayOf(String, Object[]) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support ctreateStruct(String, Object[]) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setSchema(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getSchema() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getSchema() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support abort() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support setNetworkTimeout(Executor, int) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support getNetworkTimeout() yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support unwrap(Class<T>) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException("DBConnectionCluster does not support isWrapperFor(Class<?>) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Statement getInternalStatement() throws SQLException {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
}
