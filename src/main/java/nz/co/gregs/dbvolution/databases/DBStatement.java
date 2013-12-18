/*
 * Copyright 2013 gregory.graham.
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

/**
 *
 * @author gregory.graham
 */
public class DBStatement implements Statement {

    protected final Statement realStatement;
    private boolean batchHasEntries;
    private final DBDatabase database;

    public DBStatement(DBDatabase db, Statement realStatement) {
        this.database = db;
        this.realStatement = realStatement;
    }

    @Override
    public ResultSet executeQuery(String string) throws SQLException {
        return realStatement.executeQuery(string);
    }

    @Override
    public int executeUpdate(String string) throws SQLException {
        return realStatement.executeUpdate(string);
    }

    @Override
    public void close() throws SQLException {
        try {
            realStatement.close();
        }catch(SQLException e){
            // Someone please tell me how you are supposed to cope 
            // with an exception during the close method????????
            e.printStackTrace(System.err);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return realStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {
        realStatement.setMaxFieldSize(i);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return realStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int i) throws SQLException {
        realStatement.setMaxRows(i);
    }

    @Override
    public void setEscapeProcessing(boolean bln) throws SQLException {
        realStatement.setEscapeProcessing(bln);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return realStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {
        realStatement.setQueryTimeout(i);
    }

    @Override
    public void cancel() throws SQLException {
        realStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return realStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        realStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String string) throws SQLException {
        realStatement.setCursorName(string);
    }

    @Override
    public boolean execute(String string) throws SQLException {
        database.printSQLIfRequested(string);
        return realStatement.execute(string);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return realStatement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return realStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return realStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {
        realStatement.setFetchDirection(i);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return realStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
        realStatement.setFetchSize(i);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return realStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return realStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return realStatement.getResultSetType();
    }

    @Override
    public void addBatch(String string) throws SQLException {
        realStatement.addBatch(string);
        setBatchHasEntries(true);
    }

    @Override
    public void clearBatch() throws SQLException {
        realStatement.clearBatch();
        setBatchHasEntries(false);
    }

    @Override
    public int[] executeBatch() throws SQLException {
//        if (getBatchHasEntries()) {
//            setBatchHasEntries(false);
//        }
        return realStatement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return realStatement.getConnection();
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        return realStatement.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return realStatement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String string, int i) throws SQLException {
        return realStatement.executeUpdate(string, i);
    }

    @Override
    public int executeUpdate(String string, int[] ints) throws SQLException {
        return realStatement.executeUpdate(string, ints);
    }

    @Override
    public int executeUpdate(String string, String[] strings) throws SQLException {
        return realStatement.executeUpdate(string, strings);
    }

    @Override
    public boolean execute(String string, int i) throws SQLException {
        return realStatement.execute(string, i);
    }

    @Override
    public boolean execute(String string, int[] ints) throws SQLException {
        return realStatement.execute(string, ints);
    }

    @Override
    public boolean execute(String string, String[] strings) throws SQLException {
        return realStatement.execute(string, strings);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return realStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return realStatement.isClosed();
    }

    @Override
    public void setPoolable(boolean bln) throws SQLException {
        realStatement.setPoolable(bln);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return realStatement.isPoolable();
    }

//    @Override
//    public void closeOnCompletion() throws SQLException {
//        realStatement.closeOnCompletion();
//    }
//
//    @Override
//    public boolean isCloseOnCompletion() throws SQLException {
//        return realStatement.isCloseOnCompletion();
//    }
    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        return realStatement.unwrap(type);
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return realStatement.isWrapperFor(type);
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
}
