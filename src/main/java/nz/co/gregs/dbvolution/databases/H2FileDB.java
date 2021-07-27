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

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2FileSettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;

/**
 * Stores all the required functionality to use an H2 database in memory.
 *
 * @author Gregory Graham
 */
public class H2FileDB extends H2DB {

	private static final long serialVersionUID = 1l;

	/**
	 * Creates a DBDatabase instance for an H2 Memory database with the given JDBC
	 * URL, user and password.
	 *
	 * <p>
	 * - Database exceptions may be thrown</p>
	 *
	 * @throws java.sql.SQLException database errors
	 */
	public H2FileDB() throws SQLException {
		this(new DatabaseConnectionSettings());
	}

	/**
	 * Creates a DBDatabase instance for an H2 Memory database with the given JDBC
	 * URL, user and password.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2FileDB(H2FileSettingsBuilder dataSource) throws SQLException {
		super(dataSource);
		jamDatabaseConnectionOpen();
	}

	/**
	 * Creates a DBDatabase instance for an H2 Memory database with the given JDBC
	 * URL, user and password.
	 *
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2FileDB(String jdbcURL, String username, String password) throws SQLException {
		this(new H2FileSettingsBuilder().fromJDBCURL(jdbcURL, username, password));

	}

	/**
	 * Creates a DBDatabase instance for an H2 Memory database with the given JDBC
	 * URL, user and password.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2FileDB(DataSource dataSource) throws SQLException {
		super(dataSource);
		jamDatabaseConnectionOpen();
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public H2FileDB(DatabaseConnectionSettings dataSource) throws SQLException {
		super(dataSource);
	}

	@Override
	public H2FileDB clone() throws CloneNotSupportedException {
		return (H2FileDB) super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	private void jamDatabaseConnectionOpen() {
		try {
			this.storedConnection = getConnection();
			this.storedConnection.createDBStatement();
		} catch (UnableToCreateDatabaseConnectionException | UnableToFindJDBCDriver | SQLException ex) {
			Logger.getLogger(H2DB.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Override
	public H2MemorySettingsBuilder getURLInterpreter() {
		return new H2MemorySettingsBuilder();
	}

	@Override
	public boolean isMemoryDatabase() {
		return false;
	}
}
