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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;

/**
 * Stores all the required functionality to use an H2 database in memory.
 *
 * @author Gregory Graham
 */
public class H2MemoryDB extends H2DB {

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
	public H2MemoryDB() throws SQLException {
		this(new H2MemorySettingsBuilder());
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
	public H2MemoryDB(H2MemorySettingsBuilder dataSource) throws SQLException {
		super(dataSource);
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
	public H2MemoryDB(String jdbcURL, String username, String password) throws SQLException {
		this(new H2MemorySettingsBuilder().fromJDBCURL(jdbcURL, username, password));

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
	public H2MemoryDB(DataSource dataSource) throws SQLException {
		super(dataSource);
	}

	/**
	 * Creates a DBDatabase instance for an H2 Memory database with the given
	 * database name, user and password.
	 *
	 * <p>
	 * The dummy parameter is ignored and only used to differentiate between the
	 * to 2 constructors.
	 *
	 * <p>
	 * It is recommended that you use
	 * {@link #H2MemoryDB(nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder) the settings builder constructor}
	 * instead.</p>
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param databaseName databaseName
	 * @param username username
	 * @param password password
	 * @param dummy dummy
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public H2MemoryDB(String databaseName, String username, String password, boolean dummy) throws SQLException {
		this(new H2MemorySettingsBuilder().setDatabaseName(databaseName).setUsername(username).setPassword(password));
	}

	/**
	 * Creates a new database with random (UUID based) name and label.
	 *
	 * <p>
	 * Great for we you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @return a new unique H2 Memory database
	 * @throws SQLException may throw database errors during initialization.
	 */
	public static H2MemoryDB createANewRandomDatabase() throws SQLException {
		return createANewRandomDatabase("", "");
	}

	/**
	 * Creates a new database with random (UUID based) name and label.
	 *
	 * <p>
	 * Great for we you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @param prefix the string to add before the database name and label
	 * @param suffix the string to add after the database name and label
	 * @return a new unique H2 Memory database
	 * @throws SQLException may throw database errors during initialization.
	 */
	public static H2MemoryDB createANewRandomDatabase(String prefix, String suffix) throws SQLException {
		final H2MemorySettingsBuilder settings = new H2MemorySettingsBuilder().withUniqueDatabaseName();
		settings.setDatabaseName(prefix + settings.getDatabaseName() + suffix);
		settings.setLabel(settings.getDatabaseName());
		return new H2MemoryDB(settings);
	}

	/**
	 * Creates a new database with designated label
	 *
	 * <p>
	 * Great for we you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @param label the database label to be used internally to identify the
	 * database (not related to the database name)
	 * @return a new unique H2 Memory database
	 * @throws SQLException may throw database errors during initialization.
	 */
	public static H2MemoryDB createDatabase(String label) throws SQLException {
		return new H2MemoryDB(new H2MemorySettingsBuilder().setLabel(label));
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param dcs a collection of settings the fully specify the database
	 * @throws java.sql.SQLException database errors
	 */
	public H2MemoryDB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new H2MemorySettingsBuilder().fromSettings(dcs));
	}

	@Override
	public H2MemoryDB clone() throws CloneNotSupportedException {
		return (H2MemoryDB) super.clone();
	}
  
  @Override
  final public DBConnection getConnection() throws UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver, SQLException{
    return super.getConnection();
  }

	@Override
	public H2MemorySettingsBuilder getURLInterpreter() {
		return new H2MemorySettingsBuilder();
	}

	@Override
	public boolean isMemoryDatabase() {
		return true;
	}
}
