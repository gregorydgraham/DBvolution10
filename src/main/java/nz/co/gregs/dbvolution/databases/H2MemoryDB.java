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
		super(jdbcURL, username, password);
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
	 *
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param databaseName databaseName
	 * @param username username
	 * @param password password
	 * @param dummy dummy
	 */
	public H2MemoryDB(String databaseName, String username, String password, boolean dummy) {
		super("jdbc:h2:mem:" + databaseName, username, password);
		setDatabaseName(databaseName);
	}

	@Override
	public H2MemoryDB clone() throws CloneNotSupportedException {
		return (H2MemoryDB) super.clone(); //To change body of generated methods, choose Tools | Templates.
	}
}
