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

import nz.co.gregs.dbvolution.DBDatabase;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.PostgresDBDefinition;

/**
 * A DBDatabase tweaked for PostgreSQL.
 *
 * @author Gregory Graham
 */
public class PostgresDB extends DBDatabase {

	private static final String POSTGRES_DRIVER_NAME = "org.postgresql.Driver";

	/**
	 * Creates a PostgreSQL connection for the DataSource.
	 *
	 * @param ds
	 */
	public PostgresDB(DataSource ds) {
		super(new PostgresDBDefinition(), ds);
	}

	/**
	 * Creates a PostgreSQL connection for the JDBC URL, username, and password.
	 *
	 * @param jdbcURL
	 * @param username
	 * @param password
	 */
	public PostgresDB(String jdbcURL, String username, String password) {
		super(new PostgresDBDefinition(), POSTGRES_DRIVER_NAME, jdbcURL, username, password);
	}

	/**
	 * Creates a PostgreSQL connection to the server on the port supplied, using
	 * the username and password supplied.
	 *
	 * @param hostname
	 * @param port
	 * @param databaseName
	 * @param username
	 * @param password
	 */
	public PostgresDB(String hostname, int port, String databaseName, String username, String password) {
		this(hostname, port, databaseName, username, password, null);
	}

	/**
	 * Creates a PostgreSQL connection to the server on the port supplied, using
	 * the username and password supplied.
	 *
	 * <p>
	 * Extra parameters to be added to the JDBC URL can be included in the
	 * urlExtras parameter.
	 *
	 * @param hostname
	 * @param port
	 * @param databaseName
	 * @param username
	 * @param password
	 * @param urlExtras
	 */
	public PostgresDB(String hostname, int port, String databaseName, String username, String password, String urlExtras) {
		super(new PostgresDBDefinition(),
				POSTGRES_DRIVER_NAME,
				"jdbc:postgresql://" + hostname + ":" + port + "/" + databaseName + (urlExtras == null || urlExtras.isEmpty() ? "" : "?" + urlExtras),
				username, password);
	}

	/**
	 * Creates a PostgreSQL connection to local computer("localhost") on the
	 * default port(5432) using the username and password supplied.
	 *
	 * <p>
	 * Extra parameters to be added to the JDBC URL can be included in the
	 * urlExtras parameter.
	 *
	 * @param databaseName
	 * @param username
	 * @param password
	 * @param urlExtras
	 */
	public PostgresDB(String databaseName, String username, String password, String urlExtras) {
		this("localhost", 5432, databaseName, username, password, urlExtras);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

}
