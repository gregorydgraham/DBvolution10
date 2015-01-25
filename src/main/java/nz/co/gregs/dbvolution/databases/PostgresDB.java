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

import java.io.File;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.PostgresDBDefinition;

/**
 * A DBDatabase tweaked for PostgreSQL.
 *
 * @author Gregory Graham
 */
public class PostgresDB extends DBDatabase {

	private static final String POSTGRES_DRIVER_NAME = "org.postgresql.Driver";

	/**
	 * The default port number used by PostgreSQL.
	 */
	public static final int POSTGRES_DEFAULT_PORT = 5432;

	/**
	 * The default username used by PostgreSQL.
	 */
	public static final String POSTGRES_DEFAULT_USERNAME = "postgres";

	/**
	 * Creates a PostgreSQL connection for the DataSource.
	 *
	 * @param ds	ds
	 */
	public PostgresDB(DataSource ds) {
		super(new PostgresDBDefinition(), ds);
	}

	/**
	 * Creates a PostgreSQL connection for the JDBC URL, username, and password.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public PostgresDB(String jdbcURL, String username, String password) {
		super(new PostgresDBDefinition(), POSTGRES_DRIVER_NAME, jdbcURL, username, password);
	}

	/**
	 * Creates a PostgreSQL connection to the server on the port supplied, using
	 * the username and password supplied.
	 *
	 * @param hostname hostname
	 * @param port port
	 * @param databaseName databaseName
	 * @param username username
	 * @param password password
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
	 * @param hostname hostname
	 * @param password password
	 * @param databaseName databaseName
	 * @param port port
	 * @param username username
	 * @param urlExtras urlExtras
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
	 * @param databaseName databaseName
	 * @param username username
	 * @param password password
	 * @param urlExtras urlExtras
	 */
	public PostgresDB(String databaseName, String username, String password, String urlExtras) {
		this("localhost", POSTGRES_DEFAULT_PORT, databaseName, username, password, urlExtras);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Assumes that the database and application are on the the same machine.
	 *
	 * @param table
	 * @param file
	 * @param delimiter
	 * @param nullValue
	 * @param escapeCharacter
	 * @param quoteCharacter
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing 1 Database
	 * exceptions may be thrown
	 * @throws SQLException
	 */
	public int loadFromCSVFile(DBRow table, File file, String delimiter, String nullValue, String escapeCharacter, String quoteCharacter) throws SQLException {
		int returnValue = 0;
		final DBStatement dbStatement = this.getDBStatement();
		try {
			returnValue = dbStatement.executeUpdate("COPY " + table.getTableName() + " FROM '" + file.getAbsolutePath() + "' WITH (DELIMITER '" + delimiter + "', NULL '" + nullValue + "', ESCAPE '" + escapeCharacter + "', FORMAT csv, QUOTE '" + quoteCharacter + "');");
		} finally {
			dbStatement.close();
		}
		return returnValue;
	}

	/**
	 * Create a new database/schema on this database server.
	 *
	 * <p>
	 * Generally requires all sorts of privileges and is best performed by
	 * database administrator (DBA).
	 *
	 * @param databaseName the name of the new database
	 * @throws SQLException
	 */
	public void createDatabase(String databaseName) throws SQLException {
		String sqlString = "CREATE DATABASE " + databaseName + ";";
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);
		} finally {
			dbStatement.close();
		}
	}

	/**
	 * Create a new database/schema on this database server.
	 *
	 * <p>
	 * Generally requires all sorts of privileges and is best performed by
	 * database administrator (DBA).
	 * 
	 * @param username
	 * @param password
	 * @throws SQLException
	 */
	public void createUser(String username, String password) throws SQLException {
		String sqlString = "CREATE USER \"" + username.replaceAll("\\\"", "") + "\" WITH PASSWORD '" + password.replaceAll("'", "") + "';";
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);
		} finally {
			dbStatement.close();
		}
	}
}
