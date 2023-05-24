/*
 * Copyright 2014 Gregory Graham.
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
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MariaClusterDBSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;

/**
 * DBDatabase tweaked for a Maria Cluster Database.
 *
 * <p>
 * You should probably be using {@link MariaClusterDB#MariaClusterDB(java.util.List, java.util.List, java.lang.String, java.lang.String, java.lang.String)
 * }
 *
 * @author Gregory Graham
 */
public class MariaClusterDB extends DBDatabaseImplementation {

	public final static String MARIADBDRIVERNAME = "com.mariadb.jdbc.Driver";
	public static final long serialVersionUID = 1l;

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
	 */
	public MariaClusterDB(DataSource ds) throws SQLException {
		super(
				new MariaClusterDBSettingsBuilder()
						.setDataSource(ds)
		);
	}

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
	 */
	public MariaClusterDB(DatabaseConnectionSettings ds) throws SQLException {
		this(new MariaClusterDBSettingsBuilder().fromSettings(ds));
	}

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
	 */
	public MariaClusterDB(MariaClusterDBSettingsBuilder ds) throws SQLException {
		super(ds);
	}

	/**
	 * Creates a DBDatabase instance for a MariaDB cluster hosted at the JDBC URL
	 * supplied, logging in with the username and password supplied.
	 *
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public MariaClusterDB(String jdbcURL, String username, String password) throws SQLException {
		this(new MariaClusterDBSettingsBuilder().fromJDBCURL(jdbcURL, username, password));
	}

	/**
	 * Creates a DBDatabase instance for a MariaDB cluster hosted at the server
	 * and port supplied, logging in with the username and password supplied.
	 *
	 * <p>
	 * You should probably be using
	 * {@link MariaClusterDB#MariaClusterDB(java.util.List, java.util.List, java.lang.String, java.lang.String, java.lang.String)}
	 *
	 * @param server server
	 * @param port port
	 * @param password password
	 * @param username username
	 * @param databaseName databaseName
	 * @throws java.sql.SQLException database errors
	 */
	public MariaClusterDB(String server, long port, String databaseName, String username, String password) throws SQLException {
		this(
				new MariaClusterDBSettingsBuilder()
						.setHost(server)
						.setPort(port)
						.setDatabaseName(databaseName)
						.setUsername(username)
						.setPassword(password)
		);
	}

	/**
	 * Creates a DBDatabase for a MariaDB cluster.
	 *
	 * <p>
	 * Supply multiple servers with each server's port defined in the equivalent
	 * entry in ports.
	 *
	 * @param servers servers
	 * @param ports ports
	 * @param databaseName databaseName
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public MariaClusterDB(List<String> servers, List<Long> ports, String databaseName, String username, String password) throws SQLException {
		super(
				new MariaClusterDBSettingsBuilder()
						.setDatabaseName(databaseName)
						.setUsername(username)
						.setPassword(password)
						.addHosts(servers, ports)
		);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

	@Override
	public Integer getDefaultPort() {
		return -1;
	}

	@Override
	public MariaClusterDBSettingsBuilder getURLInterpreter() {
		return new MariaClusterDBSettingsBuilder();
	}
}
