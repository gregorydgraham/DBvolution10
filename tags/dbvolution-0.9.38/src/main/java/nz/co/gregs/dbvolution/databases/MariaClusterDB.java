/*
 * Copyright 2014 gregory.graham.
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

import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.MariaDBDefinition;

/**
 * DBDatabase tweaked for a Maria Cluster Database.
 *
 * <p>
 * You should probably be using {@link MariaClusterDB#MariaClusterDB(java.util.List, java.util.List, java.lang.String, java.lang.String, java.lang.String)
 * }
 *
 * @author gregorygraham
 */
public class MariaClusterDB extends DBDatabase {

	private final static String MARIADBDRIVERNAME = "com.mariadb.jdbc.Driver";

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds
	 */
	public MariaClusterDB(DataSource ds) {
		super(new MariaDBDefinition(), ds);
	}

	/**
	 * Creates a DBDatabase instance for a MariaDB cluster hosted at the JDBC
	 * URL supplied, logging in with the username and password supplied.
	 *
	 *
	 * @param jdbcURL
	 * @param username
	 * @param password
	 */
	public MariaClusterDB(String jdbcURL, String username, String password) {
		super(new MariaDBDefinition(), MARIADBDRIVERNAME, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance for a MariaDB cluster hosted at the server
	 * and port supplied, logging in with the username and password supplied.
	 *
	 * <p>
	 * You should probably be using
	 * {@link MariaClusterDB#MariaClusterDB(java.util.List, java.util.List, java.lang.String, java.lang.String, java.lang.String)}
	 *
	 * @param server
	 * @param port
	 * @param databaseName
	 * @param username
	 * @param password
	 */
	public MariaClusterDB(String server, long port, String databaseName, String username, String password) {
		super(new MariaDBDefinition(),
				MARIADBDRIVERNAME,
				"jdbc:mariadb://" + server + ":" + port + "/" + databaseName,
				username,
				password);
		this.setDatabaseName(databaseName);
	}

	/**
	 * Creates a DBDatabase for a MariaDB cluster.
	 *
	 * <p>
	 * Supply multiple servers with each server's port defined in the equivalent
	 * entry in ports.
	 *
	 * @param servers
	 * @param ports
	 * @param databaseName
	 * @param username
	 * @param password
	 */
	public MariaClusterDB(List<String> servers, List<Long> ports, String databaseName, String username, String password) {
		String hosts = "";
		String sep = "";
		if (servers.size() == ports.size()) {
			for (int i = 0; i < servers.size(); i++) {
				String server = servers.get(i);
				Long port = ports.get(i);
				hosts += sep + server + ":" + port;
				sep = ",";
			}
		}
		setDriverName(MARIADBDRIVERNAME);
		setDefinition(new MariaDBDefinition());
		setJdbcURL("jdbc:mariadb://" + hosts + "/" + databaseName);
		setUsername(username);
		setPassword(password);
		setDatabaseName(databaseName);
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

}
