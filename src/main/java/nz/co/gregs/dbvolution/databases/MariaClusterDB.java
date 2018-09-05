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
import nz.co.gregs.dbvolution.databases.definitions.MariaDBDefinition;

/**
 * DBDatabase tweaked for a Maria Cluster Database.
 *
 * <p>
 * You should probably be using {@link MariaClusterDB#MariaClusterDB(java.util.List, java.util.List, java.lang.String, java.lang.String, java.lang.String)
 * }
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MariaClusterDB extends DBDatabase {

	private final static String MARIADBDRIVERNAME = "com.mariadb.jdbc.Driver";
	public static final long serialVersionUID = 1l;
	private String derivedURL;

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 */
	public MariaClusterDB(DataSource ds) throws SQLException {
		super(new MariaDBDefinition(), MARIADBDRIVERNAME, ds);
	}

	/**
	 * Creates a DBDatabase instance for a MariaDB cluster hosted at the JDBC URL
	 * supplied, logging in with the username and password supplied.
	 *
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public MariaClusterDB(String jdbcURL, String username, String password) throws SQLException {
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
	 * @param server server
	 * @param port port
	 * @param password password
	 * @param username username
	 * @param databaseName databaseName
	 */
	public MariaClusterDB(String server, long port, String databaseName, String username, String password) throws SQLException {
		super(new MariaDBDefinition(),
				MARIADBDRIVERNAME,
				"jdbc:mariadb://" + server + ":" + port + "/" + databaseName,
				username,
				password);
		this.setDatabaseName(databaseName);
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : "jdbc:mariadb://"
				+ settings.getHost() + ":"
				+ settings.getPort() + "/"
				+ settings.getDatabaseName();
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
	 */
	public MariaClusterDB(List<String> servers, List<Long> ports, String databaseName, String username, String password) {
		StringBuilder hosts = new StringBuilder();
		String sep = "";
		if (servers.size() == ports.size()) {
			for (int i = 0; i < servers.size(); i++) {
				String server = servers.get(i);
				Long port = ports.get(i);
				hosts.append(sep)
						.append(server)
						.append(":")
						.append(port);
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
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		;
	}

}
