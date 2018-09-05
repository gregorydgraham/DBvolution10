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
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.NuoDBDefinition;

/**
 * DBDatabase tweaked to work best with NuoDB.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class NuoDB extends DBDatabase {

	private static final int NUODB_DEFAULT_PORT = 48004;
	private static final String NUODB_DRIVER = "com.nuodb.jdbc.Driver";
	private static final String NUODB_URL_PREFIX = "jdbc:com.nuodb://";
	public static final long serialVersionUID = 1l;
	private String derivedURL;

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 */
	public NuoDB(DataSource ds) throws SQLException {
		super(new NuoDBDefinition(), NUODB_DRIVER, ds);
	}

	/**
	 * Creates a DBDatabase instance tweaked for NuoDB using the broker supplied
	 * on the default port for NuoDB.
	 *
	 * @param brokers brokers
	 * @param schema schema
	 * @param databaseName databaseName
	 * @param password password
	 * @param username username
	 */
	public NuoDB(List<String> brokers, String databaseName, String schema, String username, String password) {
		StringBuilder hosts = new StringBuilder();
		String sep = "";

		for (String server : brokers) {
			int port = NUODB_DEFAULT_PORT;
			hosts.append(sep)
					.append(server)
					.append(":")
					.append(port);
			sep = ",";
		}

		setDriverName(NUODB_DRIVER);
		setDefinition(new NuoDBDefinition());
		setJdbcURL(NUODB_URL_PREFIX + hosts + "/" + databaseName + "?schema=" + schema);
		setUsername(username);
		setPassword(password);
		setDatabaseName(databaseName);
	}

	/**
	 * Creates a DBDatabase instance tweaked for NuoDB using the broker supplied
	 * using the ports supplied for each broker.
	 *
	 * @param broker a single NuoDB broker to use.
	 * @param port the port for the broker provided.
	 * @param databaseName the database required from the brokers.
	 * @param schema the schema on the database to be used.
	 * @param username the user to login as.
	 * @param password the user's password.
	 */
	public NuoDB(String broker, Long port, String databaseName, String schema, String username, String password) {
		List<String> brokers = new ArrayList<String>();
		List<Long> ports = new ArrayList<Long>();
		brokers.add(broker);
		ports.add(port);
		initNuoDB(brokers, ports, databaseName, schema, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for NuoDB using the broker supplied
	 * using the ports supplied for each broker.
	 *
	 * @param brokers a list of the NuoDB brokers to use.
	 * @param ports a list of the port for each broker provided.
	 * @param databaseName the database required from the brokers.
	 * @param schema the schema on the database to be used.
	 * @param username the user to login as.
	 * @param password the user's password.
	 */
	public NuoDB(List<String> brokers, List<Long> ports, String databaseName, String schema, String username, String password) {
		initNuoDB(brokers, ports, databaseName, schema, username, password);
	}

	private void initNuoDB(List<String> brokers, List<Long> ports, String databaseName, String schema, String username, String password) {
		StringBuilder hosts = new StringBuilder();
		String sep = "";
		if (brokers.size() == ports.size()) {
			for (int i = 0; i < brokers.size(); i++) {
				String server = brokers.get(i);
				Long port = ports.get(i);
				hosts.append(sep)
						.append(server)
						.append(":")
						.append(port);
				sep = ",";
			}
		}
		setDriverName(NUODB_DRIVER);
		setDefinition(new NuoDBDefinition());
		setJdbcURL(NUODB_URL_PREFIX + hosts + "/" + databaseName + "?schema=" + schema);
		setUsername(username);
		setPassword(password);
		setDatabaseName(databaseName);
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : NUODB_URL_PREFIX
				+ settings.getHost() + "/"
				+ settings.getDatabaseName() + "?schema=" + getSettings().getSchema();
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		;
	}

}
