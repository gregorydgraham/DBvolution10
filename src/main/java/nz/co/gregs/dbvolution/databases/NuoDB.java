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
import nz.co.gregs.dbvolution.databases.settingsbuilders.NuoDBSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;

/**
 * DBDatabase tweaked to work best with NuoDB.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class NuoDB extends DBDatabase {

//	private static final int NUODB_DEFAULT_PORT = 48004;
	public static final String NUODB_DRIVER = "com.nuodb.jdbc.Driver";
//	private static final String NUODB_URL_PREFIX = "jdbc:com.nuodb://";
	public static final long serialVersionUID = 1l;

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
	 */
	public NuoDB(DataSource ds) throws SQLException {
		super(
				new NuoDBSettingsBuilder().setDataSource(ds)
		);
//		super(new NuoDBDefinition(), NUODB_DRIVER, ds);
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
	 * @throws java.sql.SQLException
	 */
	public NuoDB(List<String> brokers, String databaseName, String schema, String username, String password) throws SQLException {
		super(
				new NuoDBSettingsBuilder()
						.setDatabaseName(databaseName)
						.setSchema(schema)
						.setUsername(username)
						.setPassword(password)
						.addBrokers(brokers)
		);
//		StringBuilder hosts = new StringBuilder();
//		String sep = "";
//
//		for (String server : brokers) {
//			int port = NUODB_DEFAULT_PORT;
//			hosts.append(sep)
//					.append(server)
//					.append(":")
//					.append(port);
//			sep = ",";
//		}
//
//		setDriverName(NUODB_DRIVER);
//		setDefinition(new NuoDBDefinition());
//		setJdbcURL(NUODB_URL_PREFIX + hosts + "/" + databaseName + "?schema=" + schema);
//		setUsername(username);
//		setPassword(password);
//		setDatabaseName(databaseName);
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
	 * @throws java.sql.SQLException
	 */
	public NuoDB(String broker, Long port, String databaseName, String schema, String username, String password) throws SQLException {
			super(
				new NuoDBSettingsBuilder()
						.setHost(broker)
						.setPort(port)
						.setDatabaseName(databaseName)
						.setSchema(schema)
						.setUsername(username)
						.setPassword(password)
		);
//		List<String> brokers = new ArrayList<String>();
//		List<Long> ports = new ArrayList<Long>();
//		brokers.add(broker);
//		ports.add(port);
//		initNuoDB(brokers, ports, databaseName, schema, username, password);
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
	 * @throws java.sql.SQLException
	 */
	public NuoDB(List<String> brokers, List<Long> ports, String databaseName, String schema, String username, String password) throws SQLException {
			super(
				new NuoDBSettingsBuilder()
						.setDatabaseName(databaseName)
						.setSchema(schema)
						.setUsername(username)
						.setPassword(password)
						.addBrokers(brokers, ports)
		);
//		initNuoDB(brokers, ports, databaseName, schema, username, password);
	}

//	private void initNuoDB(List<String> brokers, List<Long> ports, String databaseName, String schema, String username, String password) {
//		StringBuilder hosts = new StringBuilder();
//		String sep = "";
//		if (brokers.size() == ports.size()) {
//			for (int i = 0; i < brokers.size(); i++) {
//				String server = brokers.get(i);
//				Long port = ports.get(i);
//				hosts.append(sep)
//						.append(server)
//						.append(":")
//						.append(port);
//				sep = ",";
//			}
//		}
//		setDriverName(NUODB_DRIVER);
//		setDefinition(new NuoDBDefinition());
//		setJdbcURL(NUODB_URL_PREFIX + hosts + "/" + databaseName + "?schema=" + schema);
//		setUsername(username);
//		setPassword(password);
//		setDatabaseName(databaseName);
//	}

//	@Override
//	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : NUODB_URL_PREFIX
//				+ settings.getHost() + "/"
//				+ settings.getDatabaseName() + "?schema=" + getSettings().getSchema();
//	}
	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

//	@Override
//	protected Map<String, String> getExtras() {
//		String jdbcURL = getJdbcURL();
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split("?", 2)[1];
//			return DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", "");
//		} else {
//			return new HashMap<String, String>();
//		}
//	}
//	@Override
//	protected String getHost() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:com.nuodb://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.split(":")[0];
//		
//	}
//	@Override
//	protected String getDatabaseInstance() {
//		String jdbcURL = getJdbcURL();
//		return getExtras().get("instance");
//	}
//	@Override
//	protected String getPort() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:com.nuodb://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.replaceAll("^[^:]*:+", "");
//	}
//	@Override
//	protected String getSchema() {
//		return "";
//	}
//	@Override
//	protected DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
//		DatabaseConnectionSettings set = new DatabaseConnectionSettings();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:com.nuodb://", "");
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split("?", 2)[1];
//			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
//		}
//		set.setPort(noPrefix
//					.split("/",2)[0]
//					.replaceAll("^[^:]*:+", ""));
//		set.setHost(noPrefix
//					.split("/",2)[0]
//					.split(":")[0]);
//		set.setInstance(getExtras().get("instance"));
//		set.setSchema("");
//		return set;
//	}
	@Override
	public Integer getDefaultPort() {
		return 8888;
	}

//	@Override
//	protected Class<? extends DBDatabase> getBaseDBDatabaseClass() {
//		return NuoDB.class;
//	}
	@Override
	public NuoDBSettingsBuilder getURLInterpreter() {
		return new NuoDBSettingsBuilder();
	}
}
