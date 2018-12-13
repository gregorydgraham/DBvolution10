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
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.MariaDBDefinition;

/**
 * DBDatabase tweaked for a MariaDB Database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MariaDB extends DBDatabase {

	private final static String MARIADBDRIVERNAME = "com.mariadb.jdbc.Driver";
	public static final long serialVersionUID = 1l;
	private String derivedURL;

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 */
	public MariaDB(DataSource ds) throws SQLException {
		super(new MariaDBDefinition(), MARIADBDRIVERNAME, ds);
	}

	/**
	 * Create a MariaDB instance of DBDatabase for the database with the supplied
	 * JDBC URL, using the username and password to login.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public MariaDB(String jdbcURL, String username, String password) throws SQLException {
		super(new MariaDBDefinition(), MARIADBDRIVERNAME, jdbcURL, username, password);
	}

	/**
	 * Create a MariaDB instance of DBDatabase for the database on the supplied
	 * server and port, using the username and password to login.
	 *
	 * @param server server
	 * @param password password
	 * @param port port
	 * @param databaseName databaseName
	 * @param username username
	 */
	public MariaDB(String server, long port, String databaseName, String username, String password) throws SQLException {
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

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		;
	}

//	@Override
//	protected Map<String, String> getExtras() {
//		String jdbcURL = getJdbcURL();
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split(";", 2)[1];
//			return DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", "");
//		} else {
//			return new HashMap<String, String>();
//		}
//	}
//
//	@Override
//	protected String getHost() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:mariadb://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.split(":")[0];
//		
//	}
//
//	@Override
//	protected String getDatabaseInstance() {
//		String jdbcURL = getJdbcURL();
//		return getExtras().get("instance");
//	}
//
//	@Override
//	protected String getPort() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:mariadb://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.replaceAll("^[^:]*:", "");
//	}
//
//	@Override
//	protected String getSchema() {
//		return "";
//	}
@Override
	protected DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
		DatabaseConnectionSettings set = new DatabaseConnectionSettings();
		String noPrefix = jdbcURL.replaceAll("^jdbc:mariadb://", "");
		set.setPort(noPrefix
					.split("/",2)[0]
					.replaceAll("^[^:]*:+", ""));
		set.setHost(noPrefix
					.split("/",2)[0]
					.split(":")[0]);
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split(";", 2)[1];
			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		set.setInstance(getExtras().get("instance"));
		set.setSchema("");
		return set;
	}

	@Override
	public Integer getDefaultPort() {
		return 3306;
	}

}
