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
import java.sql.Statement;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.InformixSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.InformixDBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractInformixSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;

/**
 * A version of DBDatabase tweaked for Informix 7 and higher.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class InformixDB extends DBDatabase {

	private final static String INFORMIXDRIVERNAME = "com.informix.jdbc.IfxDriver";
	public static final long serialVersionUID = 1l;
	public static final int DEFAULT_PORT = 1526;
	private String derivedURL;

	/**
	 * Create a database object for a Informix 7+ database using the supplied
	 * definition and datasource.
	 *
	 * @param definition the DBDefiition that should be used with this database.
	 * Usually this will be a {@link InformixDBDefinition} but other definitions
	 * can be supplied.
	 * @param ds the data source that defines the connection to the database.
	 * @throws java.sql.SQLException database errors
	 */
	protected InformixDB(DBDefinition definition, DataSource ds) throws SQLException {
		super(
				new InformixSettingsBuilder()
						.setDataSource(ds)
						.setDefinition(definition)
		);
//		super(definition, INFORMIXDRIVERNAME, ds);
		// Informix causes problems when using batched statements :(
		setBatchSQLStatementsWhenPossible(false);
	}

	/**
	 * Create a database object for a Informix 7+ database using the supplied
	 * definition and connection details.
	 *
	 * @param definition the DBDefiition that should be used with this database.
	 * Usually this will be a {@link InformixDBDefinition} but other definitions
	 * can be supplied.
	 * @param driverName the name of the driver class to use with this database.
	 * @param jdbcURL the JDBC URL to the database
	 * @param username the username to use when connecting to the database
	 * @param password the password to use when connecting
	 * @throws java.sql.SQLException database errors
	 */
	protected InformixDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		this(definition, driverName,
				new InformixSettingsBuilder()
						.fromJDBCURL(jdbcURL)
						.setUsername(username)
						.setPassword(password)
						.toSettings()
		//				jdbcURL, username, password
		);
		// Informix causes problems when using batched statements :(
		setBatchSQLStatementsWhenPossible(false);
	}

	/**
	 * Create a database object for a Informix 7+ database using the supplied
	 * definition and connection details.
	 *
	 * @param definition the DBDefiition that should be used with this database.
	 * Usually this will be a {@link InformixDBDefinition} but other definitions
	 * can be supplied.
	 * @param driverName the name of the driver class to use with this database.
	 * @param settings
	 * @throws java.sql.SQLException database errors
	 */
	protected InformixDB(DBDefinition definition, String driverName, DatabaseConnectionSettings settings) throws SQLException {
		super(new InformixSettingsBuilder().fromSettings(settings));
		// Informix causes problems when using batched statements :(
		setBatchSQLStatementsWhenPossible(false);
	}

	/**
	 * Create a database object for a Informix 7+ database using the supplied
	 * definition and connection details.
	 *
	 * @param settings
	 * @throws java.sql.SQLException database errors
	 */
	public InformixDB(DatabaseConnectionSettings settings) throws SQLException {
		this(new InformixDBDefinition(), INFORMIXDRIVERNAME, settings);
		// Informix causes problems when using batched statements :(
		setBatchSQLStatementsWhenPossible(false);
	}

	/**
	 * Creates a DBDatabase configured for Informix with the given JDBC URL,
	 * username, and password.
	 *
	 * <p>
	 * Remember to include the Informix JDBC driver in your classpath.
	 *
	 *
	 *
	 *
	 *
	 * - Database exceptions may be thrown
	 *
	 * @param jdbcURL jdbcURL the JDBC URL to use to connect to the database
	 * @param username username the username used for the connection
	 * @param password password the password required to connect the user to the
	 * database
	 * @throws java.sql.SQLException database errors
	 */
	public InformixDB(String jdbcURL, String username, String password) throws SQLException {
		this(new InformixDBDefinition(), INFORMIXDRIVERNAME, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase configured for Informix for the given data source.
	 *
	 * <p>
	 * Remember to include the Informix JDBC driver in your classpath.
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public InformixDB(DataSource dataSource) throws SQLException {
		this(new InformixDBDefinition(), dataSource);
	}

	/**
	 *
	 * @param builder
	 * @throws SQLException
	 */
	protected InformixDB(AbstractInformixSettingsBuilder<?, ?> builder) throws SQLException {
		super(builder);
	}

	public InformixDB(InformixSettingsBuilder builder) throws SQLException {
		super(builder);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		// none implemented so far
		;
	}

//	@Override
//	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : "jdbc:informix-sqli://"
//				+ settings.getHost() + ":"
//				+ settings.getPort() + "/"
//				+ settings.getDatabaseName() + ":INFORMIXSERVER="
//				+ settings.getInstance()
//				+ settings.formatExtras(":", "=", ";", "");
//	}
//	@Override
//	protected Map<String, String> getExtras() {
//		String jdbcURL = getJdbcURL();
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split(";", 2)[1];
//			return DatabaseConnectionSettings.decodeExtras(extrasString, ":", "=", ";", "");
//		} else {
//			return new HashMap<String, String>();
//		}
//	}
//	@Override
//	protected String getHost() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:informix-sqli://", "");
//		return noPrefix
//				.split("/", 2)[0]
//				.split(":")[0];
//
//	}
//	@Override
//	protected String getDatabaseInstance() {
//		return getExtras().get("INFORMIXSERVER");
//	}
//	@Override
//	protected String getPort() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:informix-sqli://", "");
//		return noPrefix
//				.split("/", 2)[0]
//				.replaceAll("^[^:]*:", "");
//	}
//	@Override
//	protected String getSchema() {
//		return "";
//	}
//	@Override
//	protected DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
//		DatabaseConnectionSettings set = new DatabaseConnectionSettings();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:informix-sqli://", "");
//		set.setPort(noPrefix
//				.split("/", 2)[0]
//				.replaceAll("^[^:]*:", ""));
//		set.setHost(noPrefix
//				.split("/", 2)[0]
//				.split(":")[0]);
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split(";", 2)[1];
//			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, ":", "=", ";", ""));
//		}
//		set.setInstance(getExtras().get("INFORMIXSERVER"));
//		return set;
//	}
	@Override
	public Integer getDefaultPort() {
		return 1526;
	}

//	@Override
//	protected  Class<? extends DBDatabase> getBaseDBDatabaseClass() {
//		return InformixDB.class;
//	}
	@Override
	public AbstractInformixSettingsBuilder<?, ?> getURLInterpreter() {
		return new InformixSettingsBuilder();
	}

}
