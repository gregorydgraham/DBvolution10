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
	 * @param settings settings required to connect to the Informix server
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
	 * @param settings settings required to connect to the Informix server
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
	 * Creates a DBDatabase configured for Informix for the given data source.
	 *
	 * @param builder settings required to connect to the Informix server
	 * @throws SQLException the database may throw exceptions during initialization
	 */
	protected InformixDB(AbstractInformixSettingsBuilder<?, ?> builder) throws SQLException {
		super(builder);
	}

	/**
	 * 
	 * Creates a DBDatabase configured for Informix for the given data source.
	 *
	 *
	 * @param builder settings required to connect to the Informix server
	 * @throws SQLException 
	 */
	public InformixDB(InformixSettingsBuilder builder) throws SQLException {
		super(builder);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); 
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		// none implemented so far
		;
	}

	@Override
	public Integer getDefaultPort() {
		return 1526;
	}

	@Override
	public AbstractInformixSettingsBuilder<?, ?> getURLInterpreter() {
		return new InformixSettingsBuilder();
	}

}
