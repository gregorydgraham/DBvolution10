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
import nz.co.gregs.regexi.Regex;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MSSQLServerSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.MSSQLServerDBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractMSSQLServerSettingsBuilder;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.dbvolution.internal.sqlserver.*;

/**
 * A DBDatabase object tweaked to work with Microsoft SQL Server.
 *
 * <p>
 * Remember to include the MS SQL Server JDBC driver in your classpath.
 *
 * @author Malcolm Lett
 * @author Gregory Graham
 */
public class MSSQLServerDB extends DBDatabase implements SupportsPolygonDatatype {

	public static final long serialVersionUID = 1l;

	/**
	 * The Microsoft Driver used to connect to MS SQLServer databases.
	 *
	 * <p>
	 * By default MSSQLServerDB uses the
	 * ""com.microsoft.sqlserver.jdbc.SQLServerDriver" driver.</p>
	 */
	public final static String SQLSERVERDRIVERNAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

	/**
	 * The JTDS Driver to use to connect to MS SQLServer databases.
	 */
	public final static String JTDSDRIVERNAME = "net.sourceforge.jtds.jdbc.Driver";

	/**
	 * The default port used by MS SQLServer databases.
	 *
	 * <p>
	 * THe default port for MS SQLServer is 1433.</p>
	 */
	public final static int DEFAULT_PORT_NUMBER = 1433;

	/**
	 * The default host used by MS SQLServer databases.
	 *
	 * <p>
	 * These isn't a default host defined so this defaults to LOCALHOST.</p>
	 */
	public final static String DEFAULT_HOST_NAME = "localhost";

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param dcs	a DataSource to an MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	public MSSQLServerDB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new MSSQLServerSettingsBuilder().fromSettings(dcs));
	}

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param defn the definition to use with this database connection
	 * @param settings	a DataSource to an MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	protected MSSQLServerDB(MSSQLServerDBDefinition defn, DatabaseConnectionSettings settings) throws SQLException {
		super(new MSSQLServerSettingsBuilder().fromSettings(settings).setDefinition(defn));
	}

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param defn the definition to use with this database connection
	 * @param driverName the name of the JDBC driver to be used by this database
	 * @param settings	a DataSource to an MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	protected MSSQLServerDB(MSSQLServerDBDefinition defn, String driverName, DatabaseConnectionSettings settings) throws SQLException {
		super(new MSSQLServerSettingsBuilder().fromSettings(settings).setDefinition(defn).setDriverName(driverName));
	}

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param ds	a DataSource to an MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	public MSSQLServerDB(DataSource ds) throws SQLException {
		this(new MSSQLServerDBDefinition(), ds);
	}

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param builder	a configured SettingsBuilder for a MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	public MSSQLServerDB(MSSQLServerSettingsBuilder builder) throws SQLException {
		this((AbstractMSSQLServerSettingsBuilder<?, ?>) builder);
//		this(new MSSQLServerDBDefinition(), SQLSERVERDRIVERNAME, builder.toSettings());
	}

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param builder	a configured SettingsBuilder for a MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	protected MSSQLServerDB(AbstractMSSQLServerSettingsBuilder<?, ?> builder) throws SQLException {
		super(builder);
	}

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param defn
	 * @param ds	a DataSource to an MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	protected MSSQLServerDB(MSSQLServerDBDefinition defn, MSSQLServerSettingsBuilder ds) throws SQLException {
		this(defn, SQLSERVERDRIVERNAME, ds.toSettings());
	}

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param defn
	 * @param driverName
	 * @param ds	a DataSource to an MS SQLServer database
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	protected MSSQLServerDB(MSSQLServerDBDefinition defn, String driverName, MSSQLServerSettingsBuilder ds) throws SQLException {
		this(defn, driverName, ds.toSettings());
	}

	protected MSSQLServerDB(MSSQLServerDBDefinition defn, DataSource ds) throws SQLException {
		super(
				new MSSQLServerSettingsBuilder()
						.setDataSource(ds)
						.setDefinition(defn)
		);
//		super(defn, SQLSERVERDRIVERNAME, ds);
	}

	/**
	 * Creates a {@link DBDatabase } instance for MS SQL Server using the driver,
	 * JDBC URL, username, and password.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public MSSQLServerDB(String driverName, String jdbcURL, String username, String password) throws SQLException {
		this(new MSSQLServerDBDefinition(),
				driverName,
				new MSSQLServerSettingsBuilder().fromJDBCURL(jdbcURL, username, password)
		);
	}

	@Deprecated
	public MSSQLServerDB(MSSQLServerDBDefinition defn, String driverName, String jdbcURL, String username, String password) throws SQLException {
		this(new MSSQLServerSettingsBuilder().fromJDBCURL(jdbcURL, username, password));
	}

	/**
	 * Creates a {@link DBDatabase } instance for MS SQL Server using the JDBC
	 * URL, username, and password.
	 *
	 * <p>
	 * The default driver will be used for the connection.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public MSSQLServerDB(String jdbcURL, String username, String password) throws SQLException {
		this(new MSSQLServerSettingsBuilder().fromJDBCURL(jdbcURL, username, password));
	}

	@Deprecated
	protected MSSQLServerDB(MSSQLServerDBDefinition defn, String jdbcURL, String username, String password) throws SQLException {
		this(defn, SQLSERVERDRIVERNAME, new MSSQLServerSettingsBuilder().fromJDBCURL(jdbcURL, username, password));
	}

	/**
	 * Connect to an MS SQLServer database using the connection details specified
	 * and Microsoft's driver.
	 *
	 * @param hostname the name of the server where the database resides
	 * @param instanceName the name of the particular database instance to connect
	 * to (can be null)
	 * @param databaseName the name of the database within the instance (can be
	 * null)
	 * @param portNumber the port number that the database is available on
	 * @param username the account to connect via
	 * @param password the password to identify username.
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public MSSQLServerDB(String hostname, String instanceName, String databaseName, Integer portNumber, String username, String password) throws SQLException {
		this(new MSSQLServerSettingsBuilder()
				.setHost(hostname)
				.setPort(portNumber)
				.setInstance(instanceName)
				.setDatabaseName(databaseName)
				.setUsername(username)
				.setPassword(password)
		);
	}

	@Deprecated
	public MSSQLServerDB(MSSQLServerDBDefinition defn, String hostname, String instanceName, String databaseName, Integer portNumber, String username, String password) throws SQLException {
		this(defn,
				new MSSQLServerSettingsBuilder()
						.setHost(hostname)
						.setPort(portNumber)
						.setInstance(instanceName)
						.setDatabaseName(databaseName)
						.setUsername(username)
						.setPassword(password)
		);
	}

	/**
	 * Connect to an MS SQLServer database using the connection details specified
	 * and Microsoft's driver.
	 *
	 * @param driverName the JDBC driver class to use.
	 * @param hostname the name of the server where the database resides.
	 * @param instanceName the name of the particular database instance to connect
	 * to (can be null).
	 * @param databaseName the name of the database within the instance (can be
	 * null).
	 * @param portNumber the port number that the database is available on .
	 * @param username the account to connect via.
	 * @param password the password to identify username.
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public MSSQLServerDB(String driverName, String hostname, String instanceName, String databaseName, Integer portNumber, String username, String password) throws SQLException {
		this(new MSSQLServerDBDefinition(),
				driverName,
				new MSSQLServerSettingsBuilder()
						.setHost(hostname)
						.setPort(portNumber)
						.setInstance(instanceName)
						.setDatabaseName(databaseName)
						.setUsername(username)
						.setPassword(password)
		);
	}

	@Deprecated
	protected MSSQLServerDB(MSSQLServerDBDefinition defn, String driverName, String hostname, String instanceName, String databaseName, Integer portNumber, String username, String password) throws SQLException {
		this(defn,
				driverName,
				new MSSQLServerSettingsBuilder()
						.setHost(hostname)
						.setPort(portNumber)
						.setInstance(instanceName)
						.setDatabaseName(databaseName)
						.setDatabaseName(username)
						.setPassword(password)
		);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		for (MigrationFunctions fn : MigrationFunctions.values()) {
			fn.add(statement);
		}
		for (Point2DFunctions fn : Point2DFunctions.values()) {
			fn.add(statement);
		}
		for (Line2DFunctions fn : Line2DFunctions.values()) {
			fn.add(statement);
		}
		for (MultiPoint2DFunctions fn : MultiPoint2DFunctions.values()) {
			fn.add(statement);
		}
		for (Polygon2DFunctions fn : Polygon2DFunctions.values()) {
			fn.add(statement);
		}
	}

	//Invalid object name 'TableThatDoesntExistOnTheCluster'.
//	private final static Pattern NONEXISTANT_TABLE_PATTERN = Pattern.compile("Invalid object name '[^\"]*\'.");
	private final static Regex NONEXISTANT_TABLE_PATTERN = Regex.startingAnywhere().literal("Invalid object name '").noneOfTheseCharacters("'").optionalMany().literal("'.").toRegex();
	//There is already an object named 'TableThatDoesExistOnTheCluster' in the database.
//	private final static Pattern CREATING_EXISTING_TABLE_PATTERN = Pattern.compile("There is already an object named '[^\"]*\' in the database.");
	private final static Regex CREATING_EXISTING_TABLE_PATTERN = Regex.startingAnywhere().literal("There is already an object named '").noneOfTheseCharacters("'").optionalMany().literal("' in the database.").toRegex();
	//Cannot find the object "TableThatDoesntExistOnTheCluster" because it does not exist or you do not have permissions.
//	private final static Pattern UNABLE_TO_FIND_DATABASE_OBJECT_PATTERN = Pattern.compile("Cannot find the object \"[^\"]*\" because it does not exist or you do not have permissions.");
	private final static Regex UNABLE_TO_FIND_DATABASE_OBJECT_PATTERN = Regex.startingAnywhere().literal("Cannot find the object \"").noneOfTheseCharacters("\"").optionalMany().literal("\" because it does not exist or you do not have permissions.").toRegex();

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		final String message = exp.getMessage();
		if (message.matches("IDENTITY_INSERT is already ON for table '[^']*'. Cannot perform SET operation for table.*")) {
			String table = message.split("'")[1];
			DBStatement stmt = getConnection().createDBStatement();
			final String sql = "SET IDENTITY_INSERT " + table + " ON;";
			stmt.execute(new StatementDetails("Allow identity insertion", QueryIntention.ALLOW_IDENTITY_INSERT, sql));
			return ResponseToException.REQUERY;
		} else if (intent.is(QueryIntention.CREATE_TABLE) && CREATING_EXISTING_TABLE_PATTERN.matchesWithinString(message)) {
			return ResponseToException.SKIPQUERY;
		} else if (intent.is(QueryIntention.CHECK_TABLE_EXISTS) && NONEXISTANT_TABLE_PATTERN.matchesWithinString(message)) {
			return ResponseToException.SKIPQUERY;
		} else if (UNABLE_TO_FIND_DATABASE_OBJECT_PATTERN.matchesWithinString(message)) {
			return ResponseToException.SKIPQUERY;
		}
		return super.addFeatureToFixException(exp, intent);
	}

	@Override
	public Integer getDefaultPort() {
		return 1433;
	}

	@Override
	public AbstractMSSQLServerSettingsBuilder<?, ?> getURLInterpreter() {
		return new MSSQLServerSettingsBuilder();
	}
}
