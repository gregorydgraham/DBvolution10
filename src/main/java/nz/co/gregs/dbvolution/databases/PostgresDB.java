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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TimeZone;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.settingsbuilders.PostgresSettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractPostgresSettingsBuilder;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.postgres.Line2DFunctions;
import nz.co.gregs.dbvolution.internal.postgres.MultiPoint2DFunctions;
import nz.co.gregs.dbvolution.internal.postgres.StringFunctions;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.regexi.Regex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A DBDatabase tweaked for PostgreSQL.
 *
 * @author Gregory Graham
 */
public class PostgresDB extends DBDatabaseImplementation implements SupportsPolygonDatatype {

	public static final long serialVersionUID = 1l;

	private static final Log LOG = LogFactory.getLog(PostgresDB.class);
	public static final String POSTGRES_DRIVER_NAME = "org.postgresql.Driver";

	/**
	 * The default port number used by PostgreSQL.
	 */
	public static final int POSTGRES_DEFAULT_PORT = 5432;

	/**
	 * The default username used by PostgreSQL.
	 */
	public static final String POSTGRES_DEFAULT_USERNAME = "postgres";
	private boolean postGISTopologyAlreadyTried = false;
	private boolean postGISAlreadyTried = false;
	private boolean postGISInstalled = false;

	/**
	 * Creates a PostgreSQL connection for the DataSource.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
	 */
	public PostgresDB(DataSource ds) throws SQLException {
		super(
				new PostgresSettingsBuilder().setDataSource(ds)
		);
	}

	/**
	 * Creates a PostgreSQL connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public PostgresDB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new PostgresSettingsBuilder().fromSettings(dcs));
	}

	/**
	 * Creates a PostgreSQL connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	protected PostgresDB(AbstractPostgresSettingsBuilder<?, ?> dcs) throws SQLException {
		super(dcs);
	}

	/**
	 * Creates a PostgreSQL connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public PostgresDB(PostgresSettingsBuilder dcs) throws SQLException {
		super(dcs);
	}

	/**
	 * Creates a PostgreSQL connection for the JDBC URL, username, and password.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public PostgresDB(String jdbcURL, String username, String password) throws SQLException {
		this(new PostgresSettingsBuilder().fromJDBCURL(jdbcURL, username, password));
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
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public PostgresDB(String hostname, int port, String databaseName, String username, String password) throws SQLException {
		this(new PostgresSettingsBuilder()
				.setHost(hostname)
				.setPort(port)
				.setDatabaseName(databaseName)
				.setUsername(username)
				.setPassword(password)
		);
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
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public PostgresDB(String hostname, int port, String databaseName, String username, String password, Map<String, String> urlExtras) throws SQLException {
		this(new PostgresSettingsBuilder()
				.setHost(hostname)
				.setPort(port)
				.setDatabaseName(databaseName)
				.setExtras(urlExtras)
				.setUsername(username)
				.setPassword(password)
		);
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
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public PostgresDB(String hostname, int port, String databaseName, String username, String password, String urlExtras) throws SQLException {
		super(
				new PostgresSettingsBuilder()
						.fromJDBCURL(
								"jdbc:postgresql://" + hostname + ":" + port + "/" + databaseName + (urlExtras == null || urlExtras.isEmpty() ? "" : "?" + urlExtras),
								username,
								password
						)
		);
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
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public PostgresDB(String databaseName, String username, String password, String urlExtras) throws SQLException {
		this("localhost", POSTGRES_DEFAULT_PORT, databaseName, username, password, urlExtras);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Assumes that the database and application are on the the same machine.
	 *
	 * @param table the table to be loaded
	 * @param file the file to load data from
	 * @param delimiter the separator between the values of each row
	 * @param nullValue the string that represents NULL in this file.
	 * @param escapeCharacter the character that escapes special values
	 * @param quoteCharacter the character the surrounds strings.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing 1 Database
	 * exceptions may be thrown
	 * @throws SQLException database exceptions may be thrown.
	 */
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "Escaping over values takes place within this method to protect data integrity")
	public int loadFromCSVFile(DBRow table, File file, String delimiter, String nullValue, String escapeCharacter, String quoteCharacter) throws SQLException {
		int returnValue;
		try (DBStatement dbStatement = this.getDBStatement()) {
			returnValue = dbStatement.executeUpdate("COPY " + table.getTableName().replaceAll("\\\"", "") + " FROM '" + file.getAbsolutePath().replaceAll("\\\"", "") + "' WITH (DELIMITER '" + delimiter.replaceAll("\\\"", "") + "', NULL '" + nullValue.replaceAll("\\\"", "") + "', ESCAPE '" + escapeCharacter.replaceAll("\\\"", "") + "', FORMAT csv, QUOTE '" + quoteCharacter.replaceAll("\\\"", "") + "');");
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
	 * @throws SQLException database exceptions may be thrown
	 */
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "Escaping over values takes place within this method to protect data integrity")
	public void createDatabase(String databaseName) throws SQLException {
		String sqlString = "CREATE DATABASE " + databaseName.replaceAll("\\\"", "") + ";";
		try (DBStatement dbStatement = getDBStatement()) {
			dbStatement.execute("Create database", QueryIntention.CREATE_DATABASE, sqlString);
		}
	}

	/**
	 * Create a new database/schema on this database server.
	 *
	 * <p>
	 * Generally requires all sorts of privileges and is best performed by
	 * database administrator (DBA).
	 *
	 * @param username The user to be created
	 * @param password the password the user will use.
	 * @throws SQLException database exceptions may be throwns.
	 */
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "Escaping over values takes place within this method to protect data integrity")
	public void createUser(String username, String password) throws SQLException {
		String sqlString = "CREATE USER \"" + username.replaceAll("\\\"", "") + "\" WITH PASSWORD '" + password.replaceAll("'", "") + "';";
		try (DBStatement dbStatement = getDBStatement()) {
			dbStatement.execute("Create user", QueryIntention.CREATE_USER, sqlString);
		}
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		try {
			this.dropTable(tableRow);
		} catch (org.postgresql.util.PSQLException exp) {
		} catch (SQLException exp) {
		}
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement stmnt) throws ExceptionDuringDatabaseFeatureSetup {
		setTimeZone(stmnt);
		createPostGISExtension(stmnt);
		if (postGISInstalled) {
			createPostGISTopologyExtension(stmnt);
		}
		for (StringFunctions fn : StringFunctions.values()) {
			fn.add(stmnt);
		}
		if (postGISInstalled) {
			for (Line2DFunctions fn : Line2DFunctions.values()) {
				fn.add(stmnt);
			}
			for (MultiPoint2DFunctions fn : MultiPoint2DFunctions.values()) {
				fn.add(stmnt);
			}
		}
	}

	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "Escaping over values takes place within this method to protect data integrity")
	private void setTimeZone(Statement stmnt) throws ExceptionDuringDatabaseFeatureSetup {
		String tzName = TimeZone.getDefault().getID();
		final String setTheTimezone = "set time zone '" + tzName.replaceAll("\\\"", "") + "';";
		try {
			stmnt.execute(setTheTimezone);
		} catch (Exception ex) {
			throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: set timezone", ex);
		}
	}

	private void createPostGISTopologyExtension(Statement stmnt) throws ExceptionDuringDatabaseFeatureSetup {
		try {
			if (!postGISTopologyAlreadyTried) {
				postGISTopologyAlreadyTried = true;
				boolean execute = stmnt.execute("select * from pg_extension where extname = 'postgis_topology';");
				final ResultSet resultSet = stmnt.getResultSet();
				boolean postGISAlreadyCreated = resultSet.next();
				if (!postGISAlreadyCreated) {
					stmnt.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology;");
				}
			}
		} catch (org.postgresql.util.PSQLException pexc) {
			LOG.warn("POSTGIS TOPOLOGY Rejected: Spatial operations will NOT function.", pexc);
		} catch (Exception ex) {
			throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: PostGIS Topology", ex);
		}
	}

	private void createPostGISExtension(Statement stmnt) throws ExceptionDuringDatabaseFeatureSetup {
		try {
			if (!postGISAlreadyTried) {
				postGISAlreadyTried = true;
				boolean execute = stmnt.execute("select * from pg_extension where extname = 'postgis';");
				final ResultSet resultSet = stmnt.getResultSet();
				boolean postGISAlreadyCreated = resultSet.next();
				if (!postGISAlreadyCreated) {
					stmnt.execute("CREATE EXTENSION IF NOT EXISTS postgis;");
				}
				postGISInstalled = true;
			}
		} catch (org.postgresql.util.PSQLException pexc) {
			LOG.warn("POSTGIS Rejected: Spatial operations will NOT function.", pexc);
			postGISInstalled = false;
		} catch (Exception ex) {
			throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: PostGIS", ex);
		}
	}

	/**
	 * Used to add features in a just-in-time manner.
	 *
	 * <p>
	 * During a statement the database may throw an exception because a feature
	 * has not yet been added. Use this method to parse the exception and install
	 * the required feature.
	 *
	 * <p>
	 * The statement will be automatically run after this method exits.
	 *
	 * @param exp the exception throw by the database that may need fixing
	 * @return the suggested response to this exception
	 * @throws SQLException accessing the database may cause exceptions
	 */
	private static final Regex TABLE_EXISTS = Regex.startingAnywhere()
			.literal("ERROR: relation ")
			.doublequote().anyCharacterExcept('"').doublequote()
			.literal(" already exists").toRegex();
	private static final Regex TABLE_DOES_NOT_EXIST = Regex.startingAnywhere()
			.literal("ERROR: relation ")
			.doublequote().anyCharacterExcept('"').doublequote()
			.literal(" does not exist").toRegex();

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent, StatementDetails details) throws Exception {
		if ((exp instanceof org.postgresql.util.PSQLException)) {
			String message = exp.getMessage();
			if (intent.is(QueryIntention.CREATE_TABLE) && TABLE_EXISTS.matchesWithinString(message)) { //message.matches("ERROR: relation \"[^\"]*\" already exists.*")) {
				return ResponseToException.SKIPQUERY;
			} else if (intent.is(QueryIntention.CHECK_TABLE_EXISTS)) {
				if (TABLE_DOES_NOT_EXIST.matchesWithinString(message)) {
					//message.matches("ERROR: relation \"[^\"]*\" does not exist.*")) {
					return ResponseToException.SKIPQUERY;
				} else {
					throw exp;
				}
			} else {
				throw exp;
			}
		} else {
			throw exp;
		}
	}

	@Override
	public Integer getDefaultPort() {
		return 5432;
	}

	@Override
	public AbstractPostgresSettingsBuilder<?, ?> getURLInterpreter() {
		return new PostgresSettingsBuilder();
	}

	@Override
	public boolean supportsGeometryTypesFullyInSchema() {
		return true;
	}

	@Override
	public PostgresSettingsBuilder getSettingsBuilder() {
		return new PostgresSettingsBuilder().fromSettings(this.getSettings());
	}
}
