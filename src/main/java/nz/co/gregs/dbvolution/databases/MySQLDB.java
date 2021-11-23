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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.regexi.Regex;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MySQLSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition_5_7;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractMySQLSettingsBuilder;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.mysql.MigrationFunctions;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;

/**
 * A DBDatabase tweaked for MySQL databases
 *
 * @author Gregory Graham
 */
public class MySQLDB extends DBDatabase implements SupportsPolygonDatatype {

	public final static String MYSQLDRIVERNAME = "com.mysql.jdbc.Driver";
	private static final long serialVersionUID = 1l;
	public static final int DEFAULT_PORT = 3306;

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
	 */
	public MySQLDB(DataSource ds) throws SQLException {
		super(
				new MySQLSettingsBuilder().setDataSource(ds)
		);
	}

	/**
	 * Creates a MySQL connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public MySQLDB(DatabaseConnectionSettings dcs) throws SQLException {
		super(new MySQLSettingsBuilder().fromSettings(dcs));
	}

	/**
	 * Creates a MySQL connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public MySQLDB(MySQLSettingsBuilder dcs) throws SQLException {
		super(dcs);
	}

	/**
	 * Creates a MySQL connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	protected MySQLDB(AbstractMySQLSettingsBuilder<?, ?> dcs) throws SQLException {
		super(dcs);
	}

	/**
	 * Creates DBDatabase suitable for use with MySQL attached to the supplied
	 * JDBC URL, username, and password.
	 *
	 * @param jdbcURL jdbcURL
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException database errors
	 */
	public MySQLDB(String jdbcURL, String username, String password) throws SQLException {
		this(new MySQLSettingsBuilder()
				.fromJDBCURL(jdbcURL)
				.setUsername(username)
				.setPassword(password)
		);
	}

	/**
	 * Creates DBDatabase suitable for use with MySQL attached to the supplied
	 * JDBC URL, username, and password.
	 *
	 * @param server the server to connect to.
	 * @param port the port to connect on.
	 * @param databaseName the database that is required on the server.
	 * @param username the user to login as.
	 * @param password the password required to login successfully.
	 * @throws java.sql.SQLException database errors
	 */
	public MySQLDB(String server, int port, String databaseName, String username, String password) throws SQLException {
		this(new MySQLSettingsBuilder()
				.setHost(server)
				.setPort(port)
				.setDatabaseName(databaseName)
				.setUsername(username)
				.setPassword(password)
		);
	}

	@Override
	public AbstractMySQLSettingsBuilder<?, ?> getURLInterpreter() {
		return new MySQLSettingsBuilder();
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
	}

	@Override
	public Integer getDefaultPort() {
		return 3306;
	}

	private final static Regex FUNCTION_DOES_NOT_EXISTS = Regex.startingAnywhere().literal("FUNCTION ").noneOfThisCharacter(' ').atLeastOnce().literal(" does not exist").toRegex();
	private final static Regex TABLE_ALREADY_EXISTS = Regex.startingAnywhere().literal("Table ").charactersWrappedBy('\'').literal(" already exists").toRegex();

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent, StatementDetails details) throws Exception {
		if (TABLE_ALREADY_EXISTS.matchesEntireString(exp.getMessage())) {
			return ResponseToException.SKIPQUERY;
		} else if (intent.is(QueryIntention.DROP_FUNCTION) && FUNCTION_DOES_NOT_EXISTS.matchesEntireString(exp.getMessage())) {
			return ResponseToException.SKIPQUERY;
		}
		return super.addFeatureToFixException(exp, intent, details);
	}

	@Override
	public void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		try {
			if (metaData.getDatabaseMajorVersion() < 4
					|| (metaData.getDatabaseMajorVersion() == 5 && metaData.getDatabaseMinorVersion() < 8)) {
				setDefinition(new MySQLDBDefinition_5_7());
			} else {
				setDefinition(new MySQLDBDefinition());
			}
		} catch (SQLException ex) {
			final Logger logger = Logger.getLogger(MySQLDB.class.getName());
			logger.log(Level.INFO, "Failed to get connection metadata information to set the database definition");
			logger.log(Level.INFO, null, ex);
		}
	}
}
