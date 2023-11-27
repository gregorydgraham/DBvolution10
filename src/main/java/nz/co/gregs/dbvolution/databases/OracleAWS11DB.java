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

import nz.co.gregs.dbvolution.databases.settingsbuilders.OracleAWS11SettingsBuilder;
import java.sql.SQLException;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.OracleAWS11DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.Oracle12DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleAWSDBDefinition;

/**
 * Implements support for version 11 and prior of the Oracle database as provide
 * by Amazon's AWS relational database service (RDS).
 *
 * @author Gregory Graham
 * @see OracleAWSDB
 * @see Oracle12DB
 * @see OracleAWSDBDefinition
 * @see OracleAWS11DBDefinition
 * @see Oracle12DBDefinition
 */
public class OracleAWS11DB extends OracleAWSDB {

	public static final long serialVersionUID = 1l;

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11 and above.
	 *
	 * @param dataSource a datasource to an Oracle database
	 * @throws java.sql.SQLException database errors
	 */
	public OracleAWS11DB(DataSource dataSource) throws SQLException {
		super(new OracleAWS11DBDefinition(), dataSource);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	stored settings for connecting to the database server
	 * @throws java.sql.SQLException database errors
	 */
	public OracleAWS11DB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new OracleAWS11SettingsBuilder().fromSettings(dcs));
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param settings settings required to connect to the database server
	 * @throws java.sql.SQLException database errors
	 */
	public OracleAWS11DB(OracleAWS11SettingsBuilder settings) throws SQLException {
		super(settings);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	stored settings for connecting to the database server
	 * @param defn the oracle database definition
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleAWS11DB(OracleAWS11DBDefinition defn, DatabaseConnectionSettings dcs) throws SQLException {
		super(defn, dcs);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param definition definition
	 * @param jdbcURL jdbcURL
	 * @param driverName driverName
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleAWS11DB(OracleAWSDBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(definition, driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleAWS11DB(String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(new OracleAWS11DBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public OracleAWS11DB(String jdbcURL, String username, String password) throws SQLException {
		this(new OracleAWS11SettingsBuilder().fromJDBCURL(jdbcURL, username, password));
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param host host
	 * @param port port
	 * @param serviceName serviceName
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleAWS11DB(String host, int port, String serviceName, String username, String password) throws SQLException {
		super(new OracleAWS11DBDefinition(), "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName, username, password);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public OracleAWS11SettingsBuilder getURLInterpreter() {
		return new OracleAWS11SettingsBuilder();
	}

	@Override
	public OracleAWS11SettingsBuilder getSettingsBuilder() {
		return new OracleAWS11SettingsBuilder().fromSettings(this.getSettings());
	}

}
