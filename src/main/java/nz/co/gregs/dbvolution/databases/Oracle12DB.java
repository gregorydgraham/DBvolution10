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

import nz.co.gregs.dbvolution.databases.settingsbuilders.Oracle12SettingsBuilder;
import java.sql.SQLException;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.Oracle12DBDefinition;

/**
 * Implements support for version 12 of the Oracle database.
 *
 * @author Gregory Graham
 * @see OracleAWSDB
 * @see Oracle11XEDB
 * @see Oracle12DBDefinition
 */
public class Oracle12DB extends OracleDB {

	public static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
	public static final long serialVersionUID = 1l;

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param dataSource a datasource to an Oracle database
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle12DB(DataSource dataSource) throws SQLException {
		super(
				new Oracle12SettingsBuilder().setDataSource(dataSource)
		);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param settings settings required to connect to the database server
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle12DB(Oracle12SettingsBuilder settings) throws SQLException {
		super(settings);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs settings required to connect to the database server
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle12DB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new Oracle12SettingsBuilder().fromSettings(dcs));
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	settings required to connect to the database server
	 * @param defn the oracle database definition
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public Oracle12DB(Oracle12DBDefinition defn, DatabaseConnectionSettings dcs) throws SQLException {
		this(new Oracle12SettingsBuilder().fromSettings(dcs).setDefinition(defn));
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public Oracle12DB(String driverName, String jdbcURL, String username, String password) throws SQLException {
		this(new Oracle12SettingsBuilder().fromJDBCURL(jdbcURL, username, password).setDriverName(driverName));
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle12DB(String jdbcURL, String username, String password) throws SQLException {
		this(new Oracle12SettingsBuilder().fromJDBCURL(jdbcURL, username, password));
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param host host
	 * @param port port
	 * @param serviceName serviceName
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public Oracle12DB(String host, int port, String serviceName, String username, String password) throws SQLException {
		super(new Oracle12DBDefinition(), ORACLE_JDBC_DRIVER, "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName, username, password);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public Oracle12SettingsBuilder getURLInterpreter() {
		return new Oracle12SettingsBuilder();
	}

}
