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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MariaDBSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.MariaDBDefinition;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.databases.settingsbuilders.SettingsBuilder;

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
	 * @throws java.sql.SQLException database errors
	 */
	public MariaDB(DataSource ds) throws SQLException {
		super(new MariaDBDefinition(), MARIADBDRIVERNAME, ds);
	}

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
	 */
	public MariaDB(MariaDBSettingsBuilder ds) throws SQLException {
		super(new MariaDBDefinition(), MARIADBDRIVERNAME, ds.toSettings());
	}

	/**
	 * Create a MariaDB instance of DBDatabase for the database with the supplied
	 * JDBC URL, using the username and password to login.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public MariaDB(String jdbcURL, String username, String password) throws SQLException {
		this(new MariaDBSettingsBuilder()
						.fromJDBCURL(jdbcURL)
						.setUsername(username)
						.setPassword(password)
		);
//		super(new MariaDBDefinition(), MARIADBDRIVERNAME, jdbcURL, username, password);
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
	 * @throws java.sql.SQLException database errors
	 */
	public MariaDB(String server, long port, String databaseName, String username, String password) throws SQLException {
		this(new MariaDBSettingsBuilder()
						.setHost(server)
						.setPort(port)
						.setDatabaseName(databaseName)
						.setUsername(username)
						.setPassword(password)
		);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

	@Override
	public Integer getDefaultPort() {
		return 3306;
	}

	@Override
	protected MariaDBSettingsBuilder getURLInterpreter() {
		return new MariaDBSettingsBuilder();
	}

}
