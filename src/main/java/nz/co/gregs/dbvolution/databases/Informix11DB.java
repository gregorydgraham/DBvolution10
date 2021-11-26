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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.Informix11SettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.Informix11DBDefinition;

/**
 * A version of DBDatabase tweaked for Informix 11 and higher.
 *
 * @author Gregory Graham
 */
public class Informix11DB extends InformixDB {

	public static final String INFORMIXDRIVERNAME = "com.informix.jdbc.IfxDriver";
	private static final long serialVersionUID = 1l;

	/**
	 * Creates a DBDatabase configured for Informix with the given JDBC URL,
	 * username, and password.
	 *
	 * <p>
	 * Remember to include the Informix JDBC driver in your classpath.
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.lang.ClassNotFoundException java.lang.ClassNotFoundException
	 * @throws java.sql.SQLException database errors
	 */
	public Informix11DB(String jdbcURL, String username, String password) throws ClassNotFoundException, SQLException {
		super(new Informix11DBDefinition(), INFORMIXDRIVERNAME,
				new Informix11SettingsBuilder()
						.fromJDBCURL(jdbcURL)
						.setUsername(username)
						.setPassword(password)
						.toSettings()
		);
	}

	/**
	 * Creates a DBDatabase configured for Informix with the given JDBC URL,
	 * username, and password.
	 *
	 * <p>
	 * Remember to include the Informix JDBC driver in your classpath.
	 * 
	 * 1 Database exceptions may be thrown
	 *
	 * @param dcs the database connection settings
	 * @throws java.sql.SQLException database errors
	 */
	public Informix11DB(DatabaseConnectionSettings dcs) throws SQLException {
		super(new Informix11DBDefinition(), INFORMIXDRIVERNAME, dcs);
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
	public Informix11DB(DataSource dataSource) throws SQLException {
		super(new Informix11DBDefinition(), dataSource);
	}

	/**
	 * Creates a new DBDatabase for Informix11.
	 *
	 * @param builder the settings required to connect to the Informix server.
	 * @throws SQLException database errors
	 */
	public Informix11DB(Informix11SettingsBuilder builder) throws SQLException {
		super(builder);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public Informix11SettingsBuilder getURLInterpreter() {
		return new Informix11SettingsBuilder();
	}

}
