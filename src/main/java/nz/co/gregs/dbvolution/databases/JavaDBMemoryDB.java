/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.databases.settingsbuilders.JavaDBMemorySettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;

/**
 * Use this class to work with an in-memory JavaDB.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class JavaDBMemoryDB extends DBDatabaseImplementation {

	public static final long serialVersionUID = 1l;
	public static final String DRIVER_NAME = "org.apache.derby.jdbc.ClientDriver";

	/**
	 * Default Constructor.
	 *
	 */
//	public JavaDBMemoryDB() {
//	}

	/**
	 * Creates a new JavaDB instance that will connect to the DataSource.
	 *
	 * @param dataSource	dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDBMemoryDB(DataSource dataSource) throws SQLException {
		super(
				new JavaDBMemorySettingsBuilder()
						.setDataSource(dataSource)
		);
//		super(new JavaDBMemoryDBDefinition(), DRIVER_NAME, dataSource);
	}

	/**
	 * Creates a new JavaDB instance that will connect to the DataSource.
	 *
	 * @param settings	dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDBMemoryDB(JavaDBMemorySettingsBuilder settings) throws SQLException {
		super(settings);
	}

	/**
	 * Creates a new JavaDB instance that will connect to the JDBC URL using the
	 * username and password supplied..
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDBMemoryDB(String jdbcURL, String username, String password) throws SQLException {
		this(new JavaDBMemorySettingsBuilder().fromJDBCURL(jdbcURL, username, password)
		);
	}

	/**
	 * Creates or connects to a JavaDB in-memory instance.
	 *
	 * @param host host
	 * @param username username
	 * @param port port
	 * @param database database
	 * @param password password
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public JavaDBMemoryDB(String host, int port, String database, String username, String password) throws SQLException, UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver {
		this(new JavaDBMemorySettingsBuilder()
				.setHost(host)
				.setPort(port)
				.setDatabaseName(database)
				.setUsername(username)
				.setPassword(password)
		);
	}

	@Override
	public JavaDBMemoryDB clone() throws CloneNotSupportedException {
		return (JavaDBMemoryDB) super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

	@Override
	public boolean isMemoryDatabase() {
		return true;
	}

	@Override
	public Integer getDefaultPort() {
		return 1527;
	}

	@Override
	public JavaDBMemorySettingsBuilder getURLInterpreter() {
		return new JavaDBMemorySettingsBuilder();
	}

	@Override
	public JavaDBMemorySettingsBuilder getSettingsBuilder() {
		var builder = new JavaDBMemorySettingsBuilder();
		builder.fromSettings(this.getSettings());
		return builder;
	}
}
