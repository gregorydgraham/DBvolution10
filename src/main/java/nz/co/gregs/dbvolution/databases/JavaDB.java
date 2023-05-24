/*
 * Copyright 2014 gregorygraham.
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
import nz.co.gregs.dbvolution.databases.settingsbuilders.JavaDBSettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractJavaDBSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;

/**
 * A version of DBDatabase tweaked for JavaDB.
 *
 * <p>
 * Uses the Apache Derby ClientDriver internally to allow access to remote
 * JavaDBs.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class JavaDB extends DBDatabaseImplementation {

	public static final String DRIVER_NAME = "org.apache.derby.jdbc.ClientDriver";
	public static final long serialVersionUID = 1l;

	/**
	 * Default Constructor.
	 *
	 */
//	public JavaDB() {
//	}

	/**
	 * Creates a new JavaDB instance that will connect to the DataSource.
	 *
	 * @param dataSource	dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDB(DataSource dataSource) throws SQLException {
		super(
				new JavaDBSettingsBuilder()
						.setDataSource(dataSource)
		);
//		super(new JavaDBDefinition(), DRIVER_NAME, dataSource);
	}

	/**
	 * Creates a new JavaDB instance that will connect to the DataSource.
	 *
	 * @param dataSource	dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDB(DatabaseConnectionSettings dataSource) throws SQLException {
		this(new JavaDBSettingsBuilder().fromSettings(dataSource));
	}

	/**
	 * Creates a new JavaDB instance that will connect to the DataSource.
	 *
	 * @param dataSource	dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDB(JavaDBSettingsBuilder dataSource) throws SQLException {
		super(dataSource);
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
	public JavaDB(String jdbcURL, String username, String password) throws SQLException {
		this(new JavaDBSettingsBuilder().fromJDBCURL(jdbcURL, username, password)
		);
	}

	/**
	 * Creates a new JavaDB instance that will connect to the database on the
	 * named host on the specified port with the supplied username and password.
	 *
	 * @param host host
	 * @param port port
	 * @param database database
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDB(String host, int port, String database, String username, String password) throws SQLException {
		this(new JavaDBSettingsBuilder().setHost(host).setPort(port).setUsername(username).setPassword(password)
		);
	}

	@Override
	public JavaDB clone() throws CloneNotSupportedException {
		return (JavaDB) super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

	@Override
	public Integer getDefaultPort() {
		return 1527;
	}

	@Override
	public AbstractJavaDBSettingsBuilder<?,?> getURLInterpreter() {
		return new JavaDBSettingsBuilder();
	}
}
