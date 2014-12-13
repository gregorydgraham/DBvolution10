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

import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.JavaDBDefinition;
import org.apache.derby.agg.Aggregator;

/**
 * A version of DBDatabase tweaked for JavaDB.
 *
 * <p>
 * Uses the Apache Derby ClientDriver internally to allow access to remote
 * JavaDBs.
 *
 * @author Gregory Graham
 */
public class JavaDB extends DBDatabase {

	private static final String driverName = "org.apache.derby.jdbc.ClientDriver";

	/**
	 * Default Constructor.
	 *
	 */
	public JavaDB() {
	}

	/**
	 * Creates a new JavaDB instance that will connect to the DataSource.
	 *
	 * @param dataSource
	 */
	public JavaDB(DataSource dataSource) {
		super(new JavaDBDefinition(), dataSource);
	}

	/**
	 * Creates a new JavaDB instance that will connect to the JDBC URL using the
	 * username and password supplied..
	 *
	 * @param jdbcURL
	 * @param username
	 * @param password
	 */
	public JavaDB(String jdbcURL, String username, String password) {
		super(new JavaDBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a new JavaDB instance that will connect to the database on the
	 * named host on the specified port with the supplied username and password.
	 *
	 * @param host
	 * @param port
	 * @param database
	 * @param username
	 * @param password
	 */
	public JavaDB(String host, int port, String database, String username, String password) {
		super(new JavaDBDefinition(), driverName, "jdbc:derby://" + host + ":" + port + "/" + database + ";create=true", username, password);
	}

	@Override
	protected boolean supportsFullOuterJoinNatively() {
		return false;
	}

	@Override
	public JavaDB clone() throws CloneNotSupportedException {
		return (JavaDB) super.clone(); //To change body of generated methods, choose Tools | Templates.
	}
}
