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

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.Oracle12DBDefinition;

/**
 * Implements support for version 12 of the Oracle database.
 *
 * @author gregory.graham
 * @see OracleDB
 * @see Oracle11DB
 * @see Oracle12DBDefinition
 */
public class Oracle12DB extends OracleDB {

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param definition
	 * @param driverName
	 * @param jdbcURL
	 * @param username
	 * @param password
	 */
	public Oracle12DB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) {
		super(definition, driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param driverName
	 * @param jdbcURL
	 * @param username
	 * @param password
	 */
	public Oracle12DB(String driverName, String jdbcURL, String username, String password) {
		super(new Oracle12DBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param jdbcURL
	 * @param username
	 * @param password
	 */
	public Oracle12DB(String jdbcURL, String username, String password) {
		super(new Oracle12DBDefinition(), "oracle.jdbc.driver.OracleDriver", jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param host
	 * @param port
	 * @param serviceName
	 * @param username
	 * @param password
	 */
	public Oracle12DB(String host, int port, String serviceName, String username, String password) {
		super(new Oracle12DBDefinition(), "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName, username, password);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

}
