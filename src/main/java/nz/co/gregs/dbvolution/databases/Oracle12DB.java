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
import nz.co.gregs.dbvolution.databases.definitions.Oracle12DBDefinition;

/**
 * Implements support for version 12 of the Oracle database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see OracleAWSDB
 * @see Oracle11XEDB
 * @see Oracle12DBDefinition
 */
public class Oracle12DB extends OracleDB {

	private static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
	public static final long serialVersionUID = 1l;

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 *
	 *
	 *
	 *
	 *
	 */
	protected Oracle12DB() {
		super();
	}
//	public Oracle12DB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) {
//		super(definition, driverName, jdbcURL, username, password);
//	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param dataSource a datasource to an Oracle database
	 */
	public Oracle12DB(DataSource dataSource) throws SQLException {
		super(new Oracle12DBDefinition(), dataSource);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param password password
	 * @param username username
	 */
	public Oracle12DB(String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(new Oracle12DBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public Oracle12DB(String jdbcURL, String username, String password) throws SQLException {
		super(new Oracle12DBDefinition(), ORACLE_JDBC_DRIVER, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 12 and above.
	 *
	 * @param host host
	 * @param port port
	 * @param serviceName serviceName
	 * @param username username
	 * @param password password
	 */
	public Oracle12DB(String host, int port, String serviceName, String username, String password) throws SQLException {
		super(new Oracle12DBDefinition(), ORACLE_JDBC_DRIVER, "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName, username, password);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

}
