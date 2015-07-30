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

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.Oracle11DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;

/**
 * Implements support for version 11 and prior of the Oracle database.
 *
 * @author Gregory Graham
 * @see OracleDB
 * @see Oracle12DB
 * @see OracleDBDefinition
 * @see Oracle11DBDefinition
 * @see Oracle12DBDefinition
 */
public class Oracle11DB extends OracleDB {

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11 and above.
	 *
	 * @param dataSource a datasource to an Oracle database
	 * @throws java.sql.SQLException
	 */
	public Oracle11DB(DataSource dataSource) throws SQLException {
		super(new Oracle11DBDefinition(), dataSource);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param definition definition
	 * @param jdbcURL jdbcURL
	 * @param driverName driverName
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException
	 */
	public Oracle11DB(OracleDBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(definition, driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException
	 */
	public Oracle11DB(String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(new Oracle11DBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException
	 */
	public Oracle11DB(String jdbcURL, String username, String password) throws SQLException {
		super(new Oracle11DBDefinition(), "oracle.jdbc.driver.OracleDriver", jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param host host
	 * @param port port
	 * @param serviceName serviceName
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException
	 */
	public Oracle11DB(String host, int port, String serviceName, String username, String password) throws SQLException {
		super(new Oracle11DBDefinition(), "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName, username, password);
	}

	@Override
	protected <TR extends DBRow> void dropAnyAssociatedDatabaseObjects(TR tableRow) throws SQLException {

		if (tableRow.getPrimaryKey() != null) {
			DBDefinition definition = getDefinition();
			final DBStatement dbStatement = getDBStatement();
			final String formattedTableName = definition.formatTableName(tableRow);
			final String formattedColumnName = definition.formatColumnName(tableRow.getPrimaryKeyColumnName());
			try {
				dbStatement.execute("DROP SEQUENCE " + definition.getPrimaryKeySequenceName(formattedTableName, formattedColumnName));
			} finally {
				dbStatement.close();
			}
//			final DBStatement dbStatement2 = getDBStatement();
//			try {
//				dbStatement2.execute("DROP TRIGGER " + definition.getPrimaryKeyTriggerName(formattedTableName, formattedColumnName));
//			} finally {
//				dbStatement2.close();
//			}
		}
		super.dropAnyAssociatedDatabaseObjects(tableRow);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected Connection getConnectionFromDriverManager() throws SQLException {
		return super.getConnectionFromDriverManager(); //To change body of generated methods, choose Tools | Templates.
	}

}
