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
import java.sql.Statement;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.Oracle11XEDBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.Oracle12DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;
import nz.co.gregs.dbvolution.internal.oracle.xe.*;

/**
 * Implements support for version 11 and prior of the Oracle database.
 *
 * @author Gregory Graham
 * @see OracleDB
 * @see Oracle12DB
 * @see OracleDBDefinition
 * @see Oracle11XEDBDefinition
 * @see Oracle12DBDefinition
 */
public class Oracle11XEDB extends OracleDB {

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11 and above.
	 *
	 * @param dataSource a datasource to an Oracle database
	 */
	public Oracle11XEDB(DataSource dataSource) {
		super(new Oracle11XEDBDefinition(), dataSource);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param definition definition
	 * @param jdbcURL jdbcURL
	 * @param driverName driverName
	 * @param password password
	 * @param username username
	 */
	public Oracle11XEDB(OracleDBDefinition definition, String driverName, String jdbcURL, String username, String password) {
		super(definition, driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public Oracle11XEDB(String driverName, String jdbcURL, String username, String password) {
		super(new Oracle11XEDBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public Oracle11XEDB(String jdbcURL, String username, String password) {
		super(new Oracle11XEDBDefinition(), "oracle.jdbc.driver.OracleDriver", jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param host host
	 * @param port port
	 * @param serviceName serviceName
	 * @param password password
	 * @param username username
	 */
	public Oracle11XEDB(String host, int port, String serviceName, String username, String password) {
		super(new Oracle11XEDBDefinition(), "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName, username, password);
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
	
	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		super.addDatabaseSpecificFeatures(statement);
		
		for (GeometryFunctions fn : GeometryFunctions.values()) {
			fn.add(statement);
		}
		for (MultiPoint2DFunctions fn : MultiPoint2DFunctions.values()) {
			fn.add(statement);
		}
		for (Line2DFunctions fn : Line2DFunctions.values()) {
			fn.add(statement);
		}
	}



}
