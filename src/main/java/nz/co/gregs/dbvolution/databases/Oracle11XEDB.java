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
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.Oracle11XEDBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.Oracle12DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;
import nz.co.gregs.dbvolution.internal.oracle.xe.*;

/**
 * Implements support for version 11 and prior of the Oracle database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see OracleDB
 * @see Oracle12DB
 * @see OracleDBDefinition
 * @see Oracle11XEDBDefinition
 * @see Oracle12DBDefinition
 */
public class Oracle11XEDB extends OracleDB {

	public static final long serialVersionUID = 1l;

	/**
	 *
	 * Provides a convenient constructor for DBDatabases that have configuration
	 * details hardwired or are able to automatically retrieve the details.
	 *
	 * <p>
	 * This constructor creates an empty DBDatabase with only the default
	 * settings, in particular with no driver, URL, username, password, or
	 * {@link DBDefinition}
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Instead you
	 * should define a no-parameter constructor that supplies the details for
	 * creating an instance using a more complete constructor.
	 *
	 * <p>
	 * DBDatabase encapsulates the knowledge of the database, in particular the
	 * syntax of the database in the DBDefinition and the connection details from
	 * a DataSource.
	 *
	 * @see DBDefinition
	 */
	public Oracle11XEDB() {
		super();
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11 and above.
	 *
	 * @param dataSource a datasource to an Oracle database
	 */
	public Oracle11XEDB(DataSource dataSource) throws SQLException {
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
	public Oracle11XEDB(OracleDBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
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
	public Oracle11XEDB(String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(new Oracle11XEDBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public Oracle11XEDB(String jdbcURL, String username, String password) throws SQLException {
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
	public Oracle11XEDB(String host, int port, String serviceName, String username, String password) throws SQLException {
		super(new Oracle11XEDBDefinition(), "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName, username, password);
	}

	@Override
	protected <TR extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, TR tableRow) throws SQLException {

		if (tableRow.getPrimaryKeys() != null) {
			DBDefinition definition = getDefinition();
//			final DBStatement dbStatement = getDBStatement();
			final String formattedTableName = definition.formatTableName(tableRow);
			final List<String> primaryKeyColumnNames = tableRow.getPrimaryKeyColumnNames();
			for (String primaryKeyColumnName : primaryKeyColumnNames) {
				final String formattedColumnName = definition.formatColumnName(primaryKeyColumnName);
//				try {
				dbStatement.execute("DROP SEQUENCE " + definition.getPrimaryKeySequenceName(formattedTableName, formattedColumnName));
//				} finally {
//					dbStatement.close();
//				}
			}
		}
		super.dropAnyAssociatedDatabaseObjects(dbStatement, tableRow);
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
