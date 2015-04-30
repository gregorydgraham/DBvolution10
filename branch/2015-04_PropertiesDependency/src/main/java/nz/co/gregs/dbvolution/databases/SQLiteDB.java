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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.sqlite.SQLiteConfig;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.internal.sqlite.*;

/**
 * Creates a DBDatabase for an SQLite database.
 *
 * @author Gregory Graham
 */
public class SQLiteDB extends DBDatabase implements SupportsDateRepeatDatatypeFunctions, SupportsPolygonDatatype {

	private static final String SQLITE_DRIVER_NAME = "org.sqlite.JDBC";

	/**
	 * Creates a DBDatabase tweaked for a SQLite database on the DataSource
	 * provided.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException
	 */
	public SQLiteDB(DataSource ds) throws SQLException {
		super(new SQLiteDefinition(), ds);
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database using the parameters
	 * provided.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException
	 */
	public SQLiteDB(String jdbcURL, String username, String password) throws SQLException {
		super(new SQLiteDefinition(), SQLITE_DRIVER_NAME, jdbcURL, username, password);
	}

	@Override
	protected boolean supportsFullOuterJoinNatively() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoin() {
		return false;
	}

	@Override
	protected Connection getConnectionFromDriverManager() throws SQLException {
		SQLiteConfig config = new SQLiteConfig();
		config.enableCaseSensitiveLike(true);
		Connection connection = DriverManager.getConnection(getJdbcURL(), getUsername(), getPassword());
		config.apply(connection);
		MissingStandardFunctions.addFunctions(this, connection);
		DateRepeatFunctions.addFunctions(connection);
		Point2DFunctions.addFunctions(connection);
		Line2DFunctions.addFunctions(connection);
		Polygon2DFunctions.addFunctions(connection);
		return connection;
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

}
