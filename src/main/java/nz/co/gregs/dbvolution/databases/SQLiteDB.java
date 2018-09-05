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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.sqlite.SQLiteConfig;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition;
import nz.co.gregs.dbvolution.internal.sqlite.*;

/**
 * Creates a DBDatabase for an SQLite database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class SQLiteDB extends DBDatabase {

	private static final String SQLITE_DRIVER_NAME = "org.sqlite.JDBC";
	public static final long serialVersionUID = 1l;
	private String derivedURL;

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
	protected SQLiteDB() {
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database on the DataSource
	 * provided.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException
	 */
	public SQLiteDB(DataSource ds) throws SQLException {
		super(new SQLiteDefinition(), SQLITE_DRIVER_NAME, ds);
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database on the DataSource
	 * provided.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException
	 */
	public SQLiteDB(DatabaseConnectionSettings ds) throws SQLException {
		super(new SQLiteDefinition(), SQLITE_DRIVER_NAME, ds);
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

	/**
	 * Creates a DBDatabase tweaked for a SQLite database using the parameters
	 * provided.
	 *
	 * @param databaseFile
	 * @param username username
	 * @param password password
	 * @throws java.io.IOException
	 * @throws java.sql.SQLException
	 */
	public SQLiteDB(File databaseFile, String username, String password) throws IOException, SQLException {
		super(new SQLiteDefinition(),
				SQLITE_DRIVER_NAME,
				"jdbc:sqlite:" + databaseFile.getCanonicalFile(),
				username,
				password);
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database using the parameters
	 * provided.
	 *
	 * @param filename
	 * @param username username
	 * @param password password
	 * @param dummy just use TRUE
	 * @throws java.io.IOException
	 * @throws java.sql.SQLException
	 */
	public SQLiteDB(String filename, String username, String password, boolean dummy) throws IOException, SQLException {
		super(new SQLiteDefinition(),
				SQLITE_DRIVER_NAME,
				"jdbc:sqlite:" + filename,
				username,
				password);
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : "jdbc:sqlite:"
				+ settings.getDatabaseName();
	}

	@Override
	protected Connection getConnectionFromDriverManager() throws SQLException {
		SQLiteConfig config = new SQLiteConfig();
		config.enableCaseSensitiveLike(true);
		Connection connection = DriverManager.getConnection(getJdbcURL(), getUsername(), getPassword());
		config.apply(connection);
		addMissingFunctions(connection);
		return connection;
	}

	private void addMissingFunctions(Connection connection) throws SQLException {
		MissingStandardFunctions.addFunctions(this, connection);
		DateRepeatFunctions.addFunctions(connection);
		Point2DFunctions.addFunctions(connection);
		MultiPoint2DFunctions.addFunctions(connection);
		LineSegment2DFunctions.addFunctions(connection);
		Line2DFunctions.addFunctions(connection);
		Polygon2DFunctions.addFunctions(connection);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		;
	}

}
