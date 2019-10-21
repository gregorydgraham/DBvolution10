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
import java.util.regex.Pattern;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.SQLiteURLInterpreter;
import org.sqlite.SQLiteConfig;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.sqlite.*;
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.JDBCURLInterpreter;

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
//	private String derivedURL;
//	private static final JDBCURLInterpreter URL_PROCESSOR = new SQLiteURLInterpreter();

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
	 * @throws java.sql.SQLException database errors
	 */
	public SQLiteDB(DataSource ds) throws SQLException {
		super(new SQLiteDefinition(), SQLITE_DRIVER_NAME, ds);
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database on the DataSource
	 * provided.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException database errors
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
	 * @throws java.sql.SQLException database errors
	 */
	public SQLiteDB(String jdbcURL, String username, String password) throws SQLException {
		this(new SQLiteURLInterpreter().generateSettings(jdbcURL, username, password));
//		super(new SQLiteDefinition(), SQLITE_DRIVER_NAME, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database using the parameters
	 * provided.
	 *
	 * @param databaseFile the SQLite file to be used with this database
	 * @param username username
	 * @param password password
	 * @throws java.io.IOException file system errors
	 * @throws java.sql.SQLException database errors
	 */
	public SQLiteDB(File databaseFile, String username, String password) throws IOException, SQLException {
		super(new SQLiteDefinition(),
				SQLITE_DRIVER_NAME,
				new SQLiteURLInterpreter().generateSettings()
						.flowFilename(databaseFile.getCanonicalFile().toString())
						.flowDatabaseName(databaseFile.getCanonicalFile().toString())
						.flowUsername(username)
						.flowPassword(password)
		//				"jdbc:sqlite:" + databaseFile.getCanonicalFile(),
		//				username,
		//				password
		);
//		setDatabaseName(databaseFile.getCanonicalFile().toString());
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database using the parameters
	 * provided.
	 *
	 * @param filename the SQLite file to connect to.
	 * @param username username
	 * @param password password
	 * @param dummy just use TRUE
	 * @throws java.io.IOException file system errors
	 * @throws java.sql.SQLException database errors
	 */
	public SQLiteDB(String filename, String username, String password, boolean dummy) throws IOException, SQLException {
		super(new SQLiteDefinition(),
				SQLITE_DRIVER_NAME,
				new SQLiteURLInterpreter().generateSettings()
						.flowFilename(filename)
						.flowDatabaseName(new File(filename).getCanonicalFile().toString())
						.flowUsername(username)
						.flowPassword(password)
		//				"jdbc:sqlite:" + filename,
		//				username,
		//				password
		);
//		setDatabaseName(new File(filename).getCanonicalFile().toString());
	}

//	@Override
//	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : "jdbc:sqlite:"
//				+ settings.getDatabaseName();
//	}
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
	protected void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

//	@Override
//	protected Map<String, String> getExtras() {
//		String jdbcURL = getJdbcURL();
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split("?", 2)[1];
//			return DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", "");
//		} else {
//			return new HashMap<String, String>();
//		}
//	}
//	@Override
//	protected String getHost() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:sqlite://", "");
//		return noPrefix
//				.split("/", 2)[0]
//				.split(":")[0];
//
//	}
//	@Override
//	protected String getDatabaseInstance() {
//		String jdbcURL = getJdbcURL();
//		return getExtras().get("instance");
//	}
//	@Override
//	protected String getPort() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:sqlite://", "");
//		return noPrefix
//				.split("/", 2)[0]
//				.replaceAll("^[^:]*:+", "");
//	}
//	@Override
//	protected String getSchema() {
//		return "";
//	}
//	private void setDatabasenameFromURL() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:sqlite://", "");
//		String name = noPrefix.split(":", 3)[2];
//		setDatabaseName(name);
//	}
//	@Override
//	protected DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
//		DatabaseConnectionSettings set = new DatabaseConnectionSettings();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:sqlite://", "");
//		if (jdbcURL.contains(";")) {
//			String extrasString = jdbcURL.split("\\?", 2)[1];
//			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
//		}
//		set.setDatabaseName(noPrefix.split(":", 3)[2]);
//		set.setPort(noPrefix
//				.split("/", 2)[0]
//				.replaceAll("^[^:]*:+", ""));
//		set.setHost(noPrefix
//				.split("/", 2)[0]
//				.split(":")[0]);
//		set.setInstance(getExtras().get("instance"));
//		set.setSchema("");
//		return set;
//	}
	@Override
	public Integer getDefaultPort() {
		return 5432;
	}

	private final static Pattern TABLE_ALREADY_EXISTS = Pattern.compile("\\[SQLITE_ERROR\\] SQL error or missing database \\(table [^ ]* already exists\\)");

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		if (intent.is(QueryIntention.CREATE_TABLE) && TABLE_ALREADY_EXISTS.matcher(exp.getMessage()).matches()) {
			return ResponseToException.SKIPQUERY;
		}
		return super.addFeatureToFixException(exp, intent);
	}

//	@Override
//	protected Class<? extends DBDatabase> getBaseDBDatabaseClass() {
//		return SQLiteDB.class;
//	}
	@Override
	protected JDBCURLInterpreter getURLInterpreter() {
		return new SQLiteURLInterpreter();
	}

}
