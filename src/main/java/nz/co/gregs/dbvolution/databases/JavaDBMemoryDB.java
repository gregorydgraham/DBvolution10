/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.JavaDBMemoryURLInterpreter;
import nz.co.gregs.dbvolution.databases.definitions.JavaDBMemoryDBDefinition;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.JDBCURLInterpreter;

/**
 * Use this class to work with an in-memory JavaDB.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class JavaDBMemoryDB extends DBDatabase {

	public static final long serialVersionUID = 1l;
	private static final String DRIVER_NAME = "org.apache.derby.jdbc.ClientDriver";
	private String derivedURL;

	/**
	 * Default Constructor.
	 *
	 */
	public JavaDBMemoryDB() {
	}

	/**
	 * Creates a new JavaDB instance that will connect to the DataSource.
	 *
	 * @param dataSource	dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public JavaDBMemoryDB(DataSource dataSource) throws SQLException {
		super(new JavaDBMemoryDBDefinition(), DRIVER_NAME, dataSource);
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
	public JavaDBMemoryDB(String jdbcURL, String username, String password) throws SQLException {
		super(
				new JavaDBMemoryDBDefinition(), 
				DRIVER_NAME, 
				new JavaDBMemoryURLInterpreter().generateSettings(jdbcURL, username, password)
//				jdbcURL, username, password
				);
	}

	/**
	 * Creates or connects to a JavaDB in-memory instance.
	 *
	 * @param host host
	 * @param username username
	 * @param port port
	 * @param database database
	 * @param password password
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public JavaDBMemoryDB(String host, int port, String database, String username, String password) throws SQLException, UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver {
		super(new JavaDBMemoryDBDefinition(), DRIVER_NAME, "jdbc:derby://" + host + ":" + port + "/memory:" + database + ";create=true", username, password);
	}

//	@Override
//	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : "jdbc:derby://"
//				+ settings.getHost() + ":"
//				+ settings.getPort() + "/memory:"
//				+ settings.getDatabaseName() + ";create=true";
//	}

	@Override
	public JavaDBMemoryDB clone() throws CloneNotSupportedException {
		return (JavaDBMemoryDB) super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

	@Override
	public boolean isMemoryDatabase() {
		return true;
	}

//	@Override
//	protected Map<String, String> getExtras() {
//		String jdbcURL = getJdbcURL();
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split(";", 2)[1];
//			return DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", "");
//		} else {
//			return new HashMap<String, String>();
//		}
//	}
//
//	@Override
//	protected String getHost() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:derby://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.split(":")[0];
//		
//	}
//
//	@Override
//	protected String getDatabaseInstance() {
//		String jdbcURL = getJdbcURL();
//		return getExtras().get("instance");
//	}
//
//	@Override
//	protected String getPort() {
//		String jdbcURL = getJdbcURL();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:derby://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.replaceAll("^[^:]*:", "");
//	}
//
//	@Override
//	protected String getSchema() {
//		return "";
//	}

//	@Override
//	protected DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
//		DatabaseConnectionSettings set = new DatabaseConnectionSettings();
//		String noPrefix = jdbcURL.replaceAll("^jdbc:derby://", "");
//		set.setPort(noPrefix
//					.split("/",2)[0]
//					.replaceAll("^[^:]*:", ""));
//		set.setHost(noPrefix
//					.split("/",2)[0]
//					.split(":")[0]);
//		if (jdbcURL.matches(";")) {
//			String extrasString = jdbcURL.split(";", 2)[1];
//			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
//		}
//		set.setInstance(getExtras().get("instance"));
//		set.setSchema("");
//		return set;
//	}

	@Override
	public Integer getDefaultPort() {
		return 1527;
	}

//	@Override
//	protected  Class<? extends DBDatabase> getBaseDBDatabaseClass() {
//		return JavaDBMemoryDB.class;
//	}

	@Override
	protected JDBCURLInterpreter getURLInterpreter() {
		return new JavaDBMemoryURLInterpreter();
	}
}
