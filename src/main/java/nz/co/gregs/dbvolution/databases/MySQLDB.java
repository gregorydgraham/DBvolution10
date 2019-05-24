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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition_5_7;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.internal.mysql.MigrationFunctions;

/**
 * A DBDatabase tweaked for MySQL databases
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MySQLDB extends DBDatabase implements SupportsPolygonDatatype {

	private final static String MYSQLDRIVERNAME = "com.mysql.jdbc.Driver";
	private static final long serialVersionUID = 1l;
	public static final int DEFAULT_PORT = 3306;
//	private String derivedURL;

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 * @throws java.sql.SQLException
	 */
	public MySQLDB(DataSource ds) throws SQLException {
		super(new MySQLDBDefinition(), MYSQLDRIVERNAME, ds);
	}

	/**
	 * Creates a MySQL connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException
	 */
	public MySQLDB(DatabaseConnectionSettings dcs) throws SQLException {
		super(new MySQLDBDefinition(), MYSQLDRIVERNAME, dcs);
	}

	/**
	 * Creates DBDatabase suitable for use with MySQL attached to the supplied
	 * JDBC URL, username, and password.
	 *
	 * @param jdbcURL jdbcURL
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException
	 */
	public MySQLDB(String jdbcURL, String username, String password) throws SQLException {
		super(new MySQLDBDefinition(), MYSQLDRIVERNAME, jdbcURL, username, password);
	}

	/**
	 * Creates DBDatabase suitable for use with MySQL attached to the supplied
	 * JDBC URL, username, and password.
	 *
	 * @param server the server to connect to.
	 * @param port the port to connect on.
	 * @param databaseName the database that is required on the server.
	 * @param username the user to login as.
	 * @param password the password required to login successfully.
	 * @throws java.sql.SQLException
	 */
	public MySQLDB(String server, long port, String databaseName, String username, String password) throws SQLException {
		super(new MySQLDBDefinition(),
				MYSQLDRIVERNAME,
				"jdbc:mysql://" + server + ":" + port + "/" + databaseName + "?createDatabaseIfNotExist=true&useUnicode=yes&characterEncoding=utf8&characterSetResults=utf8&verifyServerCertificate=false&useSSL=true",
				username,
				password);
		this.setDatabaseName(databaseName);
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : "jdbc:mysql://" 
				+ settings.getHost() + ":" 
				+ settings.getPort() + "/" 
				+ settings.getDatabaseName() 
				+ "?createDatabaseIfNotExist=true&useUnicode=yes&characterEncoding=utf8&characterSetResults=utf8&verifyServerCertificate=false&useSSL=true"
				+ settings.formatExtras("&", "=", "&", "");
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		for (MigrationFunctions fn : MigrationFunctions.values()) {
			fn.add(statement);
		}
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
//		String noPrefix = jdbcURL.replaceAll("^jdbc:mysql://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.split(":")[0];
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
//		String noPrefix = jdbcURL.replaceAll("^jdbc:mysql://", "");
//			return noPrefix
//					.split("/",2)[0]
//					.replaceAll("^[^:]*:+", "");
//	}

//	@Override
//	protected String getSchema() {
//		return "";
//	}
@Override
	protected DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
		DatabaseConnectionSettings set = new DatabaseConnectionSettings();
		String noPrefix = jdbcURL.replaceAll("^jdbc:mysql://", "");
		set.setPort(noPrefix
					.split("/",2)[0]
					.replaceAll("^[^:]*:+", ""));
		set.setHost(noPrefix
					.split("/",2)[0]
					.split(":")[0]);
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split("\\?", 2)[1];
			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		set.setInstance(getExtras().get("instance"));
		set.setSchema("");
		return set;
	}

	@Override
	public Integer getDefaultPort() {
		return 3306;
	}

	private final static Pattern FUNCTION_DOES_NOT_EXISTS = Pattern.compile("FUNCTION [^ ]* does not exist");
	private final static Pattern TABLE_ALREADY_EXISTS = Pattern.compile("Table '[^']*' already exists");
	@Override
	public ResponseToException addFeatureToFixException(Exception exp) throws Exception {
//		System.out.println("nz.co.gregs.dbvolution.databases.MySQLDB.addFeatureToFixException()");
//		System.out.println("nz.co.gregs.dbvolution.databases.MySQLDB.addFeatureToFixException()"+exp.getClass().getCanonicalName());
//		System.out.println("nz.co.gregs.dbvolution.databases.MySQLDB.addFeatureToFixException()"+exp.getMessage());
//		System.out.println("nz.co.gregs.dbvolution.databases.MySQLDB.addFeatureToFixException()"+TABLE_ALREADY_EXISTS.matcher(exp.getMessage()).lookingAt());
		if (TABLE_ALREADY_EXISTS.matcher(exp.getMessage()).matches()){
//		System.out.println("nz.co.gregs.dbvolution.databases.MySQLDB.addFeatureToFixException() TABLE EXISTS WHILE CREATING TABLE: OK.");
			return ResponseToException.SKIPQUERY;
		}else if(FUNCTION_DOES_NOT_EXISTS.matcher(exp.getMessage()).matches()){
			
		}
		return super.addFeatureToFixException(exp);
	}

	@Override
	protected void setDefinitionBasedOnConnectionMetaData(Properties clientInfo, DatabaseMetaData metaData) {
		try {
			if(metaData.getDatabaseMajorVersion()<8){
				setDefinition(new MySQLDBDefinition_5_7());
			}else{
				setDefinition(new MySQLDBDefinition());
			}
		} catch (SQLException ex) {
			final Logger logger = Logger.getLogger(MySQLDB.class.getName());
			logger.log(Level.INFO, "Failed to get connection metadata information to set the database definition");
			logger.log(Level.INFO, null, ex);
		}
	}

	@Override
	protected Class<? extends DBDatabase> getBaseDBDatabaseClass() {
		return MySQLDB.class;
	}
}
