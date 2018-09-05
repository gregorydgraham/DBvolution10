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
import java.sql.Statement;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition;
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
	private String derivedURL;

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 */
	public MySQLDB(DataSource ds) throws SQLException {
		super(new MySQLDBDefinition(), MYSQLDRIVERNAME, ds);
	}

	/**
	 * Creates DBDatabase suitable for use with MySQL attached to the supplied
	 * JDBC URL, username, and password.
	 *
	 * @param jdbcURL jdbcURL
	 * @param password password
	 * @param username username
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

}
