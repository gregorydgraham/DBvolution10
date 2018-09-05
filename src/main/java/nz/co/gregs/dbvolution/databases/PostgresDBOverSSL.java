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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Extends the PostgreSQL database connection by adding SSL.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class PostgresDBOverSSL extends PostgresDB {

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
	protected PostgresDBOverSSL() {
	}

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 */
	public PostgresDBOverSSL(DataSource ds) throws SQLException {
		super(ds);
	}

	/**
	 * Creates a DBDatabase for a PostgreSQL database over SSL.
	 *
	 * @param hostname host name
	 * @param databaseName databaseName
	 * @param port port
	 * @param username username
	 * @param password password
	 * @param urlExtras urlExtras
	 */
	public PostgresDBOverSSL(String hostname, int port, String databaseName, String username, String password, String urlExtras) throws SQLException {
		super(hostname, port, databaseName, username, password, "ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory" + (urlExtras == null || urlExtras.isEmpty() ? "" : "&" + urlExtras));
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : "jdbc:postgresql://"
					+ settings.getHost() + ":"
					+ settings.getPort() + "/"
					+ settings.getDatabaseName()
					+ "ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
					+ settings.formatExtras("&", "=", "&", "");
	}

	/**
	 * Creates a DBDatabase for a PostgreSQL database over SSL.
	 *
	 * @param hostname host name
	 * @param password password
	 * @param databaseName databaseName
	 * @param port port
	 * @param username username
	 */
	public PostgresDBOverSSL(String hostname, int port, String databaseName, String username, String password) throws SQLException {
		this(hostname, port, databaseName, username, password, "");
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

}
