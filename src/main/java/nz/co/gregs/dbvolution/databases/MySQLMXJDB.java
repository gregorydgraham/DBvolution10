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

/**
 * DBDatabase tweaked for the MySQL MXJ Database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MySQLMXJDB extends MySQLDB {

	public static final long serialVersionUID = 1l;

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	ds
	 */
	public MySQLMXJDB(DataSource ds) throws SQLException {
		super(ds);
	}

	/**
	 * Creates a DBDatabase tweaked for MySQL MXJ.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public MySQLMXJDB(String jdbcURL, String username, String password) throws SQLException {
		super(jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase tweaked for MySQL MXJ.
	 *
	 * @param server the server to connect to.
	 * @param port the port that the database is listening to.
	 * @param databaseName the name of the database within the server.
	 * @param databaseDir where to set the data files on the server.
	 * @param username the user to login as.
	 * @param password the password required to login successfully.
	 */
	public MySQLMXJDB(String server, long port, String databaseName, String databaseDir, String username, String password) throws SQLException {
		super("jdbc:mysql:mxj://" + server + ":" + port + "/" + databaseName
				+ "?" + "server.basedir=" + databaseDir
				+ "&" + "createDatabaseIfNotExist=true"
				+ "&" + "server.initialize-user=true",
				username,
				password);
		setDatabaseName(databaseName);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}
}
