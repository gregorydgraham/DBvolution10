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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.NuoDBSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;

/**
 * DBDatabase tweaked to work best with NuoDB.
 *
 * @author Gregory Graham
 */
public class NuoDB extends DBDatabaseImplementation {

	public static final String NUODB_DRIVER = "com.nuodb.jdbc.Driver";
	public static final long serialVersionUID = 1l;

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); 
	}

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	a NuoDB data source
	 * @throws java.sql.SQLException database errors
	 */
	public NuoDB(DataSource ds) throws SQLException {
		super(
				new NuoDBSettingsBuilder().setDataSource(ds)
		);
	}

	/**
	 * Creates a DBDatabase instance tweaked for NuoDB using the broker supplied
	 * on the default port for NuoDB.
	 *
	 * @param brokers brokers
	 * @param schema schema
	 * @param databaseName databaseName
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException database errors
	 */
	public NuoDB(List<String> brokers, String databaseName, String schema, String username, String password) throws SQLException {
		super(
				new NuoDBSettingsBuilder()
						.setDatabaseName(databaseName)
						.setSchema(schema)
						.setUsername(username)
						.setPassword(password)
						.addBrokers(brokers)
		);
	}

	/**
	 * Creates a DBDatabase instance tweaked for NuoDB using the broker supplied
	 * using the ports supplied for each broker.
	 *
	 * @param broker a single NuoDB broker to use.
	 * @param port the port for the broker provided.
	 * @param databaseName the database required from the brokers.
	 * @param schema the schema on the database to be used.
	 * @param username the user to login as.
	 * @param password the user's password.
	 * @throws java.sql.SQLException database errors
	 */
	public NuoDB(String broker, Long port, String databaseName, String schema, String username, String password) throws SQLException {
			super(
				new NuoDBSettingsBuilder()
						.setHost(broker)
						.setPort(port)
						.setDatabaseName(databaseName)
						.setSchema(schema)
						.setUsername(username)
						.setPassword(password)
		);
	}

	/**
	 * Creates a DBDatabase instance tweaked for NuoDB using the broker supplied
	 * using the ports supplied for each broker.
	 *
	 * @param brokers a list of the NuoDB brokers to use.
	 * @param ports a list of the port for each broker provided.
	 * @param databaseName the database required from the brokers.
	 * @param schema the schema on the database to be used.
	 * @param username the user to login as.
	 * @param password the user's password.
	 * @throws java.sql.SQLException database errors
	 */
	public NuoDB(List<String> brokers, List<Long> ports, String databaseName, String schema, String username, String password) throws SQLException {
			super(
				new NuoDBSettingsBuilder()
						.setDatabaseName(databaseName)
						.setSchema(schema)
						.setUsername(username)
						.setPassword(password)
						.addBrokers(brokers, ports)
		);
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		;
	}

	@Override
	public Integer getDefaultPort() {
		return 8888;
	}

	@Override
	public NuoDBSettingsBuilder getURLInterpreter() {
		return new NuoDBSettingsBuilder();
	}
}
