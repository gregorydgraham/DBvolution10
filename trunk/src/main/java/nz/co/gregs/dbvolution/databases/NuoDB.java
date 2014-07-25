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

import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.NuoDBDefinition;

/**
 *
 * @author gregory.graham
 */
public class NuoDB extends DBDatabase {

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	public NuoDB(List<String> brokers, String databaseName, String schema, String username, String password) {
		String hosts = "";
		String sep = "";

		for (String server : brokers) {
			int port = 48004;
			hosts += sep + server + ":" + port;
			sep = ",";
		}

		setDriverName("com.nuodb.jdbc.Driver");
		setDefinition(new NuoDBDefinition());
		setJdbcURL("jdbc:com.nuodb://" + hosts + "/" + databaseName+"?schema="+schema);
		setUsername(username);
		setPassword(password);
		setDatabaseName(databaseName);
	}

	public NuoDB(List<String> brokers, List<Long> ports, String databaseName, String schema, String username, String password) {
		String hosts = "";
		String sep = "";
		if (brokers.size() == ports.size()) {
			for (int i = 0; i < brokers.size(); i++) {
				String server = brokers.get(i);
				Long port = ports.get(i);
				hosts += sep + server + ":" + port;
				sep = ",";
			}
		}
		setDriverName("com.nuodb.jdbc.Driver");
		setDefinition(new NuoDBDefinition());
		setJdbcURL("jdbc:com.nuodb://" + hosts + "/" + databaseName+"?schema="+schema);
		setUsername(username);
		setPassword(password);
		setDatabaseName(databaseName);
	}

	@Override
	protected boolean supportsFullOuterJoinNatively() {
		return false;
	}

}
