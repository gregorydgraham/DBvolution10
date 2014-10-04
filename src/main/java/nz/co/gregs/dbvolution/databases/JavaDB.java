/*
 * Copyright 2014 gregorygraham.
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

import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.JavaDBDefinition;

public class JavaDB extends DBDatabase {

	private static final String driverName = "org.apache.derby.jdbc.ClientDriver";

	public JavaDB() {
	}

	public JavaDB(DataSource ds) {
		super(new JavaDBDefinition(), ds);
	}

	public JavaDB(String jdbcURL, String username, String password) {
		super(new JavaDBDefinition(), driverName, jdbcURL, username, password);
	}

	public JavaDB(String host, int port, String database, String username, String password) {
		super(new JavaDBDefinition(), driverName, "jdbc:derby://"+host+":"+port+"/" + database + ";create=true", username, password);
	}

	@Override
	protected boolean supportsFullOuterJoinNatively() {
		return false;
	}

}
