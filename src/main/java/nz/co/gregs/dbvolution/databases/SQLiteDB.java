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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition;
import org.sqlite.Function;
import org.sqlite.SQLiteConfig;

public class SQLiteDB extends DBDatabase {

	private static final String SQLITE_DRIVER_NAME = "org.sqlite.JDBC";

	public SQLiteDB(DataSource ds) {
		super(new SQLiteDefinition(), ds);
	}

	public SQLiteDB(String jdbcURL, String username, String password) {
		super(new SQLiteDefinition(), SQLITE_DRIVER_NAME, jdbcURL, username, password);
	}

	@Override
	protected boolean supportsFullOuterJoinNatively() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoin() {
		return false;
	}

	@Override
	protected Connection getConnectionFromDriverManager() throws SQLException {
		SQLiteConfig config = new SQLiteConfig();
		config.enableCaseSensitiveLike(true); 
		Connection connection = DriverManager.getConnection(getJdbcURL(), getUsername(), getPassword());
		config.apply(connection);
		Function.create(connection, "TRUNC", new Trunc());
		Function.create(connection, "LOCATION_OF", new LocationOf());
		return connection;
	}
	
	public static class Trunc extends Function{
		@Override
		protected void xFunc() throws SQLException {
			Double original = value_double(0);
			result(original.longValue());
		}	
	}
	
	public static class LocationOf extends Function{
		@Override
		protected void xFunc() throws SQLException {
			String original = value_text(0);
			String find = value_text(1);
			result(original.indexOf(find)+1);
		}	
	}
	
	public static class StandardDeviation extends Function{
		@Override
		protected void xFunc() throws SQLException {
			String original = value_text(0);
			
			String find = value_text(1);
			result(original.indexOf(find)+1);
		}	
	}
}
