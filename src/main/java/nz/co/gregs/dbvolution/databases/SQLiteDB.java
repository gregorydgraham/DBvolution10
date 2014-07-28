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
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition;
import org.sqlite.Function;
import org.sqlite.SQLiteConfig;

/**
 * Creates a DBDatabase for an SQLite database.
 *
 * @author gregorygraham
 */
public class SQLiteDB extends DBDatabase {

	private static final String SQLITE_DRIVER_NAME = "org.sqlite.JDBC";

	/**
	 * Creates a DBDatabase tweaked for a SQLite database on the DataSource
	 * provided.
	 *
	 * @param ds
	 */
	public SQLiteDB(DataSource ds) {
		super(new SQLiteDefinition(), ds);
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database using the parameters
	 * provided.
	 *
	 * @param jdbcURL
	 * @param username
	 * @param password
	 */
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
		Function.create(connection, "CURRENT_USER", new CurrentUser(getUsername()));
		Function.create(connection, "STDEV", new StandardDeviation());
		return connection;
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	private static class Trunc extends Function {

		@Override
		protected void xFunc() throws SQLException {
			Double original = value_double(0);
			result(original.longValue());
		}
	}

	private static class LocationOf extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String original = value_text(0);
			String find = value_text(1);
			result(original.indexOf(find) + 1);
		}
	}

	private static class CurrentUser extends Function {

		private final String currentUser;

		public CurrentUser(String currentUser) {
			this.currentUser = currentUser;
		}

		@Override
		protected void xFunc() throws SQLException {
			result(currentUser);
		}
	}

//	Function.create(conn, "mySum", new Function.Aggregate() {
	private static class StandardDeviation extends Function.Aggregate {

		private final List<Long> longs = new ArrayList<Long>();

		/**
		 * Performed for each row during the aggregation process
		 *
		 * @throws SQLException
		 */
		@Override
		protected void xStep() throws SQLException {
			Long longValue = value_long(0);
			longs.add(longValue);
		}

		/**
		 * Produces the final result from the aggregation process.
		 * 
		 * @throws SQLException 
		 */
		@Override
		protected void xFinal() throws SQLException {
			double sum = 0.0;
			for (Long long1 : longs) {
				sum += long1;
			}
			Double mean = sum / longs.size();
			List<Double> squaredDistances = new ArrayList<Double>();
			for (Long long1 : longs) {
				final double dist = mean - long1;
				squaredDistances.add(dist * dist);
			}
			for (Double dist : squaredDistances) {
				sum += dist;
			}
			double variance = sum / squaredDistances.size();
			result(Math.sqrt(variance));
		}

		@Override
		public Object clone() throws CloneNotSupportedException {
			return super.clone(); //To change body of generated methods, choose Tools | Templates.
		}
	}
}
