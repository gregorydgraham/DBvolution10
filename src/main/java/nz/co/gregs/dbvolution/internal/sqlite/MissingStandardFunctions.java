/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.internal.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import org.sqlite.Function;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class MissingStandardFunctions {

	/**
	 *
	 * @param db
	 * @param connection
	 * @throws SQLException
	 */
	public static void addFunctions(SQLiteDB db, Connection connection) throws SQLException {
		Function.create(connection, "TRUNC", new Trunc());
		Function.create(connection, "LOCATION_OF", new LocationOf());
		Function.create(connection, "CURRENT_USER", new CurrentUser(db.getUsername()));
		Function.create(connection, "STDEV", new StandardDeviation());
		Function.create(connection, "REGEXP_REPLACE", new RegexpReplace());
	}

	private static class RegexpReplace extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String original = value_text(0);
			String regexp = value_text(1);
			String replace = value_text(2);
			result(original.replaceAll(regexp, replace));
		}
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
		 * 1 Database exceptions may be thrown
		 */
		@Override
		protected void xStep() throws SQLException {
			Long longValue = value_long(0);
			longs.add(longValue);
		}

		/**
		 * Produces the final result from the aggregation process.
		 *
		 * 1 Database exceptions may be thrown
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

	private MissingStandardFunctions() {
	}

}
