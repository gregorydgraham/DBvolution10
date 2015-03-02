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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.datatypes.DateRepeatImpl;
import org.sqlite.Function;
import org.sqlite.SQLiteConfig;

/**
 * Creates a DBDatabase for an SQLite database.
 *
 * @author Gregory Graham
 */
public class SQLiteDB extends DBDatabase implements SupportsDateRepeatDatatypeFunctions {

	private static final String SQLITE_DRIVER_NAME = "org.sqlite.JDBC";

	/**
	 * Creates a DBDatabase tweaked for a SQLite database on the DataSource
	 * provided.
	 *
	 * @param ds	ds
	 */
	public SQLiteDB(DataSource ds) {
		super(new SQLiteDefinition(), ds);
	}

	/**
	 * Creates a DBDatabase tweaked for a SQLite database using the parameters
	 * provided.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
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
		addDateRepeatFunctions(connection);
		return connection;
	}

	private void addDateRepeatFunctions(Connection connection) throws SQLException {
		Function.create(connection, SQLiteDefinition.DATEREPEAT_CREATION_FUNCTION, new DateRepeatCreate());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_EQUALS_FUNCTION, new DateRepeatEquals());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_LESSTHAN_FUNCTION, new DateRepeatLessThan());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_GREATERTHAN_FUNCTION, new DateRepeatGreaterThan());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_LESSTHANEQUALS_FUNCTION, new DateRepeatLessThanOrEqual());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_GREATERTHANEQUALS_FUNCTION, new DateRepeatGreaterThanOrEqual());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_DATEADDITION_FUNCTION, new DateRepeatDateAddition());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_DATESUBTRACTION_FUNCTION, new DateRepeatDateSubtraction());
		
		Function.create(connection, SQLiteDefinition.DATEREPEAT_YEAR_PART_FUNCTION, new DateRepeatGetYear());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_MONTH_PART_FUNCTION, new DateRepeatGetMonth());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_DAY_PART_FUNCTION, new DateRepeatGetDay());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_HOUR_PART_FUNCTION, new DateRepeatGetHour());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_MINUTE_PART_FUNCTION, new DateRepeatGetMinute());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_SECOND_PART_FUNCTION, new DateRepeatGetSecond());
		Function.create(connection, SQLiteDefinition.DATEREPEAT_MILLISECOND_PART_FUNCTION, new DateRepeatGetMillisecond());
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

	private static class DateRepeatCreate extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				SQLiteDefinition defn = new SQLiteDefinition();
				final String originalStr = value_text(0);
				final String compareToStr = value_text(1);
				if (originalStr == null || compareToStr == null) {
					result((String) null);
				} else {
					Date original = defn.parseDateFromGetString(originalStr);
					Date compareTo = defn.parseDateFromGetString(compareToStr);
					String intervalString = DateRepeatImpl.repeatFromTwoDates(original, compareTo);
					result(intervalString);
				}
			} catch (ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Date", ex);
			}
		}
	}

	private static class DateRepeatEquals extends Function {

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);

			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result == 0 ? 1 : 0);
			}
		}
	}

	private static class DateRepeatLessThan extends Function {

		DateRepeatLessThan() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);

			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result == -1 ? 1 : 0);
			}
		}
	}

	private static class DateRepeatGreaterThan extends Function {

		DateRepeatGreaterThan() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);

			if (originalStr == null || compareToStr == null || originalStr.length() == 0 || compareToStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result == 1 ? 1 : 0);
			}
		}
	}

	private static class DateRepeatLessThanOrEqual extends Function {

		DateRepeatLessThanOrEqual() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);

			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result < 1 ? 1 : 0);
			}
		}
	}

	private static class DateRepeatGreaterThanOrEqual extends Function {

		DateRepeatGreaterThanOrEqual() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);

			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result > -1 ? 1 : 0);
			}
		}
	}

	private static class DateRepeatDateAddition extends Function {

		DateRepeatDateAddition() {
		}

		@Override
		protected void xFunc() throws SQLException {
			try {
				SQLiteDefinition defn = new SQLiteDefinition();
				final String dateStr = value_text(0);
				final String intervalStr = value_text(1);

				if (dateStr == null || intervalStr == null || intervalStr.length() == 0 || dateStr.length() == 0) {
					result((String) null);
				} else {
					Date date = defn.parseDateFromGetString(dateStr);
					Date result = DateRepeatImpl.addDateAndDateRepeatString(date, intervalStr);
					result(defn.formatDateForGetString(result));
				}
			} catch (ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new DBRuntimeException("Failed To Parse SQLite Date", ex);
			}
		}
	}

	private static class DateRepeatDateSubtraction extends Function {

		DateRepeatDateSubtraction() {
		}

		@Override
		protected void xFunc() throws SQLException {
			try {
				SQLiteDefinition defn = new SQLiteDefinition();
				final String dateStr = value_text(0);
				final String intervalStr = value_text(1);

				if (dateStr == null || intervalStr == null||dateStr.length()==0||intervalStr.length()==0) {
					result((String) null);
				} else {
					Date date = defn.parseDateFromGetString(dateStr);
					Date result = DateRepeatImpl.subtractDateAndDateRepeatString(date, intervalStr);
					result(defn.formatDateForGetString(result));
				}
			} catch (ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new DBRuntimeException("Failed To Parse SQLIte Date", ex);
			}
		}
	}

	private static class DateRepeatGetYear extends Function {

		DateRepeatGetYear() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null||intervalStr.length()==0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getYearPart(intervalStr);
				result(result);
			}
		}
	}

	private static class DateRepeatGetMonth extends Function {

		DateRepeatGetMonth() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null||intervalStr.length()==0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getMonthPart(intervalStr);
				result(result);
			}
		}
	}

	private static class DateRepeatGetDay extends Function {

		DateRepeatGetDay() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null||intervalStr.length()==0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getDayPart(intervalStr);
				result(result);
			}
		}
	}

	private static class DateRepeatGetHour extends Function {

		DateRepeatGetHour() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null||intervalStr.length()==0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getHourPart(intervalStr);
				result(result);
			}
		}
	}

	private static class DateRepeatGetMinute extends Function {

		DateRepeatGetMinute() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null||intervalStr.length()==0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getMinutePart(intervalStr);
				result(result);
			}
		}
	}

	private static class DateRepeatGetSecond extends Function {

		DateRepeatGetSecond() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null||intervalStr.length()==0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getSecondPart(intervalStr);
				result(result);
			}
		}
	}

	private static class DateRepeatGetMillisecond extends Function {

		DateRepeatGetMillisecond() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null||intervalStr.length()==0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getMillisecondPart(intervalStr);
				result(result);
			}
		}
	}
}
