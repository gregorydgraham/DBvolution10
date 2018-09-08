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
package nz.co.gregs.dbvolution.generic;

import java.io.File;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import net.sourceforge.tedhi.FlexibleDateFormat;
import net.sourceforge.tedhi.FlexibleDateRangeFormat;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.example.*;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
@RunWith(Parameterized.class)
public abstract class AbstractTest {

	public DBDatabase database;
	Marque myMarqueRow = new Marque();
	CarCompany myCarCompanyRow = new CarCompany();
	public DBTable<Marque> marquesTable;
	DBTable<CarCompany> carCompanies;
	public List<Marque> marqueRows = new ArrayList<>();
	public List<CarCompany> carTableRows = new ArrayList<>();
	public static final FlexibleDateFormat TEDHI_FORMAT = FlexibleDateFormat.getPatternInstance("dd/M/yyyy h:m:s", Locale.UK);
	public static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss", Locale.UK);
	public static final FlexibleDateRangeFormat TEDHI_RANGE_FORMAT = FlexibleDateRangeFormat.getPatternInstance("M yyyy", Locale.UK);
	public static String firstDateStr = "23/March/2013 12:34:56";
	public static String secondDateStr = "2/April/2011 1:02:03";
	public static Date march23rd2013 = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).getTime();
	public static Date april2nd2011 = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).getTime();

	@Parameters(name = "{0}")
	public static List<Object[]> data() throws IOException, SQLException, ClassNotFoundException {

		List<Object[]> databases = new ArrayList<>();

		if (System.getProperty("testClusteredDB") != null) {
			databases.add(new Object[]{"ClusteredDB",
				new DBDatabaseCluster(
				H2MemoryTestDB.getClusterDBFromSettings("h2memory"),
				SQLiteTestDB.getClusterDBFromSettings("sqlite"),
				PostgreSQLTestDatabase.getClusterDBFromSettings("postgres"),
				MySQLTestDatabase.getClusterDBFromSettings("mysql")
				)});
		}
		if (System.getProperty("testSmallCluster") != null) {
			databases.add(new Object[]{"ClusteredDB",
				new DBDatabaseCluster(
				SQLiteTestDB.getFromSettings(),
				H2MemoryTestDB.getFromSettings("h2memory")
				)});
		}
		if (System.getProperty("testBundledCluster") != null) {
			databases.add(new Object[]{"ClusteredDB",
				new DBDatabaseCluster(
				SQLiteTestDB.getFromSettings(),
				H2MemoryTestDB.getFromSettings("h2memory")
				)});
		}
		if (System.getProperty("testOpenSourceCluster") != null) {
			databases.add(new Object[]{"ClusteredDB",
				new DBDatabaseCluster(
				H2MemoryTestDB.getFromSettings("h2memory"),
				SQLiteTestDB.getFromSettings(),
				PostgreSQLTestDatabase.getFromSettings("postgres"),
				MySQLTestDatabase.getFromSettings("mysql")
				)});
		}
		if (System.getProperty("MySQL+Cluster") != null) {
			databases.add(new Object[]{"ClusteredDB",
				new DBDatabaseCluster(
				H2MemoryTestDB.getFromSettings("h2memory"),
				SQLiteTestDB.getFromSettings(),
				PostgreSQLTestDatabase.getFromSettings("postgres"),
				MySQLTestDatabase.getFromSettings("mysql")
				)});
			databases.add(new Object[]{"MySQL",
				MySQLTestDatabase.getFromSettings("mysql")
			});
		}
		if (System.getProperty("testSQLite") != null) {
			databases.add(new Object[]{"SQLiteDB", SQLiteTestDB.getFromSettings()});
		}
		if (System.getProperty("testMySQL") != null) {
			databases.add(new Object[]{"MySQLDB", MySQLTestDatabase.getFromSettings("mysql")});
		}
		if (System.getProperty("testMySQL56") != null) {
			databases.add(new Object[]{"MySQLDB-5.6", MySQL56TestDatabase.getFromSettings("mysql56")});
		}
		if (System.getProperty("testH2DB") != null) {
			databases.add(new Object[]{"H2DB", H2TestDatabase.getFromSettings("h2")});
		}
		if (System.getProperty("testH2SharedDB") != null) {
			databases.add(new Object[]{"H2SharedDB", H2TestDatabase.getSharedDBFromSettings("h2shared")});
		}
		if (System.getProperty("testH2FileDB") != null) {
			databases.add(new Object[]{"H2FileDB", H2TestDatabase.getFromSettings("h2file")});
		}
		if (System.getProperty("testH2DataSourceDB") != null) {
			databases.add(new Object[]{"H2DataSourceDB", H2TestDatabase.getFromSettingsUsingDataSource("h2datasource")});
		}
		if (System.getProperty("testPostgresSQL") != null) {
			databases.add(new Object[]{"PostgresSQL", PostgreSQLTestDatabase.getFromSettings("postgres")});
		}
		if (System.getProperty("testNuo") != null) {
			databases.add(new Object[]{"NuoDB", new NuoDB("localhost", 48004L, "dbv", "dbv", "dbv", "dbv")});
		}
		if (System.getProperty("testOracleXE") != null) {
			databases.add(new Object[]{"Oracle11DB", Oracle11XETestDB.getFromSettings("oraclexe")});
		}
		if (System.getProperty("testMSSQLServer") != null) {
			databases.add(new Object[]{"MSSQLServer", MSSQLServerTestDB.getFromSettings("sqlserver")});
		}
		if (System.getProperty("testH2MemoryDB") != null) {
			databases.add(new Object[]{"H2MemoryDB", H2MemoryTestDB.getFromSettings("h2memory")});
		}
		if (databases.isEmpty() || System.getProperty("testH2BlankDB") != null) {
			databases.add(new Object[]{"H2BlankDB", H2MemoryTestDB.blankDB()});
		}

		return databases;
	}

	public AbstractTest(Object testIterationName, Object db) {
		if (db instanceof DBDatabase) {
			this.database = (DBDatabase) db;
		}
	}

	public String testableSQL(String str) {
		if (str != null) {
			String trimStr = str.trim().replaceAll("[ \\r\\n]+", " ").toLowerCase();
			if ((database instanceof OracleDB) || (database instanceof JavaDB)) {
				return trimStr
						.replaceAll("\"", "")
						.replaceAll(" oo", " ")
						.replaceAll("\\b_+", "")
						.replaceAll(" +[aA][sS] +", " ")
						.replaceAll(" *; *$", "");
			} else if (database instanceof PostgresDB) {
				return trimStr.replaceAll("::[a-zA-Z]*", "");
			} else if ((database instanceof NuoDB)) {
				return trimStr.replaceAll("\\(\\(([^)]*)\\)=true\\)", "$1");
			} else if (database instanceof MSSQLServerDB) {
				return trimStr.replaceAll("[\\[\\]]", "");
			} else {
				return trimStr;
			}
		} else {
			return str;
		}
	}

	public String testableSQLWithoutColumnAliases(String str) {
		if (str != null) {
			String trimStr = str
					.trim()
					.replaceAll(" DB[_0-9]+", "")
					.replaceAll("[ \\r\\n]+", " ")
					.toLowerCase();
			if ((database instanceof OracleDB)
					|| (database instanceof JavaDB)) {
				return trimStr
						.replaceAll("\"", "")
						.replaceAll("\\boo", "__")
						.replaceAll("\\b_+", "")
						.replaceAll(" *; *$", "")
						.replaceAll(" as ", " ");
			} else if ((database instanceof NuoDB)) {
				return trimStr.replaceAll("\\(\\(([^)]*)\\)=true\\)", "$1");
			} else if ((database instanceof MSSQLServerDB)) {
				return trimStr
						.replaceAll("\\[", "")
						.replaceAll("]", "")
						.replaceAll(" *;", "");
			} else {
				return trimStr;
			}
		} else {
			return str;
		}
	}

	@Before
	@SuppressWarnings("empty-statement")
	public void setUp() throws Exception {
		setup(database);
	}

	public void setup(DBDatabase database) throws Exception {
		database.setPrintSQLBeforeExecuting(false);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new Marque());
		database.createTable(myMarqueRow);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(myCarCompanyRow);
		database.createTable(myCarCompanyRow);

		marquesTable = DBTable.getInstance(database, myMarqueRow);
		carCompanies = DBTable.getInstance(database, myCarCompanyRow);
		carCompanies.insert(new CarCompany("TOYOTA", 1));
		carTableRows.add(new CarCompany("Ford", 2));
		carTableRows.add(new CarCompany("GENERAL MOTORS", 3));
		carTableRows.add(new CarCompany("OTHER", 4));
		carCompanies.insert(carTableRows);

		Date firstDate = DATETIME_FORMAT.parse(firstDateStr);
		Date secondDate = DATETIME_FORMAT.parse(secondDateStr);

		marqueRows.add(new Marque(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		marqueRows.add(new Marque(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", firstDate, 2, false));
		marqueRows.add(new Marque(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", firstDate, 3, null));
		marqueRows.add(new Marque(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", firstDate, 1, null));
		marqueRows.add(new Marque(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", firstDate, 3, null));
		marqueRows.add(new Marque(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", secondDate, 4, null));
		marqueRows.add(new Marque(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", secondDate, 4, null));
		marqueRows.add(new Marque(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", firstDate, 1, true));
		marqueRows.add(new Marque(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", secondDate, 3, null));

		marquesTable.insert(marqueRows);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new CompanyLogo());
		database.createTable(new CompanyLogo());

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new CompanyText());
		database.createTable(new CompanyText());

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new LinkCarCompanyAndLogo());
		database.createOrUpdateTable(new LinkCarCompanyAndLogo());
	}

	@After
	public void tearDown() throws Exception {
		tearDown(database);
	}

	public void tearDown(DBDatabase database) throws Exception {
	}

	private static class H2TestDatabase extends H2DB {

		public static final long serialVersionUID = 1l;

		public static H2DB getFromSettings(String prefix) throws SQLException, IOException {
			String url = System.getProperty(prefix + ".url");
			String host = System.getProperty(prefix + ".host");
			String port = System.getProperty(prefix + ".port");
			String instance = System.getProperty(prefix + ".instance");
			String database = System.getProperty(prefix + ".database");
			String username = System.getProperty(prefix + ".username");
			String password = System.getProperty(prefix + ".password");
			String schema = System.getProperty(prefix + ".schema");
			String file = System.getProperty(prefix + ".file");
			if (file != null && !file.isEmpty()) {
				return H2TestDatabaseFromFilename(file, username, password);
			} else {
				return new H2TestDatabase(url, username, password);
			}
		}

		public static H2DB getSharedDBFromSettings(String prefix) throws SQLException, IOException {
			DatabaseConnectionSettings settings = DatabaseConnectionSettings.getSettingsfromSystemUsingPrefix(prefix + ".");
			return new H2SharedDB(settings);
		}

		public static H2DB getClusterDBFromSettings(String prefix) throws SQLException, IOException {
			String url = System.getProperty(prefix + ".url");
			String host = System.getProperty(prefix + ".host");
			String port = System.getProperty(prefix + ".port");
			String instance = System.getProperty(prefix + ".instance");
			String database = System.getProperty(prefix + ".database");
			String username = System.getProperty(prefix + ".username");
			String password = System.getProperty(prefix + ".password");
			String schema = System.getProperty(prefix + ".schema");
			String file = System.getProperty(prefix + ".file");
			if (file != null && !file.equals("")) {
				System.out.println("MAKING H2DB with FILENAME: " + file);
				return H2TestDatabaseFromFilename(file + "-cluster.h2db", username, password);
			} else {
				return new H2TestDatabase(url, username, password);
			}
		}

		public static H2DB getFromSettingsUsingDataSource(String prefix) throws SQLException {
			String url = System.getProperty(prefix + ".url");
			String host = System.getProperty(prefix + ".host");
			String port = System.getProperty(prefix + ".port");
			String instance = System.getProperty(prefix + ".instance");
			String database = System.getProperty(prefix + ".database");
			String username = System.getProperty(prefix + ".username");
			String password = System.getProperty(prefix + ".password");
			String schema = System.getProperty(prefix + ".schema");
			String file = System.getProperty(prefix + ".file");

			JdbcDataSource h2DataSource = new JdbcDataSource();
			h2DataSource.setUser(username);
			h2DataSource.setPassword(password);
			h2DataSource.setURL(url);
			return H2TestDatabase.getDatabaseFromDataSource(h2DataSource);
		}

		private static H2DB getDatabaseFromDataSource(JdbcDataSource h2DataSource) throws SQLException {
			return new H2DB(h2DataSource);
		}

		public static H2DB H2TestDatabaseFromFilename(String file, String username, String password) throws SQLException, IOException {
			return new H2DB(new File(file), username, password);
		}

		public H2TestDatabase(String url, String username, String password) throws SQLException {
			super(url, username, password);
		}
	}

	private static class MySQL56TestDatabase extends MySQLDB {

		public static final long serialVersionUID = 1l;

		public static MySQL56TestDatabase getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return new MySQL56TestDatabase(url, username, password);
		}

		public MySQL56TestDatabase(String url, String username, String password) throws SQLException {
			super(url, username, password);
		}
	}

	private static class MySQLTestDatabase extends MySQLDB {

		public static final long serialVersionUID = 1l;

		public static MySQLTestDatabase getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return new MySQLTestDatabase(host, port, database, username, password, schema);
		}

		public static MySQLTestDatabase getClusterDBFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database") + "_cluster";
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema") + "cluster";
			return new MySQLTestDatabase(host, port, database, username, password, schema);
		}

		public MySQLTestDatabase(String host, String port, String database, String username, String password, String schema) throws SQLException {
			super(host, new Integer(port), database, username, password);
		}

		public MySQLTestDatabase(String url, String username, String password) throws SQLException {
			super(url, username, password);
		}
	}

	private static class PostgreSQLTestDatabase extends PostgresDB {
		
		private final static long serialVersionUID = 1l;

		public static PostgresDB getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return PostgreSQLTestDatabase.getTestDatabase(url, host, port, instance, username, password, schema);
		}

		public static PostgresDB getClusterDBFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database") + "_cluster";
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return PostgreSQLTestDatabase.getTestDatabase(url, host, port, database, username, password, schema);
		}

		protected static PostgresDB getTestDatabase(String url, String host, String port, String database, String username, String password, String schema) throws SQLException {
			return new PostgresDB(host, new Integer(port), database, username, password);
		}
	}

	private static class SQLiteTestDB extends SQLiteDB {
		
		private final static long serialVersionUID = 1l;

		public static SQLiteTestDB getFromSettings() throws IOException, SQLException {
			return getFromSettings("sqlite");
		}

		public static SQLiteTestDB getFromSettings(String prefix) throws IOException, SQLException {
			String url = System.getProperty(prefix + ".url");
//			String filename = System.getProperty(prefix + ".filename");
			String username = System.getProperty(prefix + ".username");
			String password = System.getProperty(prefix + ".password");
			return new SQLiteTestDB(url, username, password);
		}

		public static SQLiteTestDB getClusterDBFromSettings(String prefix) throws IOException, SQLException {
//			String url = System.getProperty(prefix + ".url");
			String filename = System.getProperty(prefix + ".filename") + "-cluster.sqlite";
			String username = System.getProperty(prefix + ".username");
			String password = System.getProperty(prefix + ".password");
			return new SQLiteTestDB(new File(filename), username, password);
		}

		public SQLiteTestDB(String jdbcurl, String username, String password) throws IOException, SQLException {
			super(jdbcurl, username, password);
		}

		private SQLiteTestDB(File file, String username, String password) throws IOException, SQLException {
			super(file, username, password);
		}
	}

	private static class Oracle11XETestDB extends Oracle11XEDB {
		
		private final static long serialVersionUID = 1l;

		public static Oracle11XETestDB getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return new Oracle11XETestDB(host, port, instance, username, password);
		}

		public Oracle11XETestDB(String host, String port, String instance, String username, String password) throws SQLException {
			super(host, Integer.parseInt(port), instance, username, password);
		}
	}

	private static class MSSQLServerTestDB extends MSSQLServer2012DB {
		
		private final static long serialVersionUID = 1l;

		public static MSSQLServerTestDB getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return new MSSQLServerTestDB(host, instance, database, port, username, password);
		}

		public MSSQLServerTestDB(String host, String instance, String database, String port, String username, String password) throws SQLException {
			super(host, instance, database, Integer.parseInt(port), username, password);
		}
	}

	private static class H2MemoryTestDB extends H2MemoryDB {

		public static final long serialVersionUID = 1l;

		public static H2MemoryTestDB getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return new H2MemoryTestDB(instance, username, password);
		}

		public static H2MemoryTestDB getClusterDBFromSettings(String prefix) throws SQLException {
			String url = System.getProperty(prefix + ".url");
			String host = System.getProperty(prefix + ".host");
			String port = System.getProperty(prefix + ".port");
			String instance = System.getProperty(prefix + ".instance");
			String database = System.getProperty(prefix + ".database");
			String username = System.getProperty(prefix + ".username");
			String password = System.getProperty(prefix + ".password");
			String schema = System.getProperty(prefix + ".schema");
			String file = System.getProperty(prefix + ".file");
			if (file != null && !file.equals("")) {
				return new H2MemoryTestDB(file + "-cluster.h2db", username, password);
			} else {
				return new H2MemoryTestDB("cluster.h2db", username, password);
			}
		}

		public static H2MemoryTestDB blankDB() throws SQLException {
			return new H2MemoryTestDB("Blank", "", "");
		}

		public H2MemoryTestDB() throws SQLException {
			this("memoryTest.h2db", "", "");
		}

		public H2MemoryTestDB(String instance, String username, String password) throws SQLException {
			super(instance, username, password, false);
		}
	}
}
