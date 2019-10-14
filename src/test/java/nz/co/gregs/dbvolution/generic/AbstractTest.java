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
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.OracleContainer;

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
//	public static final FlexibleDateFormat TEDHI_FORMAT = FlexibleDateFormat.getPatternInstance("dd/M/yyyy h:m:s", Locale.UK);
	public static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss", Locale.UK);
	public static final DateTimeFormatter LOCALDATETIME_FORMAT = DateTimeFormatter.ofPattern("dd/MMMM/y HH:mm:ss");
	public static final FlexibleDateRangeFormat TEDHI_RANGE_FORMAT = FlexibleDateRangeFormat.getPatternInstance("M yyyy", Locale.UK);
	public static String firstDateStr = "23/March/2013 12:34:56";
	public static String secondDateStr = "2/April/2011 1:02:03";
	public static Date march23rd2013 = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).getTime();
	public static Date april2nd2011 = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).getTime();

	@Parameters(name = "{0}")
	public static List<Object[]> data() throws IOException, SQLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		List<Object[]> databases = new ArrayList<>();

		if (System.getProperty("testSmallCluster") != null) {
			databases.add(new Object[]{"ClusteredDB-H2+SQLite",
				new DBDatabaseCluster("testSmallCluster", DBDatabaseCluster.Configuration.manual(),
				SQLiteTestDB.getFromSettings(),
				H2MemoryTestDB.getFromSettings("h2memory")
				)});
		}
		if (System.getProperty("testBundledCluster") != null) {
			databases.add(new Object[]{"ClusteredDB-H2+SQLite",
				new DBDatabaseCluster("testSmallCluster", DBDatabaseCluster.Configuration.manual(),
				SQLiteTestDB.getClusterDBFromSettings("sqlite", "bundled"),
				H2MemoryTestDB.getFromSettings("h2memory")
				)});
		}
		if (System.getProperty("testOpenSourceCluster") != null) {
			databases.add(new Object[]{"ClusteredDB-H2+SQLite+Postgres+MySQL",
				new DBDatabaseCluster("testOpenSourceCluster", DBDatabaseCluster.Configuration.manual(),
				H2MemoryTestDB.getFromSettings("h2memory"),
				SQLiteTestDB.getClusterDBFromSettings("sqlite", "open"),
				PostgreSQLTestDatabase.getFromSettings("postgres"),
				MySQLTestDatabase.getFromSettings("mysql")
				)});
		}
		if (System.getProperty("testFullCluster") != null) {
			databases.add(new Object[]{"ClusteredDB-H2+SQLite+Postgres+MySQL+SQLServer",
				new DBDatabaseCluster("testFullCluster", DBDatabaseCluster.Configuration.manual(),
				H2MemoryTestDB.getFromSettings("h2memory"),
				SQLiteTestDB.getClusterDBFromSettings("sqlite", "full"),
				PostgreSQLTestDatabase.getFromSettings("postgres"),
				MySQLTestDatabase.getFromSettings("mysql"),
				getMSSQLServerContainerDatabaseForCluster()
				)});
		}
		if (System.getProperty("MySQL+Cluster") != null) {
			databases.add(new Object[]{"ClusteredDB-H2+SQLite+Postgres+MySQL",
				new DBDatabaseCluster("MySQL+Cluster", DBDatabaseCluster.Configuration.manual(),
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
		if (System.getProperty("testOracleXEContainer") != null) {
			databases.add(new Object[]{"Oracle11XEContainer", Oracle11XEContainerTestDB.getFromSettings("oraclexecontainer")});
		}
		if (System.getProperty("testMSSQLServerContainer") != null) {
			databases.add(new Object[]{"MSSQLServerContainer", getMSSQLServerContainerDatabase()});
		}
		if (System.getProperty("testMSSQLServerLocal") != null) {
			databases.add(new Object[]{"MSSQLServerLocal", MSSQLServerLocalTestDB.getFromSettings("sqlserver")});
		}
		if (System.getProperty("testH2MemoryDB") != null) {
			databases.add(new Object[]{"H2MemoryDB", H2MemoryTestDB.getFromSettings("h2memory")});
		}
		if (databases.isEmpty() || System.getProperty("testH2BlankDB") != null) {
			databases.add(new Object[]{"H2BlankDB", H2MemoryTestDB.blankDB()});
		}

		for (Object[] database : databases) {
			System.out.println("Processing: Database " + database[0]);
		}

		return databases;
	}

	private static MSSQLServerContainerTestDB MSSQLSERVER_CONTAINER_DATABASE = null;
	private static MSSQLServerContainerTestDB MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER = null;

	private static MSSQLServerContainerTestDB getMSSQLServerContainerDatabase() {
		if (MSSQLSERVER_CONTAINER_DATABASE == null) {
			MSSQLSERVER_CONTAINER_DATABASE = MSSQLServerContainerTestDB.getInstance();
		}
		return MSSQLSERVER_CONTAINER_DATABASE;
	}

	private static MSSQLServerContainerTestDB getMSSQLServerContainerDatabaseForCluster() {
		if (MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER == null) {
			MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER = MSSQLServerContainerTestDB.getInstance();
		}
		return MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER;
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
		database.preventDroppingOfTables(false);
		database.dropTableIfExists(new Marque());
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

		public static DBDatabase getSharedDBFromSettings(String prefix)
				throws SQLException, IOException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			DatabaseConnectionSettings settings = DatabaseConnectionSettings.getSettingsfromSystemUsingPrefix(prefix + ".");
			settings.setDbdatabaseClass(H2SharedDB.class.getCanonicalName());
			return settings.createDBDatabase();
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
//				System.out.println("MAKING H2DB with FILENAME: " + file);
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
			super(host, Integer.valueOf(port), database, username, password);
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
			return new PostgresDB(host, Integer.valueOf(port), database, username, password);
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

		public static SQLiteTestDB getClusterDBFromSettings(String prefix, String name) throws IOException, SQLException {
//			String url = System.getProperty(prefix + ".url");
			String filename = System.getProperty(prefix + ".filename") + "-" + name + "cluster.sqlite";
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

	private static class Oracle11XEContainerTestDB extends Oracle11XEDB {

		private final static long serialVersionUID = 1l;
		private static OracleContainer container = null;
		private static Oracle11XEContainerTestDB staticDatabase;

		public static Oracle11XEContainerTestDB getFromSettings(String prefix) throws SQLException {
			if (container == null) {
				String url = System.getProperty("" + prefix + ".url");
				String instance = System.getProperty("" + prefix + ".instance", "xe");
				String database = System.getProperty("" + prefix + ".database");
				String username = System.getProperty("" + prefix + ".username", "system");
				String password = System.getProperty("" + prefix + ".password", "oracle");
				String schema = System.getProperty("" + prefix + ".schema");

				container = new OracleContainer("oracleinanutshell/oracle-xe-11g");
				container.start();
				String host = container.getContainerIpAddress();
				Integer port = container.getMappedPort(1521);

				staticDatabase = new Oracle11XEContainerTestDB(host, port, instance, username, password);
			}
			return staticDatabase;
		}

		private Oracle11XEContainerTestDB(String host, Integer port, String instance, String username, String password) throws SQLException {
			super(host, port, instance, username, password);
		}
	}

	private static class MSSQLServerLocalTestDB extends MSSQLServerDB {

		private final static long serialVersionUID = 1l;

		public static MSSQLServerLocalTestDB getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			System.out.println("nz.co.gregs.dbvolution.generic.AbstractTest.MSSQLServerLocalTestDB.getFromSettings()");
			System.out.println("" + host + " : " + instance + " : " + database + " : " + port + " : " + username + " : " + password);
			return new MSSQLServerLocalTestDB(host, instance, database, port, username, password);
		}

		public MSSQLServerLocalTestDB(String host, String instance, String database, String port, String username, String password) throws SQLException {
			super(host, instance, database, Integer.parseInt(port), username, password);
		}
	}

	public static class MSSQLServerContainerTestDB extends MSSQLServerDB {

		private final static long serialVersionUID = 1l;

		protected final GenericContainer mssqlServerContainer;

		public static MSSQLServerContainerTestDB getInstance() {
			String instance = "MSSQLServer";
			String database = "";
			String username = "sa";
			String password = "Password23";

			/*
					ACCEPT_EULA=Y accepts the agreement with MS and allows the database instance to start
					SA_PASSWORD=Password23 defines the password so we can login
					'TZ=Pacific/Auckland' sets the container timezone to where I do my test (TODO set to server location)
			 */
			
			MSSQLServerContainer container
					= //new GenericContainer<>("mcr.microsoft.com/mssql/server:2019-CTP3.2-ubuntu")
					//new GenericContainer<>("microsoft/mssql-server-linux:2017-CU13")
					new MSSQLServerContainer<>()//"mcr.microsoft.com/mssql/server:2019-CTP3.2-ubuntu")
//							.withEnv("ACCEPT_EULA", "Y")
//							.withEnv("SA_PASSWORD", password)
//							.withEnv("MSSQL_SA_PASSWORD", password)
//							.withEnv("TZ", ZoneId.systemDefault().getId())
//							.withStartupTimeout(Duration.ofSeconds(30))
//							.withExposedPorts(1433)
//							.withStartupTimeout(Duration.ofMinutes(5))
					;
			container.withEnv("TZ", "Pacific/Auckland");
//			container.withEnv("TZ", ZoneId.systemDefault().getId());
			container.start();
			password = container.getPassword();
			username = container.getUsername();
			String url = container.getJdbcUrl();
			String host = container.getContainerIpAddress();
			Integer port = container.getFirstMappedPort();

			System.out.println("nz.co.gregs.dbvolution.generic.AbstractTest.MSSQLServerContainerTestDB.getInstance()");
			System.out.println("URL: "+url);
			System.out.println("" + host + " : " + instance + " : " + database + " : " + port + " : " + username + " : " + password);
			MSSQLServerContainerTestDB staticDatabase;
			try {
				staticDatabase = new MSSQLServerContainerTestDB(container, host, instance, database, port, username, password);
				return staticDatabase;
			} catch (SQLException ex) {
				Logger.getLogger(AbstractTest.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Unable To Create MSSQLServer Database in Docker Container", ex);
			}
		}

		public MSSQLServerContainerTestDB(GenericContainer container, String host, String instance, String database, Integer port, String username, String password) throws SQLException {
			super(host, instance, database, port, username, password);
			this.mssqlServerContainer = container;
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
