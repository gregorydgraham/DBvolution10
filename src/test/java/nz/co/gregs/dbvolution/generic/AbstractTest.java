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

import nz.co.gregs.dbvolution.databases.MSSQLServer2017ContainerDB;
import nz.co.gregs.dbvolution.databases.Postgres10ContainerDB;
import nz.co.gregs.dbvolution.databases.Oracle11XEContainerDB;
import nz.co.gregs.dbvolution.databases.MySQL8ContainerDB;
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
import net.sourceforge.tedhi.FlexibleDateRangeFormat;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.*;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
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
	static List<Object[]> databases = new ArrayList<>(0);
	Marque myMarqueRow = new Marque();
	CarCompany myCarCompanyRow = new CarCompany();
	public DBTable<Marque> marquesTable;
	DBTable<CarCompany> carCompanies;
	public List<Marque> marqueRows = new ArrayList<>();
	public List<CarCompany> carTableRows = new ArrayList<>();
	public static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss", Locale.UK);
	public static final DateTimeFormatter LOCALDATETIME_FORMAT = DateTimeFormatter.ofPattern("dd/MMMM/y HH:mm:ss");
	public static final FlexibleDateRangeFormat TEDHI_RANGE_FORMAT = FlexibleDateRangeFormat.getPatternInstance("M yyyy", Locale.UK);
	public static String firstDateStr = "23/March/2013 12:34:56";
	public static String secondDateStr = "2/April/2011 1:02:03";
	public static Date march23rd2013 = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).getTime();
	public static Date april2nd2011 = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).getTime();

	@Parameters(name = "{0}")
	public static List<Object[]> data() throws IOException, SQLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		if (databases.isEmpty()) {
			getDatabasesFromSettings();
		}
		databases.forEach(database -> {
			System.out.println("Processing: Database " + database[0]);
		});
		return databases;
	}

	public synchronized final DBDatabase getDatabaseThatDoesNotSupportDifferenceBetweenEmptyStringsAndNull() throws SQLException {
		final String name = "DatabaseThatDoesNotSupportDifferenceBetweenEmptyStringsAndNull-" + (Math.round(Math.random() * 1000000000));
		return new H2MemorySettingsBuilder()
				.setLabel(name)
				.setDatabaseName(name)
				.setDefinition(new H2DBDefinition().getOracleCompatibleVersion())
				.getDBDatabase();
	}

	protected synchronized static void getDatabasesFromSettings() throws InvocationTargetException, IllegalArgumentException, IOException, InstantiationException, SQLException, IllegalAccessException, ClassNotFoundException, SecurityException, NoSuchMethodException {
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
				getPostgresContainerDatabaseForCluster(),
				MySQLTestDatabase.getFromSettings("mysql")
				)});
		}
		if (System.getProperty("testFullCluster") != null) {
			final H2MemoryTestDB h2Mem = H2MemoryTestDB.getFromSettings("h2memory");
			final SQLiteTestDB sqlite = SQLiteTestDB.getClusterDBFromSettings("sqlite", "full");
			final PostgresDB postgres = PostgreSQLTestDatabaseProvider.getFromSettings("postgresfullcluster");
//			final PostgresDB postgres = getPostgresContainerDatabase();
			final MySQLDB mysql = MySQLTestDatabase.getFromSettings("mysql");
			final MSSQLServerDB sqlserver = MSSQLServerLocalTestDB.getFromSettings("sqlserver");
//			final MSSQLServer2017ContainerDB sqlserver = getMSSQLServerContainerDatabaseForCluster();
//			final Oracle11XEContainerDB oracle = getOracleContainerDatabaseForCluster();
			final Oracle11XEDB oracle = Oracle11XETestDB.getFromSettings("oraclexe");
			databases.add(new Object[]{"ClusteredDB-H2+SQLite+Postgres+MySQL+SQLServer+Oracle",
				new DBDatabaseCluster("testFullCluster", DBDatabaseCluster.Configuration.manual(), h2Mem, sqlite, postgres, mysql, sqlserver, oracle)});
		}
		if (System.getProperty("MySQL+Cluster") != null) {
			databases.add(new Object[]{"ClusteredDB-H2+SQLite+Postgres+MySQL",
				new DBDatabaseCluster("MySQL+Cluster", DBDatabaseCluster.Configuration.manual(),
				H2MemoryTestDB.getFromSettings("h2memory"),
				SQLiteTestDB.getFromSettings(),
				getPostgresContainerDatabaseForCluster(),
				getMySQLContainerDatabaseForCluster()
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
		if (System.getProperty("testMySQLContainer") != null) {
			databases.add(new Object[]{"MySQLDBContainer", getMySQLContainerDatabase()});
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
			//Quite convoluted creation but it's meant to test the file builder
			final DatabaseConnectionSettings settings = DatabaseConnectionSettings.getSettingsfromSystemUsingPrefix("h2file");
			databases.add(new Object[]{"H2FileDB", new H2FileDB(
				new H2FileSettingsBuilder()
				.setFilename(settings.getFilename())
				.setUsername(settings.getUsername())
				.setPassword(settings.getUsername())
				)
			}
			);
		}
		if (System.getProperty("testH2DataSourceDB") != null) {
			databases.add(new Object[]{"H2DataSourceDB", H2TestDatabase.getFromSettingsUsingDataSource("h2datasource")});
		}
		if (System.getProperty("testPostgresSQL") != null) {
			databases.add(new Object[]{"PostgresSQL", PostgreSQLTestDatabaseProvider.getFromSettings("postgres")});
		}
		if (System.getProperty("testPostgresContainer") != null) {
			databases.add(new Object[]{"PostgresContainer", getPostgresContainerDatabase()});
		}
		if (System.getProperty("testNuo") != null) {
			databases.add(new Object[]{"NuoDB", new NuoDB("localhost", 48004L, "dbv", "dbv", "dbv", "dbv")});
		}
		if (System.getProperty("testOracleXE") != null) {
			databases.add(new Object[]{"OracleXEDB", Oracle11XETestDB.getFromSettings("oraclexe")});
		}
		if (System.getProperty("testOracleXEContainer") != null) {
			databases.add(new Object[]{"Oracle11XEContainer", getOracleContainerDatabase()});
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
	}

	private static Postgres10ContainerDB POSTGRES_CONTAINER_DATABASE = null;

	private static Postgres10ContainerDB getPostgresContainerDatabase() {
		if (POSTGRES_CONTAINER_DATABASE == null) {
			POSTGRES_CONTAINER_DATABASE = Postgres10ContainerDB.getLabelledInstance("Postgres for Testing");
		}
		return POSTGRES_CONTAINER_DATABASE;
	}
	private static Postgres10ContainerDB POSTGRES_CONTAINER_DATABASE_FOR_CLUSTER = null;

	private static Postgres10ContainerDB getPostgresContainerDatabaseForCluster() {
		if (POSTGRES_CONTAINER_DATABASE_FOR_CLUSTER == null) {
			POSTGRES_CONTAINER_DATABASE_FOR_CLUSTER = Postgres10ContainerDB.getLabelledInstance("Postgres for Cluster");
		}
		return POSTGRES_CONTAINER_DATABASE_FOR_CLUSTER;
	}
	private static MySQL8ContainerDB MySQL_CONTAINER_DATABASE = null;
	private static MySQL8ContainerDB MySQL_CONTAINER_DATABASE_FOR_CLUSTER = null;

	private static MySQL8ContainerDB getMySQLContainerDatabase() {
		if (MySQL_CONTAINER_DATABASE == null) {
			MySQL_CONTAINER_DATABASE = MySQL8ContainerDB.getInstance();
		}
		MySQL_CONTAINER_DATABASE.setLabel("MySQL Test Database");
		return MySQL_CONTAINER_DATABASE;
	}

	private static MySQL8ContainerDB getMySQLContainerDatabaseForCluster() {
		if (MySQL_CONTAINER_DATABASE_FOR_CLUSTER == null) {
			MySQL_CONTAINER_DATABASE_FOR_CLUSTER = MySQL8ContainerDB.getInstance();
		}
		MySQL_CONTAINER_DATABASE_FOR_CLUSTER.setLabel("MySQL for Cluster");
		return MySQL_CONTAINER_DATABASE_FOR_CLUSTER;
	}

	private static MSSQLServer2017ContainerDB MSSQLSERVER_CONTAINER_DATABASE = null;
	private static MSSQLServer2017ContainerDB MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER = null;

	private static MSSQLServer2017ContainerDB getMSSQLServerContainerDatabase() {
		if (MSSQLSERVER_CONTAINER_DATABASE == null) {
			MSSQLSERVER_CONTAINER_DATABASE = MSSQLServer2017ContainerDB.getLabelledInstance("MSSQLServer Container DB");
		}
		return MSSQLSERVER_CONTAINER_DATABASE;
	}

	private static MSSQLServer2017ContainerDB getMSSQLServerContainerDatabaseForCluster() {
		while (MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER == null) {
			MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER = MSSQLServer2017ContainerDB.getLabelledInstance("MSSQLServer Container DB for Cluster Testing");
		}
		return MSSQLSERVER_CONTAINER_DATABASE_FOR_CLUSTER;
	}

	private static Oracle11XEContainerDB ORACLE_CONTAINER_DATABASE = null;

	private static Oracle11XEContainerDB getOracleContainerDatabase() {
		if (ORACLE_CONTAINER_DATABASE == null) {
			ORACLE_CONTAINER_DATABASE = Oracle11XEContainerDB.getInstance();
		}
		return ORACLE_CONTAINER_DATABASE;
	}

	private static Oracle11XEContainerDB ORACLE_CONTAINER_DATABASE_FOR_CLUSTER = null;

	private static Oracle11XEContainerDB getOracleContainerDatabaseForCluster() {
		if (ORACLE_CONTAINER_DATABASE_FOR_CLUSTER == null) {
			ORACLE_CONTAINER_DATABASE_FOR_CLUSTER = Oracle11XEContainerDB.getInstance();
		}
		return ORACLE_CONTAINER_DATABASE_FOR_CLUSTER;
	}

	public AbstractTest(Object testIterationName, Object db) {
		if (db instanceof DBDatabase) {
			this.database = (DBDatabase) db;
			database.setLabel("Actual Test Database (" + db.getClass().getSimpleName() + ")");
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

	protected String oracleSafeStrings(String expect) throws NoAvailableDatabaseException {
		return database.supportsDifferenceBetweenNullAndEmptyString() ? expect : expect == null ? "" : expect;
	}

	private static class H2TestDatabase {

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
				return getH2TestDatabaseFromFilename(file, username, password);
			} else {
				return new H2DB(url, username, password);
			}
		}

		public static DBDatabase getSharedDBFromSettings(String prefix)
				throws SQLException, IOException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			DatabaseConnectionSettings settings = DatabaseConnectionSettings.getSettingsfromSystemUsingPrefix(prefix);
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
				return getH2TestDatabaseFromFilename(file + "-cluster.h2db", username, password);
			} else {
				return new H2DB(url, username, password);
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

		public static H2DB getH2TestDatabaseFromFilename(String file, String username, String password) throws SQLException, IOException {
			return new H2DB(new File(file), username, password);
		}
	}

	private static class MySQL56TestDatabase {

		public static final long serialVersionUID = 1l;

		public static MySQLDB getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return new MySQLDB(url, username, password);
		}

//		public MySQL56TestDatabase(String url, String username, String password) throws SQLException {
//			super(url, username, password);
//		}
	}

	private static class MySQLTestDatabase {

		public static final long serialVersionUID = 1l;

		public static MySQLDB getFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database");
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema");
			return new MySQLDB(
					new MySQLSettingsBuilder()
							.setHost(host)
							.setPort(Integer.parseInt(port))
							.setDatabaseName(database)
							.setUsername(username)
							.setPassword(password)
							.setSchema(schema)
			);
		}

		public static MySQLDB getClusterDBFromSettings(String prefix) throws SQLException {
			String url = System.getProperty("" + prefix + ".url");
			String host = System.getProperty("" + prefix + ".host");
			String port = System.getProperty("" + prefix + ".port");
			String instance = System.getProperty("" + prefix + ".instance");
			String database = System.getProperty("" + prefix + ".database") + "_cluster";
			String username = System.getProperty("" + prefix + ".username");
			String password = System.getProperty("" + prefix + ".password");
			String schema = System.getProperty("" + prefix + ".schema") + "cluster";
			return new MySQLDB(
					new MySQLSettingsBuilder()
							.setHost(host)
							.setPort(Integer.parseInt(port))
							.setDatabaseName(database)
							.setUsername(username)
							.setPassword(password)
							.setSchema(schema)
			);
		}

//		public MySQLTestDatabase(String host, String port, String database, String username, String password, String schema) throws SQLException {
//			super(host, Integer.valueOf(port), database, username, password);
//		}
//
//		public MySQLTestDatabase(String url, String username, String password) throws SQLException {
//			super(url, username, password);
//		}
	}

	private static class PostgreSQLTestDatabaseProvider {

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
			return PostgreSQLTestDatabaseProvider.getTestDatabase(url, host, port, database, username, password, schema);
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
			return PostgreSQLTestDatabaseProvider.getTestDatabase(url, host, port, database, username, password, schema);
		}

		protected static PostgresDB getTestDatabase(String url, String host, String port, String database, String username, String password, String schema) throws SQLException {
			return new PostgresDB(
					new PostgresSettingsBuilder()
							.setHost(host)
							.setPort(Integer.valueOf(port))
							.setDatabaseName(database)
							.setUsername(username)
							.setPassword(password)
			);
		}
	}

	private static class SQLiteTestDB extends SQLiteDB {

		private final static long serialVersionUID = 1l;

		public static SQLiteTestDB getFromSettings() throws IOException, SQLException {
			return getFromSettings("sqlite");
		}

		public static SQLiteTestDB getFromSettings(String prefix) throws IOException, SQLException {
			SQLiteSettingsBuilder builder = new SQLiteSettingsBuilder().fromSystemUsingPrefix(prefix);
			return new SQLiteTestDB(builder);
		}

		public static SQLiteTestDB getClusterDBFromSettings(String prefix, String name) throws IOException, SQLException {
			SQLiteSettingsBuilder builder = new SQLiteSettingsBuilder().fromSystemUsingPrefix(prefix);
			builder.setFilename(builder.getFilename() + "-" + name + "cluster.sqlite");
			return new SQLiteTestDB(builder);

		}

		public SQLiteTestDB(SQLiteSettingsBuilder builder) throws IOException, SQLException {
			super(builder);
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
			super(
					new Oracle11XESettingsBuilder()
							.setHost(host)
							.setPort(Integer.parseInt(port))
							.setInstance(instance)
							.setUsername(username)
							.setPassword(password)
			);
		}
	}

	private static class MSSQLServerLocalTestDB extends MSSQLServerDB {

		private final static long serialVersionUID = 1l;

		public static MSSQLServerLocalTestDB getFromSettings(String prefix) throws SQLException {
			MSSQLServerSettingsBuilder builder = new MSSQLServerSettingsBuilder().fromSystemUsingPrefix(prefix);
			return new MSSQLServerLocalTestDB(builder);
		}

		private MSSQLServerLocalTestDB(MSSQLServerSettingsBuilder builder) throws SQLException {
			super(builder);
		}
	}

	private static class H2MemoryTestDB extends H2MemoryDB {

		public static final long serialVersionUID = 1l;

		public static H2MemoryTestDB getFromSettings(String prefix) throws SQLException {
//			DatabaseConnectionSettings settings = DatabaseConnectionSettings.getSettingsfromSystemUsingPrefix(prefix);
			H2MemorySettingsBuilder builder = new H2MemorySettingsBuilder().fromSystemUsingPrefix(prefix);
			return new H2MemoryTestDB(builder);
		}

		public static H2MemoryTestDB getClusterDBFromSettings(String prefix) throws SQLException {
//			DatabaseConnectionSettings settings = DatabaseConnectionSettings.getSettingsfromSystemUsingPrefix(prefix);
			H2MemorySettingsBuilder builder = new H2MemorySettingsBuilder().fromSystemUsingPrefix(prefix);
			String file = builder.getDatabaseName();
			if (file != null && !file.equals("")) {
				builder.setDatabaseName(file + "-cluster.h2db");
			} else {
				builder.setDatabaseName("cluster.h2db");
			}
			return new H2MemoryTestDB(builder);
		}

		public static H2MemoryTestDB blankDB() throws SQLException {
			return new H2MemoryTestDB();
		}

		public H2MemoryTestDB() throws SQLException {
			this(
					new H2MemorySettingsBuilder()
							.setDatabaseName("Blank")
			);
		}

		public H2MemoryTestDB(H2MemorySettingsBuilder builder) throws SQLException {
			super(builder);
		}
	}
}
