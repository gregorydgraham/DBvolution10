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
import nz.co.gregs.dbvolution.databases.Oracle11XEContainerDB;
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
	public static List<Object[]> data() throws IOException, SQLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, Exception {

		if (databases.isEmpty()) {
			getDatabasesFromSettings();
			databases.forEach(database -> {
				System.out.println("Processing: Database " + database[0] + " = " + ((DBDatabase) database[1]).getJdbcURL());
			});
		}
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

	protected synchronized static void getDatabasesFromSettings() throws InvocationTargetException, IllegalArgumentException, IOException, InstantiationException, SQLException, IllegalAccessException, ClassNotFoundException, SecurityException, NoSuchMethodException, Exception {
		if (System.getProperty("testSmallCluster") != null) {
			final DBDatabaseCluster cluster
					= new DBDatabaseCluster(
							"testSmallCluster",
							DBDatabaseCluster.Configuration.autoStart(),
							getSQLiteDBFromSystem(),
							new H2MemorySettingsBuilder().fromSystemUsingPrefix("h2memory").getDBDatabase()
					);
			cluster.setLabel("ClusteredDB-H2+SQLite");
			cluster.waitUntilSynchronised();
			databases.add(new Object[]{cluster.getLabel(), cluster});
		}
		if (System.getProperty("testBundledCluster") != null) {
			final DBDatabaseCluster cluster = new DBDatabaseCluster("testBundledCluster", DBDatabaseCluster.Configuration.autoStart(),
					getSQLiteDBFromSystem("bundle"),
					H2MemoryTestDB.getFromSettings("h2memory")
			);
			cluster.setLabel("ClusteredDB-H2+SQLite");
			cluster.waitUntilSynchronised();
			databases.add(new Object[]{cluster.getLabel(), cluster});
		}
		if (System.getProperty("testOpenSourceCluster") != null) {
			final DBDatabaseCluster cluster = new DBDatabaseCluster("testOpenSourceCluster", DBDatabaseCluster.Configuration.autoStart(),
					H2MemoryTestDB.getFromSettings("h2memory"),
					getSQLiteDBFromSystem("open"),
					new PostgresSettingsBuilder().fromSystemUsingPrefix("postgresfullcluster").getDBDatabase(),
					new MySQLSettingsBuilder().fromSystemUsingPrefix("mysql").getDBDatabase()
			);
			cluster.setLabel("ClusteredDB-H2+SQLite+Postgres+MySQL");
			cluster.waitUntilSynchronised();
			databases.add(new Object[]{cluster.getLabel(), cluster});
		}
		if (System.getProperty("testFullCluster") != null) {
			final H2MemoryDB h2Mem = H2MemoryTestDB.getFromSettings("h2memory");
			final SQLiteDB sqlite = getSQLiteDBFromSystem("full");
			final PostgresDB postgres = new PostgresSettingsBuilder().fromSystemUsingPrefix("postgresfullcluster").getDBDatabase();
			final MySQLDB mysql = new MySQLSettingsBuilder().fromSystemUsingPrefix("mysql").getDBDatabase();
			final MSSQLServerDB sqlserver = MSSQLServerLocalTestDB.getFromSettings("sqlserver");
			final Oracle11XEDB oracle = new Oracle11XESettingsBuilder().fromSystemUsingPrefix("oraclexe").getDBDatabase();
			final DBDatabaseCluster cluster = new DBDatabaseCluster("testFullCluster", DBDatabaseCluster.Configuration.autoStart(), h2Mem, sqlite,
					postgres, mysql, sqlserver,
					oracle);
			cluster.setLabel("ClusteredDB-H2+SQLite+Postgres+MySQL+SQLServer+Oracle");
			cluster.waitUntilSynchronised();
			databases.add(new Object[]{cluster.getLabel(), cluster});
		}
		if (System.getProperty("MySQL+Cluster") != null) {
			databases.add(
					new Object[]{"ClusteredDB-H2+SQLite+Postgres+MySQL",
						new DBDatabaseCluster("MySQL+Cluster",
								DBDatabaseCluster.Configuration.autoStart(),
								H2MemoryTestDB.getFromSettings("h2memory"),
								getSQLiteDBFromSystem(),
								new PostgresSettingsBuilder().fromSystemUsingPrefix("postgresfullcluster").getDBDatabase(),
								new MySQLSettingsBuilder().fromSystemUsingPrefix("mysqlcluster").getDBDatabase()
						)});
			final MySQLDB dbDatabase = new MySQLSettingsBuilder().fromSystemUsingPrefix("mysql").getDBDatabase();
			databases.add(new Object[]{"MySQL", dbDatabase});
		}
		if (System.getProperty("testSQLite") != null) {
			databases.add(new Object[]{"SQLiteDB", getSQLiteDBFromSystem()});
		}
		if (System.getProperty("testMySQL") != null) {
			databases.add(new Object[]{"MySQLDB", new MySQLSettingsBuilder().fromSystemUsingPrefix("mysql").getDBDatabase()});
		}
		if (System.getProperty("testMySQLContainer") != null) {
			databases.add(new Object[]{"MySQLDBContainer",
				new MySQLSettingsBuilder().fromSystemUsingPrefix("mysqlcontainer").getDBDatabase()
			});
		}
		if (System.getProperty("testMySQL56") != null) {
			databases.add(new Object[]{"MySQLDB-5.6", new MySQLSettingsBuilder().fromSystemUsingPrefix("mysql56").getDBDatabase()});
		}
		if (System.getProperty("testH2DB") != null) {
			databases.add(new Object[]{"H2DB", new H2SettingsBuilder().fromSystemUsingPrefix("h2").getDBDatabase()});
		}
		if (System.getProperty("testH2SharedDB") != null) {
			databases.add(new Object[]{"H2SharedDB", new H2SharedSettingsBuilder().fromSystemUsingPrefix("h2shared").getDBDatabase()});
		}
		if (System.getProperty("testH2FileDB") != null) {
			//Quite convoluted creation but it's meant to test the file builder
			final DatabaseConnectionSettings settings = DatabaseConnectionSettings.getSettingsfromSystemUsingPrefix("h2file");
			final H2FileDB h2FileDB = new H2FileDB(
					new H2FileSettingsBuilder()
							.setFilename(settings.getFilename())
							.setUsername(settings.getUsername())
							.setPassword(settings.getUsername())
			);
			h2FileDB.setLabel("H2FileDB");
			databases.add(new Object[]{h2FileDB.getLabel(), h2FileDB}
			);
		}
		if (System.getProperty("testH2DataSourceDB") != null) {
			databases.add(new Object[]{"H2DataSourceDB", getH2SharedDatabase()});
		}
		if (System.getProperty("testPostgresSQL") != null) {
			databases.add(new Object[]{"PostgresSQL", new PostgresSettingsBuilder().fromSystemUsingPrefix("postgres").getDBDatabase()});
		}
		if (System.getProperty("testPostgresContainer") != null) {
			databases.add(new Object[]{
				"PostgresContainer",
				new PostgresSettingsBuilder().fromSystemUsingPrefix("postgrescontainer").setLabel("PostgresContainer").getDBDatabase()
			});
		}
		if (System.getProperty("testNuo") != null) {
			databases.add(new Object[]{"NuoDB", new NuoDB("localhost", 48004L, "dbv", "dbv", "dbv", "dbv")});
		}
		if (System.getProperty("testOracleXE") != null) {
			databases.add(new Object[]{"OracleXEDB", new Oracle11XESettingsBuilder().fromSystemUsingPrefix("oraclexe").getDBDatabase()});
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

	private static SQLiteDB getSQLiteDBFromSystem(String clusterName) throws Exception {
		final SQLiteSettingsBuilder sqliteBuilder = new SQLiteSettingsBuilder().fromSystemUsingPrefix("sqlite");
		sqliteBuilder.setFilename(sqliteBuilder.getFilename() + "-" + clusterName + "cluster.sqlite");
		return sqliteBuilder.getDBDatabase();
	}

	private static SQLiteDB getSQLiteDBFromSystem() throws Exception {
		final SQLiteSettingsBuilder sqliteBuilder = new SQLiteSettingsBuilder().fromSystemUsingPrefix("sqlite");
		return sqliteBuilder.getDBDatabase();
	}

	protected static H2DB getH2SharedDatabase() throws SQLException {
		final String prefix = "h2datasource";
		String url = System.getProperty(prefix + ".url");
		String username = System.getProperty(prefix + ".username");
		String password = System.getProperty(prefix + ".password");
		JdbcDataSource h2DataSource = new JdbcDataSource();
		h2DataSource.setUser(username);
		h2DataSource.setPassword(password);
		h2DataSource.setURL(url);
		final H2DB h2DB = new H2DB(h2DataSource);
		return h2DB;
	}

	private static MSSQLServer2017ContainerDB MSSQLSERVER_CONTAINER_DATABASE = null;

	private static MSSQLServer2017ContainerDB getMSSQLServerContainerDatabase() {
		if (MSSQLSERVER_CONTAINER_DATABASE == null) {
			MSSQLSERVER_CONTAINER_DATABASE = MSSQLServer2017ContainerDB.getLabelledInstance("MSSQLServer Container DB");
		}
		return MSSQLSERVER_CONTAINER_DATABASE;
	}

	private static Oracle11XEContainerDB ORACLE_CONTAINER_DATABASE = null;

	private static Oracle11XEContainerDB getOracleContainerDatabase() {
		if (ORACLE_CONTAINER_DATABASE == null) {
			ORACLE_CONTAINER_DATABASE = Oracle11XEContainerDB.getInstance();
		}
		return ORACLE_CONTAINER_DATABASE;
	}

	public AbstractTest(Object testIterationName, Object db) {
		if (db instanceof DBDatabase) {
			this.database = (DBDatabase) db;
		}
	}

	public String testableSQL(String str) {
		if (str != null) {
			String trimStr = str.trim().replaceAll("[ \\r\\n]+", " ").toLowerCase().replaceAll(", ", ",");
			if ((database instanceof OracleDB) || (database instanceof JavaDB)) {
				return trimStr
						.replaceAll("\"", "")
						.replaceAll(" oo", " ")
						.replaceAll("\\b_+", "")
						.replaceAll(" +[aA][sS] +", " ")
						.replaceAll(" *; *$", "");
			} else if (database instanceof H2DB) {
				return trimStr
						.replaceAll("\"", "");
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
					.replaceAll(" [dD][bB][_0-9]+", "")
					.replaceAll("[ \\r\\n]+", " ")
					.replaceAll(", ", ",")
					.toLowerCase();
			if ((database instanceof OracleDB)
					|| (database instanceof JavaDB)) {
				return trimStr
						.replaceAll("\"", "")
						.replaceAll("\\boo", "__")
						.replaceAll("\\b_+", "")
						.replaceAll(" *; *$", "")
						.replaceAll(" as ", " ");
			} else if (database instanceof H2DB) {
				return trimStr
						.replaceAll("\"", "");
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
		if (database != null) {
			if (database instanceof DBDatabaseCluster) {
				DBDatabaseCluster cluster = (DBDatabaseCluster) database;
				cluster.reconnectQuarantinedDatabases();
				cluster.waitUntilSynchronised();
			}
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

	private static class MSSQLServerLocalTestDB {

		private final static long serialVersionUID = 1l;

		public static MSSQLServerDB getFromSettings(String prefix) throws SQLException {
			MSSQLServerSettingsBuilder builder = new MSSQLServerSettingsBuilder().fromSystemUsingPrefix(prefix);
			return new MSSQLServerDB(builder);
		}

		private MSSQLServerLocalTestDB() {
		}

	}

	private static class H2MemoryTestDB {

		public static final long serialVersionUID = 1l;

		public static H2MemoryDB getFromSettings(String prefix) throws SQLException {
			H2MemorySettingsBuilder builder = new H2MemorySettingsBuilder().fromSystemUsingPrefix(prefix);
			return new H2MemoryDB(builder);
		}

		public static H2MemoryDB getClusterDBFromSettings(String prefix) throws SQLException {
			H2MemorySettingsBuilder builder = new H2MemorySettingsBuilder().fromSystemUsingPrefix(prefix);
			String file = builder.getDatabaseName();
			if (file != null && !file.equals("")) {
				builder.setDatabaseName(file + "-cluster.h2db");
			} else {
				builder.setDatabaseName("cluster.h2db");
			}
			return new H2MemoryDB(builder);
		}

		public static H2MemoryDB blankDB() throws SQLException {
			return new H2MemoryDB();
		}

		private H2MemoryTestDB() {
		}
	}
}
