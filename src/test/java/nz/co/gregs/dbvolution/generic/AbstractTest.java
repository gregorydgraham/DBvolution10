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

import nz.co.gregs.dbvolution.DBDatabase;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import net.sourceforge.tedhi.FlexibleDateFormat;
import net.sourceforge.tedhi.FlexibleDateRangeFormat;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.mysql.MySQLMXJDBInitialisation;
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
	Marque myMarqueRow = new Marque();
	CarCompany myCarCompanyRow = new CarCompany();
	public DBTable<Marque> marquesTable;
	DBTable<CarCompany> carCompanies;
	public List<Marque> marqueRows = new ArrayList<Marque>();
	public List<CarCompany> carTableRows = new ArrayList<CarCompany>();
	public static final FlexibleDateFormat tedhiFormat = FlexibleDateFormat.getPatternInstance("dd/M/yyyy h:m:s", Locale.UK);
	public static final SimpleDateFormat datetimeFormat = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss", Locale.UK);
	public static final FlexibleDateRangeFormat tedhiRangeFormat = FlexibleDateRangeFormat.getPatternInstance("M yyyy", Locale.UK);
	public String firstDateStr = "23/March/2013 12:34:56";
	public String secondDateStr = "2/April/2012 1:02:03";

	@Parameters(name = "{0}")
	public static List<Object[]> data() throws IOException, SQLException, ClassNotFoundException {

		List<Object[]> databases = new ArrayList<Object[]>();
		final SQLiteDB sqliteDB = new SQLiteDB("jdbc:sqlite:dbvolutionTest.sqlite", "dbv", "dbv");
		final H2MemoryDB h2MemoryDB = new H2MemoryDB("dbvolutionTest", "", "", false);

		if (System.getProperty("testSQLite") != null) {
			databases.add(new Object[]{"SQLiteDB", sqliteDB});
		}
		if (System.getProperty("testMySQLMXJDB") != null) {
			databases.add(new Object[]{"SQLMXJDB", MySQLMXJDBInitialisation.getMySQLDBInstance()});
		}
		if (System.getProperty("testMySQL") != null) {
			databases.add(new Object[]{"MySQLDB", new MySQLDB("jdbc:mysql://localhost:3306/test?createDatabaseIfNotExist=true", "dbv", "dbv")});
		}
		if (System.getProperty("testH2DB") != null) {
			databases.add(new Object[]{"H2DB", new H2DB("jdbc:h2:~/dbvolutionTest", "", "")});
		}
		if (System.getProperty("testH2DataSourceDB") != null) {
			JdbcDataSource h2DataSource = new JdbcDataSource();
			h2DataSource.setUser("");
			h2DataSource.setPassword("");
			h2DataSource.setURL("jdbc:h2:~/dbvolutionTest");
			databases.add(new Object[]{"H2DataSourceDB", new H2DB(h2DataSource)});
		}
		if (System.getProperty("testPostgresSQL") != null) {
			databases.add(new Object[]{"PostgresSQL", new PostgresDB("localhost", "5432", "dbvtest", "dbv", "dbv", "")});
		}
		if (System.getProperty("testNuo") != null) {
			databases.add(new Object[]{"NuoDB", new NuoDB(Arrays.asList(new String[]{"localhost"}), Arrays.asList(new Long[]{48004L}), "dbv", "dbv", "dbv", "dbv")});
		}
		if (System.getProperty("testOracle") != null) {
			databases.add(new Object[]{"Oracle11DB", new Oracle11DB("localhost", 1521, "XE", "dbv", "dbv")});
		}
		if (System.getProperty("testJavaDBMemory") != null) {
			databases.add(new Object[]{"JavaDBMemory", new JavaDBMemoryDB("localhost", 1527, "dbv", "dbv", "dbv")});
		}
		if (System.getProperty("testJavaDB") != null) {
			databases.add(new Object[]{"JavaDB", new JavaDB("localhost", 1527, "dbv", "dbv", "dbv")});
		}
		if (databases.isEmpty() || System.getProperty("testH2MemoryDB") != null) {
			// Do basic testing
			databases.add(new Object[]{"H2MemoryDB", h2MemoryDB});
		}

		return databases;
	}

	public AbstractTest(Object testIterationName, Object db) {
		if (db instanceof DBDatabase) {
			this.database = (DBDatabase) db;
			database.setPrintSQLBeforeExecuting(true);
		}
	}

	public String testableSQL(String str) {
		if (str != null) {
			String trimStr = str.trim().replaceAll("[ \\r\\n]+", " ").toLowerCase();
			if ((database instanceof OracleDB) || (database instanceof JavaDB)) {
				return trimStr.replaceAll(" oo", " __").replaceAll(" +[aA][sS] +", " ").replaceAll("\"", "").replaceAll(" *; *$", "");
			} else if (database instanceof PostgresDB) {
				return trimStr.replaceAll("::[a-zA-Z]*", "");
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
						.replaceAll(" oo", " __")
						.replaceAll("\"", "")
						.replaceAll(" *; *$", "")
						.replaceAll(" as ", " ");
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

		Date firstDate = datetimeFormat.parse(firstDateStr);
		Date secondDate = datetimeFormat.parse(secondDateStr);

		marqueRows.add(new Marque(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, null));
		marqueRows.add(new Marque(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", firstDate, 2, null));
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
		marqueRows.add(new Marque(8376505, "False", 1246974, "", 0, "", "ISUZU", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(8587147, "False", 1246974, "", 0, "", "DAEWOO", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", firstDate, 4, null));
		marqueRows.add(new Marque(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", secondDate, 4, null));
		marqueRows.add(new Marque(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", secondDate, 4, null));
		marqueRows.add(new Marque(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", firstDate, 1, null));
		marqueRows.add(new Marque(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", secondDate, 3, null));

		marquesTable.insert(marqueRows);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new CompanyLogo());
		database.createTable(new CompanyLogo());

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new LinkCarCompanyAndLogo());
		database.createTable(new LinkCarCompanyAndLogo());

		database.setPrintSQLBeforeExecuting(true);
	}

	@After
	public void tearDown() throws Exception {
		tearDown(database);
	}

	public void tearDown(DBDatabase database) throws Exception {
		database.setPrintSQLBeforeExecuting(false);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new LinkCarCompanyAndLogo());
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new CompanyLogo());
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(myMarqueRow);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(myCarCompanyRow);
		try {
			database.preventDroppingOfTables(false);
			database.preventDroppingOfDatabases(false);
			database.dropDatabase(true);
		} catch (UnsupportedOperationException unsupported) {
			;
		}
	}
}
