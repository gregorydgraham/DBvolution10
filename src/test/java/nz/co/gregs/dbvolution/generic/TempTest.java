/*
 * Copyright 2023 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.actions.NoOpDBAction;
import nz.co.gregs.dbvolution.databases.*;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.PROCESSING;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.READY;
import nz.co.gregs.dbvolution.example.*;
import static nz.co.gregs.dbvolution.generic.AbstractTest.*;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.internal.database.ClusterMemberList;
import nz.co.gregs.looper.StopWatch;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.*;

/**
 *
 * @author gregorygraham
 */
public class TempTest {

	DBDatabase database;

	public TempTest() {
		try {
			this.database = H2MemoryDB.createDatabase("Member 1");
		} catch (SQLException ex) {
			Logger.getLogger(TempTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() throws Exception {
		try {
			if (database != null) {
				if (database instanceof DBDatabaseCluster) {
					try {
						DBDatabaseCluster cluster = (DBDatabaseCluster) database;
						cluster.reconnectQuarantinedDatabases();
						cluster.waitUntilSynchronised();
					} catch (Exception ex) {
						ex.printStackTrace();
						throw ex;
					}
				}
				database.preventDroppingOfTables(false);
				final Marque myMarqueRow = new Marque();
				database.dropTableIfExists(myMarqueRow);
				database.createTable(myMarqueRow);

				database.preventDroppingOfTables(false);
				final CarCompany myCarCompanyRow = new CarCompany();
				database.dropTableNoExceptions(myCarCompanyRow);
				database.createTable(myCarCompanyRow);

				DBTable<Marque> marquesTable = DBTable.getInstance(database, myMarqueRow);
				DBTable<CarCompany> carCompanies = DBTable.getInstance(database, myCarCompanyRow);
				carCompanies.insert(new CarCompany("TOYOTA", 1));

				var carTableRows = new ArrayList<CarCompany>(3);
				carTableRows.add(new CarCompany("Ford", 2));
				carTableRows.add(new CarCompany("GENERAL MOTORS", 3));
				carTableRows.add(new CarCompany("OTHER", 4));
				carCompanies.insert(carTableRows);

				Date firstDate = DATETIME_FORMAT.parse(firstDateStr);
				Date secondDate = DATETIME_FORMAT.parse(secondDateStr);

				var marqueRows = new ArrayList<Marque>(50);
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
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

	@After
	public void tearDown() {
	}

	@Test
	public synchronized void testTest() throws Exception {
	}

	@Test
	public void testWaitUntilDatabaseHasSynchronized() throws SQLException {
		ClusterMemberList databaseList = new ClusterMemberList(new ClusterDetails("temptest"));
		database.setLabel("member1-testWaitUntilDatabaseHasSynchronized");
		H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		db2.setLabel("member2-testWaitUntilDatabaseHasSynchronized");
		H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		db3.setLabel("member3-testWaitUntilDatabaseHasSynchronized");
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		System.out.println("STATUS: " + database.getLabel() + "-" + databaseList.getMember(database).getStatus());
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		System.out.println("STATUS: " + database.getLabel() + "-" + databaseList.getMember(database).getStatus());
		StopWatch timer = StopWatch.stopwatch();
		timer.time(() -> databaseList.waitUntilDatabaseHasSynchronized(database, 10));
		System.out.println("STATUS: " + database.getLabel() + "-" + databaseList.getMember(database).getStatus());
		assertThat(timer.duration(), is(greaterThanOrEqualTo(10l))); // make sure it took some time
		assertThat(timer.duration(), is(lessThanOrEqualTo(100l))); // make sure it didn't take TOO MUCH time
		databaseList.getMembers().stream().forEach((m)->System.out.println("STATUS: "+m.getDatabase().getLabel()+" - "+m.getStatus()));
		assertThat(databaseList.getStatusOf(database), is(PROCESSING));// make sure that it DID NOT synchronise
		
		timer.time(() -> databaseList.waitUntilDatabaseHasSynchronized(database, 1000));
		assertThat(databaseList.getStatusOf(database), is(READY));// make sure that it DID actually synchronise
	}

	@Test
	public synchronized void testAutoConnectClusterLoadsAndConnectsToDatabases() throws SQLException {
		final String nameOfCluster = "testAutoConnectClusterLoadsAndConnectsToDatabases";
		String soloDB2Settings;
		final Function<DBDatabase, String> turnDatabasesToSettings = d -> d.getSettings().toString();
		{
			DBDatabaseCluster cluster
					= new DBDatabaseCluster(
							nameOfCluster,
							DBDatabaseCluster.Configuration.fullyManual().withAutoConnect().withAutoStart(),
							database);
			cluster.dismantle();
		}
		{
			DBDatabaseCluster cluster
					= new DBDatabaseCluster(
							nameOfCluster,
							DBDatabaseCluster.Configuration.fullyManual().withAutoConnect().withAutoStart(),
							database);
			cluster.waitUntilSynchronised();
			H2MemoryDB soloDB2 = H2MemoryDB.createDatabase("soloDB2");
			soloDB2.setLabel("Member 2-testAutoConnectClusterLoadsAndConnectsToDatabases");
			soloDB2Settings = soloDB2.getSettings().toString();
			boolean synchronised = cluster.addDatabaseAndWait(soloDB2);
			assertThat(synchronised, is(true));
			cluster.getClusterStatusSnapshot().print();
			cluster.waitUntilSynchronised(100);
			cluster.getClusterStatusSnapshot().print();
			assertThat(cluster.size(), is(2));
			cluster.stop();
		}
		{
			DBDatabaseCluster cluster = new DBDatabaseCluster(
					nameOfCluster, DBDatabaseCluster.Configuration.fullyManual().withAutoConnect().withAutoStart()
			);
			System.out.println("" + cluster.getClusterStatus());
			cluster.waitUntilSynchronised();
			assertThat(cluster.size(), is(2));
			final DBDatabase[] dbsInCluster = cluster.getDatabases();
			List<String> databases = new ArrayList<>();
			for (DBDatabase db : dbsInCluster) {
				final DatabaseConnectionSettings settings = db.getSettings();
				final String str = settings.toString();
				databases.add(str);
			}
			assertThat(databases, hasItem(soloDB2Settings));
			assertThat(databases, hasItem(is(database.getSettings().toString())));

			cluster.addDatabase(H2MemoryDB.createDatabase("Check Added Database Is Recreated At Startup"));
			cluster.stop();
		}
		{
			DBDatabaseCluster cluster = new DBDatabaseCluster(
					nameOfCluster,
					DBDatabaseCluster.Configuration
							.fullyManual()
							.withAutoConnect()
							.withAutoStart()
			);
			cluster.waitUntilSynchronised();

			assertThat(cluster.size(), is(3));
			var databases = Arrays.asList(cluster.getDatabases()).stream().map(turnDatabasesToSettings).collect(Collectors.toList());
			assertThat(databases, hasItem(soloDB2Settings));
			assertThat(databases, hasItem(database.getSettings().toString()));

			cluster.removeDatabase(database);
			assertThat(cluster.size(), is(2));
		}
		{
			DBDatabaseCluster cluster = new DBDatabaseCluster(
					nameOfCluster, DBDatabaseCluster.Configuration.fullyManual().withAutoConnect().withAutoStart()
			);
			cluster.waitUntilSynchronised();

			try {
				assertThat(cluster.size(), is(2));
				var databases = Arrays.asList(cluster.getDatabases()).stream().map(turnDatabasesToSettings).collect(Collectors.toList());
				assertThat(databases, hasItem(soloDB2Settings));
				assertThat(databases, not(hasItem(database.getSettings().toString())));
			} finally {
				cluster.dismantle();
			}
		}
	}
}
