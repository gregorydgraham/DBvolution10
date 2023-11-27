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
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.actions.BrokenAction;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.regexi.Match;
import nz.co.gregs.regexi.Regex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import org.junit.*;
import static org.junit.Assert.assertTrue;

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
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testDatabaseRemainsInClusterAfterActionFailsOnAllDatabases() throws SQLException {
		try {
			var DB1 = new BrokenDatabase(database);
			try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(DB1)) {
				H2MemorySettingsBuilder secondBuilder = new H2MemorySettingsBuilder().setLabel("Member 2").setDatabaseName("Member2");
				H2MemoryDB newMember = secondBuilder.getDBDatabase();
				var DB2 = new BrokenDatabase(newMember);
				cluster.setPrintSQLBeforeExecuting(true);
				assertThat(cluster.addDatabaseAndWait(DB2), is(true));
				assertThat(cluster.size(), is(2));
				try {
					System.out.println("\nTESTING SQL EXCEPTION THROWING...\n");
					DB1.useBrokenBehaviour = true;
					DB2.useBrokenBehaviour = true;
					cluster.createTable(new DBDatabaseClusterTest.TableThatDoesExistOnTheCluster());
					assertThat("we got here", is("We should never get here"));
				} catch (SQLException | AutoCommitActionDuringTransactionException e) {
				}
				assertThat(cluster.size(), is(2));
			}
		} catch (SQLException ex) {
			Logger.getLogger(TempTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Test
	public void testDatabaseRemovedFromClusterAfterActionFails() throws SQLException {
		Regex allActiveRegex = Regex
				.multiline()
				.literal("READY Databases: 2 of 2").newline()
				.anyCharacter().oneOrMore().literal("QUARANTINED Databases: 0 of 2").endRegex();
		Regex only1Active = Regex
				.multiline()
				.namedCapture("ready").literal("READY Databases: 1 of 2").endNamedCapture()
				.anyCharacter().oneOrMore().namedCapture("other").word().literal(" Databases: 1 of 2").endNamedCapture().endRegex();

		try {
			try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
				H2MemorySettingsBuilder dbBuilder = new H2MemorySettingsBuilder().setLabel("Member 2").setDatabaseName("Member2");
				var member2 = new BrokenDatabase(dbBuilder.getDBDatabase());
				cluster.setPrintSQLBeforeExecuting(true);
				assertThat(cluster.addDatabaseAndWait(member2), is(true));
				assertThat(cluster.size(), is(2));
				String clusterStatus = cluster.getClusterStatus();
				System.out.println(clusterStatus);
				assertTrue(allActiveRegex.matchesWithinString(clusterStatus));

				try {
					System.out.println("\nTESTING SQL EXCEPTION THROWING...\n");
					member2.useBrokenBehaviour = true;
					cluster.createTable(new DBDatabaseClusterTest.TableThatDoesExistOnTheCluster());
					cluster.getDetails().waitOnStatusChange(DBDatabaseCluster.Status.QUARANTINED, 1000, member2);
				} catch (SQLException | AutoCommitActionDuringTransactionException e) {
					assertThat("we got here", is("We should never get here"));
				}
				assertThat(cluster.size(), is(1));

				clusterStatus = cluster.getClusterStatus();
				System.out.println(clusterStatus);
				for (Match match : only1Active.getAllMatches(clusterStatus)) {
					System.out.println("MATCH: " + match.getEntireMatch());
				}
				System.out.println(only1Active.getAllNamedCapturesOfFirstMatchWithinString(clusterStatus).get("ready"));
				System.out.println(only1Active.getAllNamedCapturesOfFirstMatchWithinString(clusterStatus).get("other"));
				only1Active.testAgainst(clusterStatus);
				assertTrue(only1Active.matchesWithinString(clusterStatus));
				assertTrue(only1Active.matchesWithinString(clusterStatus));
			}
		} catch (SQLException ex) {
			Logger.getLogger(TempTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
@Test
	public synchronized void testAutomaticDataUpdating() throws SQLException, InterruptedException, UnexpectedNumberOfRowsException {
		final DBDatabaseClusterTest.DBDatabaseClusterTestTable2 testTable = new DBDatabaseClusterTest.DBDatabaseClusterTestTable2();

		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			// check that the table was automatically created, as all required tables should be
			Assert.assertTrue(cluster.tableExists(testTable));
			final DBTable<DBDatabaseClusterTest.DBDatabaseClusterTestTable2> query
					= cluster
							.getDBTable(testTable)
							.setBlankQueryAllowed(true);
			final List<DBDatabaseClusterTest.DBDatabaseClusterTestTable2> allRows = query.getAllRows();

			// clear any old data, there shouldn't be any but you never know
			cluster.delete(allRows);
			// check that the data is NOT in the cluster
			assertThat(cluster.getDBTable(testTable).count(), is(0l));
			// check that the data is NOT in the actual database
			assertThat(database.getDBTable(testTable).count(), is(0l));

			// make some data
			Date firstDate = new Date();
			Date secondDate = new Date();
			List<DBDatabaseClusterTest.DBDatabaseClusterTestTable2> data = createData2(firstDate, secondDate);
			// insert the data we going to use
			cluster.insert(data);
			// check that the data is in the cluster
			assertThat(cluster.getDBTable(testTable).count(), is(22l));
			// check that the data is in the actual database
			assertThat(database.getDBTable(testTable).count(), is(22l));

			// create a completely new database
			try (H2MemoryDB soloDB = H2MemoryDB.createANewRandomDatabase()) {
				soloDB.setLabel("MEMBER 2");
				// check that it automatically gets the required table
				Assert.assertTrue(soloDB.tableExists(testTable));
				// check that it DOES NOT have the data
				assertThat(soloDB.getDBTable(testTable).count(), is(0l));

				// add it to the database and give it a chance to catch up
				cluster.addDatabase(soloDB);
				cluster.waitUntilDatabaseIsSynchronised(soloDB);
//				cluster.waitUntilSynchronised(5000);
				System.out.println("STATUSES: \n"+cluster.getClusterStatus());
				System.out.println("STATUSES: \n"+cluster.getDatabaseStatuses());

				// check that the cluster still has the data
				assertThat(cluster.getDBTable(testTable).count(), is(22l));
				assertThat(cluster.getDBTable(testTable).count(), is(22l));
				assertThat(cluster.getDBTable(testTable).count(), is(22l));
				assertThat(database.getDBTable(testTable).count(), is(22l));
				// check that the new database has gained the data
				assertThat(soloDB.getDBTable(testTable).count(), is(22l));

				cluster.removeDatabase(soloDB);
				DBDatabaseClusterTest.DBDatabaseClusterTestTable2 example = new DBDatabaseClusterTest.DBDatabaseClusterTestTable2();
				example.uidMarque.permittedValues(1);
				DBDatabaseClusterTest.DBDatabaseClusterTestTable2 row = soloDB.getDBTable(example).getOnlyRow();
				row.isUsedForTAFROs.setValue("ANYTHING");
				soloDB.update(row);

				row = soloDB.getDBTable(example).getOnlyRow();
				assertThat(row.isUsedForTAFROs.getValue(), is("ANYTHING"));

				cluster.addDatabaseAndWait(soloDB);

				assertThat(cluster.getDBTable(testTable).count(), is(22l));
				assertThat(database.getDBTable(testTable).count(), is(22l));
				assertThat(soloDB.getDBTable(testTable).count(), is(22l));

				example = new DBDatabaseClusterTest.DBDatabaseClusterTestTable2();
				example.uidMarque.permittedValues(1);
				row = soloDB.getDBTable(example).getOnlyRow();
				assertThat(row.isUsedForTAFROs.getValue(), is("False"));
			}
		}
	}

	private List<DBDatabaseClusterTest.DBDatabaseClusterTestTable2> createData2(Date firstDate, Date secondDate) {
		List<DBDatabaseClusterTest.DBDatabaseClusterTestTable2> data = new ArrayList<>();
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", firstDate, 2, false));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", firstDate, 1, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", firstDate, 1, true));
		data.add(new DBDatabaseClusterTest.DBDatabaseClusterTestTable2(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", secondDate, 3, null));

		return data;

	}
	
	@Test
	public synchronized void testWithoutAutoConnectClusterDoesNotLoadDatabases() throws SQLException {
		final String nameOfCluster = "testWithoutAutoConnectClusterDoesNotLoadDatabases";
		{
			// make sure there isn't anything left around from a previous version
			DBDatabaseCluster cluster
					= new DBDatabaseCluster(
							nameOfCluster,
							DBDatabaseCluster.Configuration.fullyManual().withAutoStart().withAutoConnect(),
							database);
			cluster.dismantle();
		}
		{
			// construct a new cluster with 2 databases
			DBDatabaseCluster cluster
					= new DBDatabaseCluster(
							nameOfCluster,
							DBDatabaseCluster.Configuration.fullyManual().withAutoStart(),
							database);

			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);
			cluster.waitUntilSynchronised(5000);
			assertThat(cluster.size(), is(2));
		}
		{
			// construct a new empty instance
			DBDatabaseCluster cluster = new DBDatabaseCluster(
					nameOfCluster,
					DBDatabaseCluster.Configuration.fullyManual().withAutoStart()
			);

			// make sure it's empty
			assertThat(cluster.size(), is(0));

			// and clean up after ourselves
			cluster.dismantle();
		}
	}

	private static class BrokenDatabase extends DBDatabaseHandle {

		private static final long serialVersionUID = 1L;
		public boolean useBrokenBehaviour;
		private final transient Object ACTION = new Object();

		public BrokenDatabase(DBDatabase db) throws SQLException {
			super(db);
		}

		@Override
		public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
			if (useBrokenBehaviour) {
				try {
					System.out.println("DATABASE " + getLabel() + " INSERTING  BROKEN ACTION");
					final DBActionList result = super.executeDBAction(new BrokenAction());
					return result;
				} finally {
					synchronized (ACTION) {
						ACTION.notifyAll();
					}
				}
			} else {
				return super.executeDBAction(action);
			}
		}
	}
}
