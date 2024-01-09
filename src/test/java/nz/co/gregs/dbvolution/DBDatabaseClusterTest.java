/*
 * Copyright 2017 gregorygraham.
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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.utility.Brake;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBRequiredTable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DBDatabaseClusterWithConfigFile;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.looper.Looper;
import nz.co.gregs.regexi.Match;
import nz.co.gregs.regexi.Regex;
import nz.co.gregs.regexi.internal.PartialRegex;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Assert;

import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class DBDatabaseClusterTest extends AbstractTest {

	public DBDatabaseClusterTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public synchronized void testAutomaticDataCreation() throws SQLException, InterruptedException {
		final DBDatabaseClusterTestTable testTable = new DBDatabaseClusterTestTable();

		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.addTrackedTable(testTable);
			Assert.assertTrue(cluster.tableExists(testTable));
			final DBTable<DBDatabaseClusterTestTable> query = cluster
					.getDBTable(testTable)
					.setBlankQueryAllowed(true);
			final List<DBDatabaseClusterTestTable> allRows = query.getAllRows();

			cluster.delete(allRows);

			Date firstDate = new Date();
			Date secondDate = new Date();
			List<DBDatabaseClusterTestTable> data = createData(firstDate, secondDate);

			cluster.insert(data);

			assertThat(cluster.getDBTable(testTable).count(), is(22l));

			try (H2MemoryDB soloDB = H2MemoryDB.createANewRandomDatabase()) {

				Assert.assertTrue(soloDB.tableExists(testTable));
				assertThat(soloDB.getDBTable(testTable).count(), is(0l));

				cluster.addDatabaseAndWait(soloDB);

				assertThat(cluster.getDBTable(testTable).count(), is(22l));
				assertThat(soloDB.getDBTable(testTable).count(), is(22l));

				try (TestingDatabase slowSynchingDB = TestingDatabase.createANewRandomDatabase("SlowSynchingDatabase-", "-H2")) {
					Brake brake = slowSynchingDB.getBrake();
					brake.setTimeout(10);

					brake.release();
					assertThat(slowSynchingDB.getDBTable(testTable).count(), is(0l));

					brake.apply();
					cluster.addDatabase(slowSynchingDB);
					assertThat(cluster.getDatabaseStatus(slowSynchingDB), not(DBDatabaseCluster.Status.READY));
					assertThat(slowSynchingDB.getDBTable(testTable).count(), is(0l));

					Looper looper = Looper.loopUntilSuccessOrLimit(5);
					looper.loop(
							(index) -> {
								cluster.waitUntilDatabaseIsSynchronised(slowSynchingDB, 100);
							},
							(index) -> cluster.getDatabaseStatus(slowSynchingDB) == DBDatabaseCluster.Status.READY,
							(index) -> {
								System.out.println("" + looper.attempts() + "> SYNCHRONISED: elapsed time " + looper.elapsedTime());
								System.out.println("-----THIS SHOULD NOT HAVE HAPPENED-----");
							},
							(index) -> {
								System.out.println("FAILED TO SYNCHRONISE in " + looper.attempts() + " attempts: elapsed time " + looper.elapsedTime());
								System.out.println("THIS IS DELIBERATE AND EXPECTED");
							}
					);

					brake.release();
					cluster.waitUntilSynchronised();

					assertThat(looper.attempts(), is(Matchers.greaterThan(1)));
					assertThat(cluster.getDatabaseStatus(slowSynchingDB), is(DBDatabaseCluster.Status.READY));
					assertThat(slowSynchingDB.getDBTable(testTable).count(), is(22l));

					cluster.delete(cluster.getDBTable(testTable)
							.setBlankQueryAllowed(true)
							.getAllRows()
					);

					assertThat(cluster.getDBTable(testTable).count(), is(0l));

					assertThat(soloDB.getDBTable(testTable).count(), is(0l));
					assertThat(slowSynchingDB.getDBTable(testTable).count(), is(0l));
				}
			}
		}
	}

	@Test
	public synchronized void testAutomaticDataUpdating() throws SQLException, InterruptedException, UnexpectedNumberOfRowsException {
		final DBDatabaseClusterTestTable2 testTable = new DBDatabaseClusterTestTable2();

		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			// check that the table was automatically created, as all required tables should be
			Assert.assertTrue(cluster.tableExists(testTable));
			final DBTable<DBDatabaseClusterTestTable2> query
					= cluster
							.getDBTable(testTable)
							.setBlankQueryAllowed(true);
			final List<DBDatabaseClusterTestTable2> allRows = query.getAllRows();

			// clear any old data, there shouldn't be any but you never know
			cluster.delete(allRows);
			// check that the data is NOT in the cluster
			assertThat(cluster.getDBTable(testTable).count(), is(0l));
			// check that the data is NOT in the actual database
			assertThat(database.getDBTable(testTable).count(), is(0l));

			// make some data
			Date firstDate = new Date();
			Date secondDate = new Date();
			List<DBDatabaseClusterTestTable2> data = createData2(firstDate, secondDate);
			// insert the data we going to use
			cluster.insert(data);
			// check that the data is in the cluster
			assertThat(cluster.getDBTable(testTable).count(), is(22l));
			// check that the data is in the actual database
			assertThat(database.getDBTable(testTable).count(), is(22l));

			// create a completely new database
			try (H2MemoryDB soloDB = H2MemoryDB.createANewRandomDatabase()) {
				// check that it automatically gets the required table
				Assert.assertTrue(soloDB.tableExists(testTable));
				// check that it DOES NOT have the data
				assertThat(soloDB.getDBTable(testTable).count(), is(0l));

				// add it to the database and give it a chance to catch up
				cluster.addDatabase(soloDB);
				cluster.waitUntilDatabaseIsSynchronised(soloDB);

				// check that the cluster still has the data
				assertThat(cluster.getDBTable(testTable).count(), is(22l));
				assertThat(database.getDBTable(testTable).count(), is(22l));
				// check that the new database has gained the data
				assertThat(soloDB.getDBTable(testTable).count(), is(22l));

				cluster.removeDatabase(soloDB);
				DBDatabaseClusterTestTable2 example = new DBDatabaseClusterTestTable2();
				example.uidMarque.permittedValues(1);
				DBDatabaseClusterTestTable2 row = soloDB.getDBTable(example).getOnlyRow();
				row.isUsedForTAFROs.setValue("ANYTHING");
				soloDB.update(row);

				row = soloDB.getDBTable(example).getOnlyRow();
				assertThat(row.isUsedForTAFROs.getValue(), is("ANYTHING"));

				cluster.addDatabaseAndWait(soloDB);

				assertThat(cluster.getDBTable(testTable).count(), is(22l));
				assertThat(database.getDBTable(testTable).count(), is(22l));
				assertThat(soloDB.getDBTable(testTable).count(), is(22l));

				example = new DBDatabaseClusterTestTable2();
				example.uidMarque.permittedValues(1);
				row = soloDB.getDBTable(example).getOnlyRow();
				assertThat(row.isUsedForTAFROs.getValue(), is("False"));
			}
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQuery() throws SQLException {
		System.out.println("nz.co.gregs.dbvolution.DBDatabaseClusterTest.testDatabaseRemovedAfterErrorInQuery()");
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("CLUSTER-testDatabaseRemovedAfterErrorInQuery");
			assertThat(cluster.size(), is(1));

			cluster.setFailOnQuarantine(false);

			cluster.addTrackedTable(new Marque());
			try (H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase()) {
				soloDB2.setLabel("MEMBER-2");
				cluster.addDatabaseAndWait(soloDB2);
				System.out.println("STATUSES: \n" + cluster.getDatabaseStatuses());
				assertThat(cluster.size(), is(2));

				DBQuery query = cluster.getDBQuery(new Marque());
				query.setRawSQL("and blart = norn");
				try {
					// avoid printing lots of exceptions 
					cluster.setQuietExceptionsPreference(true);
					query.setQuietExceptions(true);
					List<DBQueryRow> allRows = query.getAllRows();
					// should throw an exception before this
					System.out.println("ALLROWS: " + allRows.size());
					Assert.fail("Failed to quarantine database after unsuccessful query");
				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				} finally {
					cluster.setQuietExceptionsPreference(false);
				}
				assertThat(cluster.size(), is(1));
			}
		}
	}

	@Test
	public synchronized void testLastDatabaseCannotBeRemovedAfterErrorInQuery() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.addTrackedTable(new Marque());
			try (H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase()) {
				cluster.addDatabaseAndWait(soloDB2);
				assertThat(cluster.size(), is(2));

				DBQuery query = cluster.getDBQuery(new Marque());
				query.setRawSQL("and blart = norn");
				query.setQuietExceptions(true);
				try {
					// avoid printing lots of exceptions
					cluster.setQuietExceptionsPreference(true);

					List<DBQueryRow> allRows = query.getAllRows();
					Assert.fail("Failed to throw exception after unsuccessful query");
				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				} finally {
					cluster.setQuietExceptionsPreference(false);
				}
				assertThat(cluster.size(), is(1));
				query = cluster.getDBQuery(new Marque());
				try {
					List<DBQueryRow> allRows = query.getAllRows();
					Assert.fail("Failed to throw exception after unsuccessful query");
				} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				}
				assertThat(cluster.size(), is(1));
			}
		}
	}

	@Test
	public synchronized void testAutoRebuildRecreatesData() throws SQLException {
		if (!database.isMemoryDatabase()) {
			final String nameOfCluster = "testAutoRebuildRearrangesCluster";
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(
								nameOfCluster,
								DBDatabaseCluster.Configuration.autoRebuild(),
								database);
				boolean dismantleCluster = true;
				H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
				try {
					cluster.addDatabase(soloDB2);
					assertThat(cluster.size(), is(2));

					cluster.delete(cluster.getDBTable(new DBDatabaseClusterTestTable()).setBlankQueryAllowed(true).getAllRows());
					cluster.insert(createData(new Date(), new Date()));
					DBQuery query = cluster.getDBQuery(new DBDatabaseClusterTestTable()).setBlankQueryAllowed(true);
					assertThat(query.getAllRows().size(), is(22));
					dismantleCluster = false;
				} finally {
					if (dismantleCluster) {
						cluster.dismantle();
					}
					soloDB2.stop();
				}
			}
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(nameOfCluster, DBDatabaseCluster.Configuration.autoRebuild());

				try {
					H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
					assertThat(
							soloDB2.getDBTable(new DBDatabaseClusterTestTable()).setBlankQueryAllowed(true).count(),
							is(0l));

					cluster.addDatabase(soloDB2);
					assertThat(cluster.size(), is(1));

					DBQuery query = cluster
							.getDBQuery(new DBDatabaseClusterTestTable())
							.setBlankQueryAllowed(true);
					assertThat(query.getAllRows().size(), is(22));
				} finally {
					cluster.dismantle();
				}
			}
		}
	}

	@Test
	public synchronized void testWithoutAutoRebuildItDoesntRecreateData() throws SQLException {
		if (!database.isMemoryDatabase()) {
			final String nameOfCluster = "testWithoutAutoRebuildItDoesntRecreateData";
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(
								nameOfCluster,
								DBDatabaseCluster.Configuration.autoStart(),
								database);
				boolean dismantleCluster = true;
				H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
				try {
					cluster.addDatabaseAndWait(soloDB2);
					assertThat(cluster.size(), is(2));

					cluster.delete(cluster.getDBTable(new DBDatabaseClusterTestTable()).setBlankQueryAllowed(true).getAllRows());
					cluster.insert(createData(new Date(), new Date()));
					DBQuery query = cluster.getDBQuery(new DBDatabaseClusterTestTable()).setBlankQueryAllowed(true);
					assertThat(query.getAllRows().size(), is(22));
					dismantleCluster = false;
				} finally {
					if (dismantleCluster) {
						cluster.dismantle();
					}
					soloDB2.stop();
				}
			}
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(nameOfCluster, DBDatabaseCluster.Configuration.autoStart());

				try {
					H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
					assertThat(
							soloDB2.getDBTable(new DBDatabaseClusterTestTable()).setBlankQueryAllowed(true).count(),
							is(0l));

					cluster.addDatabase(soloDB2);
					assertThat(cluster.size(), is(1));

					DBQuery query = cluster
							.getDBQuery(new DBDatabaseClusterTestTable())
							.setBlankQueryAllowed(true);
					assertThat(query.getAllRows().size(), is(0));
				} finally {
					cluster.dismantle();
				}
			}
		}
	}

	@Test
	public synchronized void testAutoRebuildLoadsTrackedTables() throws SQLException {
		if (!database.isMemoryDatabase()) {
			final Marque newTrackedTable = new Marque();
			final String nameOfCluster = "testAutoRebuildLoadsTrackedTables";
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(
								nameOfCluster,
								DBDatabaseCluster.Configuration.autoRebuild(),
								database);
				boolean dismantleCluster = true;
				H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
				try {
					cluster.addDatabase(soloDB2);
					assertThat(cluster.size(), is(2));

					cluster.addTrackedTable(newTrackedTable);

					List<String> asList = Arrays.asList(cluster.getTrackedTables())
							.stream()
							.map(t -> t.getClass().getCanonicalName())
							.collect(Collectors.toList());
					assertThat(asList, hasItem(newTrackedTable.getClass().getCanonicalName()));

					dismantleCluster = false;
				} finally {
					if (dismantleCluster) {
						cluster.dismantle();
					}
					soloDB2.stop();
				}
			}
			{
				DBDatabaseCluster cluster = new DBDatabaseCluster(
						nameOfCluster, DBDatabaseCluster.Configuration.autoRebuild()
				);

				try {
					List<String> asList = Arrays.asList(cluster.getTrackedTables())
							.stream()
							.map(t -> t.getClass().getCanonicalName())
							.collect(Collectors.toList());
					assertThat(asList, hasItem(newTrackedTable.getClass().getCanonicalName()));
				} finally {
					cluster.dismantle();
				}
			}
		}
	}

	@Test
	public synchronized void testAutoRebuildLoadsStaticInnerTrackedTables() throws SQLException {
		if (!database.isMemoryDatabase()) {
			final DBDatabaseClusterTestTrackedTable newTrackedTable = new DBDatabaseClusterTestTrackedTable();
			final String nameOfCluster = "testAutoRebuildLoadsStaticInnerTrackedTables";
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(
								nameOfCluster,
								DBDatabaseCluster.Configuration.autoRebuild(),
								database);
				boolean dismantleCluster = true;
				H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
				try {
					cluster.addDatabase(soloDB2);
					assertThat(cluster.size(), is(2));

					cluster.addTrackedTable(newTrackedTable);

					List<String> asList = Arrays.asList(cluster.getTrackedTables())
							.stream()
							.map(t -> t.getClass().getCanonicalName())
							.collect(Collectors.toList());
					assertThat(asList, hasItem(newTrackedTable.getClass().getCanonicalName()));

					dismantleCluster = false;
				} finally {
					if (dismantleCluster) {
						cluster.dismantle();
					}
					soloDB2.stop();
				}
			}
			{
				DBDatabaseCluster cluster = new DBDatabaseCluster(
						nameOfCluster, DBDatabaseCluster.Configuration.autoRebuild()
				);

				try {
					List<String> asList = Arrays.asList(cluster.getTrackedTables())
							.stream()
							.map(t -> t.getClass().getCanonicalName())
							.collect(Collectors.toList());
					assertThat(asList, hasItem(newTrackedTable.getClass().getCanonicalName()));
				} finally {
					cluster.dismantle();
				}
			}
		}
	}

	@Test
	public synchronized void testManualDoesntLoadTrackedTables() throws SQLException {
		if (!database.isMemoryDatabase()) {
			final String nameOfCluster = "testManualDoesntLoadTrackedTables";
			final DBDatabaseClusterTestTrackedTable trackedTable = new DBDatabaseClusterTestTrackedTable();
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(
								nameOfCluster,
								DBDatabaseCluster.Configuration.autoStart(),
								database);
				boolean dismantleCluster = true;
				H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
				try {
					cluster.addDatabase(soloDB2);
					assertThat(cluster.size(), is(2));

					cluster.addTrackedTable(trackedTable);
					List<String> asList = Arrays.asList(cluster.getTrackedTables()).stream().map(t -> t.getClass().getCanonicalName()).collect(Collectors.toList());
					assertThat(asList, hasItem(DBDatabaseClusterTestTrackedTable.class
							.getCanonicalName()));
					dismantleCluster = false;
				} finally {
					if (dismantleCluster) {
						cluster.dismantle();
					}
					soloDB2.stop();
				}
			}
			{
				DBDatabaseCluster cluster
						= new DBDatabaseCluster(nameOfCluster, DBDatabaseCluster.Configuration.autoRebuild());

				try {
					List<DBRow> asList = Arrays.asList(cluster.getTrackedTables());
					assertThat(asList, not(hasItem(trackedTable)));
				} finally {
					cluster.dismantle();

				}
			}
		}
	}

	public static class DBDatabaseClusterTestTrackedTable extends DBRow {

		public static final long serialVersionUID = 1L;
		@DBColumn(value = "NUMERIC_CODE")
		public DBNumber numericCode = new DBNumber();
		@DBColumn(value = "UID_MARQUE")
		@DBPrimaryKey
		public DBInteger uidMarque = new DBInteger();
		@DBColumn(value = "ISUSEDFORTAFROS")
		public DBString isUsedForTAFROs = new DBString();

		public DBDatabaseClusterTestTrackedTable() {
		}
	}

	@Test
	public synchronized void testLastDatabaseCannotBeRemovedDirectly() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testLastDatabaseCannotBeRemovedDirectly");
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabaseAndWait(soloDB2);
			assertThat(cluster.size(), is(2));

			assertThat(cluster.removeDatabase(cluster.getReadyDatabase()), is(true));
			assertThat(cluster.size(), is(1));
			try {
				cluster.removeDatabase(cluster.getReadyDatabase());
				Assert.fail();
			} catch (Exception e) {
				assertThat(e, is(instanceOf(UnableToRemoveLastDatabaseFromClusterException.class
				)));
			}
			assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterPersistentErrorInDelete() throws ClusterHasQuarantinedADatabaseException, SQLException {
		try (DBDatabaseCluster cluster
				= DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterPersistentErrorInDelete");
			var testingDB = TestingDatabase.createANewRandomDatabase();
			cluster.addDatabaseAndWait(testingDB);
			assertThat(cluster.size(), is(2));
			final TableThatDoesExistOnTheCluster tab = new TableThatDoesExistOnTheCluster();
			tab.pkid.setValue(1);
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				testingDB.setFailOnDelete(true);
				// do the actual action we're trying to test
				cluster.delete(tab);
			} catch (SQLException exc) {
				// we don't expect a failure here so print the stacktrace and fail
				exc.printStackTrace();
				Assert.fail("UNEXPECTED EXCEPTION");
			} finally {
				cluster.setQuietExceptionsPreference(false);
				assertThat(cluster.size(), is(1));
			}
		}
	}

	@Test
	public synchronized void testClusterUnchangedAfterDeletingFromNonExistentTable() throws ClusterHasQuarantinedADatabaseException, SQLException {
		boolean succeeded = false;
		try (DBDatabaseCluster cluster
				= DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterErrorInDelete");
			var testingDB = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabaseAndWait(testingDB);
			assertThat(cluster.size(), is(2));
			final TableThatDoesntExistOnTheCluster tab = new TableThatDoesntExistOnTheCluster();
			tab.pkid.setValue(1);
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				// do the actual action we're trying to test
				cluster.delete(tab);
			} catch (SQLException exc) {
				// we expect it to throw an SQLException because it can't perform it on any database
				assertThat(cluster.size(), is(2));
				succeeded = true;
			} finally {
				cluster.setQuietExceptionsPreference(false);
			}
			assertThat(cluster.size(), is(2));
		}
		assertThat(succeeded, is(true));
	}

	@Test(expected = SQLException.class)
	public synchronized void testSQLExceptionAfterErrorInInsert() throws SQLException {
		if (database instanceof DBDatabaseCluster) {
			DBDatabaseCluster cluster = (DBDatabaseCluster) database;
			cluster.waitUntilSynchronised();
		}
		try (TestingCluster cluster = TestingCluster.randomManualCluster(database)) {
			cluster.setLabel("testSQLExceptionAfterErrorInInsert");
			cluster.start();
			cluster.waitUntilSynchronised();
			// avoid printing lots of exceptions
			cluster.setQuietExceptionsPreference(true);
			try (H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase()) {
				cluster.addDatabaseAndWait(soloDB2);
				assertThat(cluster.size(), is(2));
				final TableThatDoesntExistOnTheCluster tab = new TableThatDoesntExistOnTheCluster();
				tab.pkid.setValue(1);
				System.out.println("DELIBERATE FAILURE WHILE INSERTING");
				try {
					cluster.setFailOnInsert(true);
					cluster.insert(tab);
					assertThat("Never get here", is("We got here"));
				} catch (SQLException e) {
					e.printStackTrace();
					cluster.setFailOnInsert(false);
					throw e;
				} catch (Exception ex) {
					assertThat(ex.getClass(), is(instanceOf(SQLException.class)));
				} finally {
					cluster.setQuietExceptionsPreference(false);
				}
			}
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterFailingInsertPersistently() throws SQLException {
		try (DBDatabaseCluster cluster
				= DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterFailingInsertPersistently");
			TestingDatabase testingDB = TestingDatabase.createANewRandomDatabase();
			cluster.addDatabaseAndWait(testingDB);
			assertThat(cluster.size(), is(2));
			var row = new CarCompany("GREAT WALL", 17);
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				testingDB.setFailOnInsert(true);
				cluster.insert(row);
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				testingDB.setFailOnInsert(false);
				cluster.setQuietExceptionsPreference(false);
				try {
					assertThat(cluster.size(), is(1));
				} finally {
					cluster.delete(row);
				}

			}
		}
	}

	@Test()
	public synchronized void testDatabaseRemovedAfterErrorInUpdate() throws SQLException {
		try (DBDatabaseCluster cluster
				= DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterErrorInUpdate");
			CarCompany row = new CarCompany("GREAT WALL", 17);
			cluster.insert(row);
			row.name.setValue("GREATER WALL");
			var testingDB = TestingDatabase.createANewRandomDatabase();
			cluster.addDatabaseAndWait(testingDB);
			assertThat(cluster.size(), is(2));
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				testingDB.setFailOnUpdate(true);
				cluster.update(row);
			} finally {
				cluster.setQuietExceptionsPreference(false);
				try {
					assertThat(cluster.size(), is(1));
				} finally {
					cluster.delete(row);
				}
			}
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInCreateTable() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterErrorInCreateTable");

			var testingDB = TestingDatabase.createANewRandomDatabase();
			cluster.addDatabaseAndWait(testingDB);
			assertThat(cluster.size(), is(2));
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				// set the database to fail whenever creating databases
				testingDB.setFailOnCreateTable(true);
				// try to create the table via the cluster and thus get quarantined
				cluster.createTableNoExceptions(new TableThatDoesExistOnTheCluster());
			} finally {
				cluster.setQuietExceptionsPreference(false);
				try {
					assertThat(cluster.size(), is(1));
				} finally {
					// drop the table so we don't interfere with other tests
					cluster.preventDroppingOfTables(false);
					cluster.dropTable(new TableThatDoesExistOnTheCluster());
				}
			}
		}
	}

	@Test
	public synchronized void testDatabaseRemainsInClusterAfterCreatingExistingTable() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemainsInClusterAfterCreatingExistingTable");
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabaseAndWait(soloDB2);
			assertThat(cluster.size(), is(2));
			try {
				cluster.createTable(new TableThatDoesExistOnTheCluster());
				// Create table now self corrects if the table exists already
				// so just wait for synchronisation
				cluster.waitUntilSynchronised(10000);
			} catch (SQLException | AutoCommitActionDuringTransactionException e) {
			}
			assertThat(cluster.size(), is(2));
		}
	}

	@Test
	public synchronized void testDatabaseTableExists() throws SQLException {
		Assert.assertTrue(database.tableExists(new TableThatDoesExistOnTheCluster()));
	}

	@Test
	public synchronized void testDatabaseTableDoesNotExists() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseTableDoesNotExists");
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabaseAndWait(soloDB2);
			cluster.setQuietExceptionsPreference(true);
			final boolean tableExists = cluster.tableExists(new TableThatDoesntExistOnTheCluster());
			cluster.setQuietExceptionsPreference(false);
			Assert.assertFalse(tableExists);
		}
	}

	@Test
	public void testYAMLFileProcessing() throws SQLException {
		final String yamlConfigFilename = "DBDatabaseCluster.yml";

		File file = new File(yamlConfigFilename);
		file.delete();

		DBDatabaseCluster cluster = new DBDatabaseCluster("testYAMLFileProcessing",
				DBDatabaseCluster.Configuration.autoStart());
		try {
			cluster = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessing2",
					DBDatabaseCluster.Configuration.autoStart(), yamlConfigFilename);
		} catch (SecurityException | IllegalArgumentException | DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
			assertThat(ex, is(instanceOf(DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound.class
			)));
		}
		String clusterStatus = cluster.getClusterStatus();

		// Check that the statuses are all "0 of 0"
		final String STATUS_NAME = "name";
		PartialRegex statuses = Regex.multiline()
				.beginNamedCapture(STATUS_NAME).uppercaseCharacter().oneOrMore().endNamedCapture()
				.space().literal("Databases:").space();
		Regex statusesFound
				= statuses.integer()
						.space().literal("of")
						.space().integer().endRegex();
		Regex statusesOfZero
				= statuses.literal("0 of 0").endRegex();
		assertThat(statusesFound.getAllMatches(clusterStatus).size(), is(9));
		assertThat(statusesOfZero.getAllMatches(clusterStatus).size(), is(9));

		DatabaseConnectionSettings source = new DatabaseConnectionSettings();
		source.setDbdatabaseClass(H2MemoryDB.class
				.getCanonicalName());
		source.setDatabaseName("DBDatabaseClusterWithConfigFile.h2");
		source.setUsername("admin");
		source.setPassword("admin");

		DatabaseConnectionSettings source2 = new DatabaseConnectionSettings();
		source2.setDbdatabaseClass(SQLiteDB.class
				.getCanonicalName());
		source2.setUrl("jdbc:sqlite:DBDatabaseClusterWithConfigFile.sqlite");
		source2.setUsername("admin");
		source2.setPassword("admin");

		final YAMLFactory yamlFactory = new YAMLFactory();
		file = new File(yamlConfigFilename);
		JsonGenerator generator = null;
		try {
			generator = yamlFactory.createGenerator(file, JsonEncoding.UTF8);
		} catch (IOException ex) {
			Logger.getLogger(DBDatabaseClusterTest.class
					.getName()).log(Level.SEVERE, null, ex);
			Assert.fail(ex.getMessage());
		}
		ObjectMapper mapper = new ObjectMapper(yamlFactory);
		ObjectWriter writerFor = mapper.writerFor(DatabaseConnectionSettings.class
		);
		SequenceWriter writeValuesAsArray = null;
		try {
			writeValuesAsArray = writerFor.writeValuesAsArray(generator);
		} catch (IOException ex) {
			Logger.getLogger(DBDatabaseClusterTest.class
					.getName()).log(Level.SEVERE, null, ex);
			Assert.fail(ex.getMessage());
		}
		try {
			if (writeValuesAsArray != null) {
				writeValuesAsArray.writeAll(new DatabaseConnectionSettings[]{source, source2});
			}
		} catch (IOException ex) {
			Logger.getLogger(DBDatabaseClusterTest.class
					.getName()).log(Level.SEVERE, null, ex);
			Assert.fail(ex.getMessage());
		}
		try {
			try {
				cluster = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessing3",
						DBDatabaseCluster.Configuration.autoStart(), yamlConfigFilename);
				assertThat(cluster.getDatabases()[1].getJdbcURL(), containsString("jdbc:h2:mem:DBDatabaseClusterWithConfigFile.h2"));
				assertThat(cluster.getDatabases()[0].getJdbcURL(), containsString("jdbc:sqlite:DBDatabaseClusterWithConfigFile.sqlite"));
			} catch (DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
				Logger.getLogger(DBDatabaseClusterTest.class
						.getName()).log(Level.SEVERE, null, ex);
				Assert.fail(ex.getMessage());
			}
			clusterStatus = cluster.getClusterStatus();

			// Check that READY is "2 of 2" and the rest are "0 of 2"
			assertThat(statusesFound.getAllMatches(clusterStatus).size(), is(9));
			assertThat(statusesOfZero.getAllMatches(clusterStatus).size(), is(0));
			final String VALUE = "value";
			final String TOTAL = "total";
			Regex actualStatuses = statuses
					.beginNamedCapture(VALUE).integer().endNamedCapture()
					.literal(" of ")
					.beginNamedCapture(TOTAL).integer().endNamedCapture()
					.endRegex();
			List<Match> matches = actualStatuses.getAllMatches(clusterStatus);
			assertThat(matches.size(), is(9));
			boolean foundReady = false;
			for (Match match : matches) {
				final String name = match.getNamedCapture(STATUS_NAME);
				final String value = match.getNamedCapture(VALUE);
				final String total = match.getNamedCapture(TOTAL);
				assertThat(total, is("2"));
				switch (name) {
					case "READY":
						assertThat(value, is("2"));
						foundReady = true;
						break;

					default:
						assertThat(value, is("0"));
				}
			}
			assertThat(foundReady, is(true));
		} finally {
			cluster.dismantle();
		}

		file.delete();
	}

	@Test
	public void testYAMLFileProcessingWithFile() throws SQLException {

		new DBDatabaseCluster("testYAMLFileProcessingWithFile",
				DBDatabaseCluster.Configuration.autoStart()).dismantle();
		new DBDatabaseCluster("testYAMLFileProcessingWithFile2",
				DBDatabaseCluster.Configuration.autoStart()).dismantle();

		final String yamlConfigFilename = "DBDatabaseCluster.yml";

		File file = new File(yamlConfigFilename);
		file.delete();

		DBDatabaseCluster cluster = new DBDatabaseCluster("testYAMLFileProcessingWithFile",
				DBDatabaseCluster.Configuration.autoStart());
		try {
			try {
				cluster = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessingWithFile2",
						DBDatabaseCluster.Configuration.autoStart(), yamlConfigFilename);
			} catch (SecurityException | IllegalArgumentException | DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
				assertThat(ex, is(instanceOf(DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound.class
				)));
			}
			String clusterStatus = cluster.getClusterStatus();

			// Check that the statuses are all "0 of 0"
			final String STATUS_NAME = "name";
			PartialRegex statuses = Regex.multiline()
					.beginNamedCapture(STATUS_NAME).uppercaseCharacter().oneOrMore().endNamedCapture()
					.space().literal("Databases:").space();
			Regex statusesFound
					= statuses.integer()
							.space().literal("of")
							.space().integer().endRegex();
			Regex statusesOfZero
					= statuses.literal("0 of 0").endRegex();
			assertThat(statusesFound.getAllMatches(clusterStatus).size(), is(9));
			assertThat(statusesOfZero.getAllMatches(clusterStatus).size(), is(9));

			DatabaseConnectionSettings source = new DatabaseConnectionSettings();
			source.setDbdatabaseClass(H2MemoryDB.class
					.getCanonicalName());
			source.setDatabaseName("DBDatabaseClusterWithConfigFile.h2");
			source.setUsername("admin");
			source.setPassword("admin");

			DatabaseConnectionSettings source2 = new DatabaseConnectionSettings();
			source2.setDbdatabaseClass(SQLiteDB.class
					.getCanonicalName());
			source2.setUrl("jdbc:sqlite:DBDatabaseClusterWithConfigFile.sqlite");
			source2.setUsername("admin");
			source2.setPassword("admin");

			final YAMLFactory yamlFactory = new YAMLFactory();
			file = new File(yamlConfigFilename);
			JsonGenerator generator = null;
			try {
				generator = yamlFactory.createGenerator(file, JsonEncoding.UTF8);
			} catch (IOException ex) {
				Logger.getLogger(DBDatabaseClusterTest.class
						.getName()).log(Level.SEVERE, null, ex);
				Assert.fail(ex.getMessage());
			}
			ObjectMapper mapper = new ObjectMapper(yamlFactory);
			ObjectWriter writerFor = mapper.writerFor(DatabaseConnectionSettings.class
			);
			SequenceWriter writeValuesAsArray = null;
			try {
				writeValuesAsArray = writerFor.writeValuesAsArray(generator);
			} catch (IOException ex) {
				Logger.getLogger(DBDatabaseClusterTest.class
						.getName()).log(Level.SEVERE, null, ex);
				Assert.fail(ex.getMessage());
			}
			try {
				if (writeValuesAsArray != null) {
					writeValuesAsArray.writeAll(new DatabaseConnectionSettings[]{source, source2});
				}
			} catch (IOException ex) {
				Logger.getLogger(DBDatabaseClusterTest.class
						.getName()).log(Level.SEVERE, null, ex);
				Assert.fail(ex.getMessage());
			}

			try {
				cluster = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessingWithFile",
						DBDatabaseCluster.Configuration.autoStart(), file);
				assertThat(cluster.getDatabases()[1].getJdbcURL(), containsString("jdbc:h2:mem:DBDatabaseClusterWithConfigFile.h2"));
				assertThat(cluster.getDatabases()[0].getJdbcURL(), containsString("jdbc:sqlite:DBDatabaseClusterWithConfigFile.sqlite"));
			} catch (DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
				Logger.getLogger(DBDatabaseClusterTest.class
						.getName()).log(Level.SEVERE, null, ex);
				Assert.fail(ex.getMessage());
			}
			clusterStatus = cluster.getClusterStatus();

			// Check that READY is "2 of 2" and the rest are "0 of 2"
			assertThat(statusesFound.getAllMatches(clusterStatus).size(), is(9));
			assertThat(statusesOfZero.getAllMatches(clusterStatus).size(), is(0));
			final String VALUE = "value";
			final String TOTAL = "total";
			Regex actualStatuses = statuses.beginNamedCapture(VALUE).integer().endNamedCapture().literal(" of ").beginNamedCapture(TOTAL).integer().endNamedCapture().endRegex();
			List<Match> matches = actualStatuses.getAllMatches(clusterStatus);
			assertThat(matches.size(), is(9));
			boolean foundReady = false;
			for (Match match : matches) {
				final String name = match.getNamedCapture(STATUS_NAME);
				final String value = match.getNamedCapture(VALUE);
				final String total = match.getNamedCapture(TOTAL);
				assertThat(total, is("2"));
				switch (name) {
					case "READY":
						assertThat(value, is("2"));
						foundReady = true;
						break;

					default:
						assertThat(value, is("0"));
				}
			}
			assertThat(foundReady, is(true));

			file.delete();

		} finally {
			new DBDatabaseCluster("testYAMLFileProcessingWithFile",
					DBDatabaseCluster.Configuration.autoStart()).dismantle();
			new DBDatabaseCluster("testYAMLFileProcessingWithFile2",
					DBDatabaseCluster.Configuration.autoStart()).dismantle();
		}
	}

	@Test
	public synchronized void testClusterSwitchsSupportForNullStringsWhenOracleIsAddedAndRemoved() throws SQLException {

		try (H2MemoryDB soloDB1 = H2MemoryDB.createANewRandomDatabase()) {
			assertThat(soloDB1.supportsDifferenceBetweenNullAndEmptyString(), is(true));
			try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(soloDB1)) {
				try (H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase()) {
					assertThat(soloDB2.supportsDifferenceBetweenNullAndEmptyString(), is(true));
					cluster.addDatabaseAndWait(soloDB2);
					assertThat(cluster.size(), is(2));
					assertThat(cluster.supportsDifferenceBetweenNullAndEmptyString(), is(true));

					DBDatabase oracle = getDatabaseThatDoesNotSupportDifferenceBetweenEmptyStringsAndNull();
					assertThat(oracle.supportsDifferenceBetweenNullAndEmptyString(), is(false));
					cluster.addDatabaseAndWait(oracle);

					assertThat(cluster.size(), is(3));
					assertThat(cluster.supportsDifferenceBetweenNullAndEmptyString(), is(false));
					assertThat(soloDB1.supportsDifferenceBetweenNullAndEmptyString(), is(true));
					assertThat(soloDB2.supportsDifferenceBetweenNullAndEmptyString(), is(true));
					assertThat(oracle.supportsDifferenceBetweenNullAndEmptyString(), is(false));

					cluster.removeDatabase(oracle);
					assertThat(cluster.size(), is(2));
					assertThat(cluster.supportsDifferenceBetweenNullAndEmptyString(), is(true));
					assertThat(soloDB1.supportsDifferenceBetweenNullAndEmptyString(), is(true));
					assertThat(soloDB2.supportsDifferenceBetweenNullAndEmptyString(), is(true));
					assertThat(oracle.supportsDifferenceBetweenNullAndEmptyString(), is(false));
				} finally {
					assertThat(cluster.size(), Matchers.greaterThan(0));
					cluster.dismantle();
					assertThat(cluster.size(), is(0));
				}
			}
		}
	}

	@Test
	public synchronized void testClusterSwitchsSupportForNullStringsWhenOracleIsAddedAndWhenClusterDismantled() throws SQLException {

		H2MemoryDB soloDB1 = H2MemoryDB.createANewRandomDatabase();
		H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(soloDB1)) {
			cluster.addDatabaseAndWait(soloDB2);
			assertThat(cluster.size(), is(2));
			assertThat(cluster.supportsDifferenceBetweenNullAndEmptyString(), is(true));

			DBDatabase oracle = getDatabaseThatDoesNotSupportDifferenceBetweenEmptyStringsAndNull();
			cluster.addDatabaseAndWait(oracle);
			assertThat(cluster.size(), is(3));
			assertThat(cluster.supportsDifferenceBetweenNullAndEmptyString(), is(false));
			assertThat(oracle.supportsDifferenceBetweenNullAndEmptyString(), is(false));
			assertThat(soloDB1.supportsDifferenceBetweenNullAndEmptyString(), is(true));
			assertThat(soloDB2.supportsDifferenceBetweenNullAndEmptyString(), is(true));
		}
		assertThat(soloDB1.supportsDifferenceBetweenNullAndEmptyString(), is(true));
		assertThat(soloDB2.supportsDifferenceBetweenNullAndEmptyString(), is(true));
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
			soloDB2.setLabel("Member 2");
			soloDB2Settings = soloDB2.getSettings().toString();
			cluster.addDatabaseAndWait(soloDB2);
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
					nameOfCluster, DBDatabaseCluster.Configuration.fullyManual().withAutoConnect().withAutoStart()
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

	private List<DBDatabaseClusterTestTable> createData(Date firstDate, Date secondDate) {
		List<DBDatabaseClusterTestTable> data = new ArrayList<>();
		data.add(new DBDatabaseClusterTestTable(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		data.add(new DBDatabaseClusterTestTable(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", firstDate, 2, false));
		data.add(new DBDatabaseClusterTestTable(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTestTable(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", firstDate, 1, null));
		data.add(new DBDatabaseClusterTestTable(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTestTable(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", firstDate, 1, true));
		data.add(new DBDatabaseClusterTestTable(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", secondDate, 3, null));

		return data;

	}

	@DBRequiredTable
	public static class DBDatabaseClusterTestTable extends DBRow {

		public static final long serialVersionUID = 1L;
		@DBColumn(value = "NUMERIC_CODE")
		public nz.co.gregs.dbvolution.datatypes.DBNumber numericCode = new DBNumber();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "UID_MARQUE")
		@nz.co.gregs.dbvolution.annotations.DBPrimaryKey
		public nz.co.gregs.dbvolution.datatypes.DBInteger uidMarque = new DBInteger();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "ISUSEDFORTAFROS")
		public nz.co.gregs.dbvolution.datatypes.DBString isUsedForTAFROs = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "FK_TOYSTATUSCLASS")
		public nz.co.gregs.dbvolution.datatypes.DBNumber statusClassID = new DBNumber();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "INTINDALLOCALLOWED")
		public nz.co.gregs.dbvolution.datatypes.DBString individualAllocationsAllowed = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "UPD_COUNT")
		public nz.co.gregs.dbvolution.datatypes.DBInteger updateCount = new DBInteger();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "AUTO_CREATED")
		public nz.co.gregs.dbvolution.datatypes.DBString auto_created = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "NAME")
		public nz.co.gregs.dbvolution.datatypes.DBString name = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "PRICINGCODEPREFIX")
		public nz.co.gregs.dbvolution.datatypes.DBString pricingCodePrefix = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "RESERVATIONSALWD")
		public nz.co.gregs.dbvolution.datatypes.DBString reservationsAllowed = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "CREATION_DATE")
		public nz.co.gregs.dbvolution.datatypes.DBDate creationDate = new DBDate();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "ENABLED")
		public nz.co.gregs.dbvolution.datatypes.DBBoolean enabled = new DBBoolean();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "FK_CARCOMPANY")
		public nz.co.gregs.dbvolution.datatypes.DBInteger carCompany = new DBInteger();

		public DBDatabaseClusterTestTable() {
		}

		public DBDatabaseClusterTestTable(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, Date creationDate, int carCompany, Boolean enabled) {
			this.uidMarque.setValue(uidMarque);
			this.isUsedForTAFROs.setValue(isUsedForTAFROs);
			this.statusClassID.setValue(statusClass);
			this.individualAllocationsAllowed.setValue(intIndividualAllocationsAllowed);
			this.updateCount.setValue(updateCount);
			this.auto_created.setValue(autoCreated);
			this.name.setValue(name);
			this.pricingCodePrefix.setValue(pricingCodePrefix);
			this.reservationsAllowed.setValue(reservationsAllowed);
			this.creationDate.setValue(creationDate);
			this.carCompany.setValue(carCompany);
			this.enabled.setValue(enabled);
		}
	}

	private List<DBDatabaseClusterTestTable2> createData2(Date firstDate, Date secondDate) {
		List<DBDatabaseClusterTestTable2> data = new ArrayList<>();
		data.add(new DBDatabaseClusterTestTable2(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		data.add(new DBDatabaseClusterTestTable2(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", firstDate, 2, false));
		data.add(new DBDatabaseClusterTestTable2(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTestTable2(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", firstDate, 1, null));
		data.add(new DBDatabaseClusterTestTable2(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTestTable2(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTestTable2(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", firstDate, 1, true));
		data.add(new DBDatabaseClusterTestTable2(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", secondDate, 3, null));

		return data;

	}

	@DBRequiredTable
	public static class DBDatabaseClusterTestTable2 extends DBRow {

		public static final long serialVersionUID = 1L;
		@DBColumn(value = "NUMERIC_CODE")
		public nz.co.gregs.dbvolution.datatypes.DBNumber numericCode = new DBNumber();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "UID_MARQUE")
		@nz.co.gregs.dbvolution.annotations.DBPrimaryKey
		public nz.co.gregs.dbvolution.datatypes.DBInteger uidMarque = new DBInteger();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "ISUSEDFORTAFROS")
		public nz.co.gregs.dbvolution.datatypes.DBString isUsedForTAFROs = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "FK_TOYSTATUSCLASS")
		public nz.co.gregs.dbvolution.datatypes.DBNumber statusClassID = new DBNumber();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "INTINDALLOCALLOWED")
		public nz.co.gregs.dbvolution.datatypes.DBString individualAllocationsAllowed = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "UPD_COUNT")
		public nz.co.gregs.dbvolution.datatypes.DBInteger updateCount = new DBInteger();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "AUTO_CREATED")
		public nz.co.gregs.dbvolution.datatypes.DBString auto_created = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "NAME")
		public nz.co.gregs.dbvolution.datatypes.DBString name = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "PRICINGCODEPREFIX")
		public nz.co.gregs.dbvolution.datatypes.DBString pricingCodePrefix = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "RESERVATIONSALWD")
		public nz.co.gregs.dbvolution.datatypes.DBString reservationsAllowed = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "CREATION_DATE")
		public nz.co.gregs.dbvolution.datatypes.DBDate creationDate = new DBDate();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "ENABLED")
		public nz.co.gregs.dbvolution.datatypes.DBBoolean enabled = new DBBoolean();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "FK_CARCOMPANY")
		public nz.co.gregs.dbvolution.datatypes.DBInteger carCompany = new DBInteger();

		public DBDatabaseClusterTestTable2() {
		}

		public DBDatabaseClusterTestTable2(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, Date creationDate, int carCompany, Boolean enabled) {
			this.uidMarque.setValue(uidMarque);
			this.isUsedForTAFROs.setValue(isUsedForTAFROs);
			this.statusClassID.setValue(statusClass);
			this.individualAllocationsAllowed.setValue(intIndividualAllocationsAllowed);
			this.updateCount.setValue(updateCount);
			this.auto_created.setValue(autoCreated);
			this.name.setValue(name);
			this.pricingCodePrefix.setValue(pricingCodePrefix);
			this.reservationsAllowed.setValue(reservationsAllowed);
			this.creationDate.setValue(creationDate);
			this.carCompany.setValue(carCompany);
			this.enabled.setValue(enabled);
		}
	}

	public static class TableThatDoesntExistOnTheCluster extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger pkid = new DBInteger();
	}

	public static class TableThatDoesExistOnTheCluster extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger pkid = new DBInteger();
	}
}
