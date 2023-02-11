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
import nz.co.gregs.dbvolution.actions.DBActionList;
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
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.utility.LoopVariable;
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

			H2MemoryDB soloDB = H2MemoryDB.createANewRandomDatabase();

			Assert.assertTrue(soloDB.tableExists(testTable));
			assertThat(soloDB.getDBTable(testTable).count(), is(0l));

			cluster.addDatabase(soloDB);

			assertThat(cluster.getDBTable(testTable).count(), is(22l));
			assertThat(soloDB.getDBTable(testTable).count(), is(22l));

			SlowSynchingDatabase slowSynchingDB = SlowSynchingDatabase.createANewRandomDatabase("SlowSynchingDatabase-", "-H2");
			Brake brake = slowSynchingDB.getBrake();
			brake.setTimeout(10);

			try (slowSynchingDB) {

				brake.release();
				assertThat(slowSynchingDB.getDBTable(testTable).count(), is(0l));

				brake.apply();
				cluster.addDatabase(slowSynchingDB);
				assertThat(cluster.getDatabaseStatus(slowSynchingDB), not(DBDatabaseCluster.Status.READY));
				assertThat(slowSynchingDB.getDBTable(testTable).count(), is(0l));

				LoopVariable looper = LoopVariable.factory();
				looper.setMaxAttemptsAllowed(5);
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

	@Test
	public synchronized void testAutomaticDataUpdating() throws SQLException, InterruptedException, UnexpectedNumberOfRowsException {
		final DBDatabaseClusterTestTable2 testTable = new DBDatabaseClusterTestTable2();

		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			Assert.assertTrue(cluster.tableExists(testTable));
			cluster.addTrackedTable(testTable);
			final DBTable<DBDatabaseClusterTestTable2> query = cluster
					.getDBTable(testTable)
					.setBlankQueryAllowed(true);
			final List<DBDatabaseClusterTestTable2> allRows = query.getAllRows();

			cluster.delete(allRows);

			Date firstDate = new Date();
			Date secondDate = new Date();
			List<DBDatabaseClusterTestTable2> data = createData2(firstDate, secondDate);

			cluster.insert(data);

			assertThat(cluster.getDBTable(testTable).count(), is(22l));
			assertThat(database.getDBTable(testTable).count(), is(22l));

			H2MemoryDB soloDB = H2MemoryDB.createANewRandomDatabase();

			Assert.assertTrue(soloDB.tableExists(testTable));
			assertThat(soloDB.getDBTable(testTable).count(), is(0l));

			cluster.addDatabase(soloDB);
			cluster.waitUntilSynchronised();

			assertThat(cluster.getDBTable(testTable).count(), is(22l));
			assertThat(database.getDBTable(testTable).count(), is(22l));
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

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQuery() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			assertThat(cluster.size(), is(1));

			cluster.addTrackedTable(new Marque());
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabaseAndWait(soloDB2);
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

	@Test
	public synchronized void testLastDatabaseCannotBeRemovedAfterErrorInQuery() throws SQLException {
		DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database);
		cluster.addTrackedTable(new Marque());
		try {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
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
		} finally {
			cluster.dismantle();
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
			cluster.addDatabase(soloDB2);
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
	public synchronized void testDatabaseRemovedAfterErrorInDelete() throws SQLException {
		try (DBDatabaseCluster cluster
				= DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterErrorInDelete");
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);
			final TableThatDoesntExistOnTheCluster tab = new TableThatDoesntExistOnTheCluster();
			tab.pkid.permittedValues(1);
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				// do the actual action we're trying to test
				cluster.delete(tab);
			} catch (SQLException e) {
			} finally {
				cluster.setQuietExceptionsPreference(false);
			}
			assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInInsert() throws SQLException {
		try (DBDatabaseCluster cluster
				= DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterErrorInInsert");
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabaseAndWait(soloDB2);
			assertThat(cluster.size(), is(2));
			final TableThatDoesntExistOnTheCluster tab = new TableThatDoesntExistOnTheCluster();
			tab.pkid.setValue(1);
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				cluster.insert(tab);
			} catch (SQLException e) {
			} finally {
				cluster.setQuietExceptionsPreference(false);
				assertThat(cluster.size(), is(1));
			}
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInUpdate() throws SQLException {
		try (DBDatabaseCluster cluster
				= DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterErrorInUpdate");
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabaseAndWait(soloDB2);
			assertThat(cluster.size(), is(2));
			final TableThatDoesntExistOnTheCluster tab = new TableThatDoesntExistOnTheCluster();
			tab.pkid.setValue(1);
			tab.setDefined();//naughty, but needed otherwise the update won't be generated
			tab.pkid.setValue(2);
			try {
				// avoid printing lots of exceptions
				cluster.setQuietExceptionsPreference(true);
				cluster.update(tab);
			} catch (SQLException e) {
			} finally {
				cluster.setQuietExceptionsPreference(false);
			}
			assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInCreateTable() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			cluster.setLabel("testDatabaseRemovedAfterErrorInCreateTable");

			cluster.createTableNoExceptions(new TableThatDoesExistOnTheCluster());
			
			final H2MemorySettingsBuilder settings
					= new H2MemorySettingsBuilder()
							.setLabel("testDatabaseRemovedAfterErrorInCreateTable")
							.setDatabaseName("testDatabaseRemovedAfterErrorInCreateTable")
							.setUsername("who")
							.setPassword("what");
			H2MemoryDB soloDB2 = new H2MemoryDB(settings) {
				private static final long serialVersionUID = 1l;

				@Override
				public DBActionList createTable(DBRow newTableRow, boolean includeForeignKeyClauses) throws SQLException, AutoCommitActionDuringTransactionException {
					if (newTableRow instanceof TableThatDoesExistOnTheCluster) {
						throw new SQLException("DELIBERATE EXCEPTION");
					} else {
						return super.createTable(newTableRow, includeForeignKeyClauses);
					}
				}

			};
			cluster.setQuietExceptionsPreference(true);
			cluster.addDatabaseAndWait(soloDB2);
			cluster.setQuietExceptionsPreference(false);
			assertThat(cluster.size(), is(1));
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

		DBDatabaseCluster db = new DBDatabaseCluster("testYAMLFileProcessing",
				DBDatabaseCluster.Configuration.autoStart());
		try {
			db = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessing2",
					DBDatabaseCluster.Configuration.autoStart(), yamlConfigFilename);
		} catch (SecurityException | IllegalArgumentException | DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
			assertThat(ex, is(instanceOf(DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound.class
			)));
		}
		assertThat(
				db.getClusterStatus().replaceAll("[a-zA-Z]* [a-zA-Z]* [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [A-Z]{2,4} [0-9]{4}", ""),
				is("Active Databases: 0 of 0\nUnsynchronised: 0 of 0\nQuarantined Databases: 0 of 0"));

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
				db = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessing3",
						DBDatabaseCluster.Configuration.autoStart(), yamlConfigFilename);
				assertThat(db.getDatabases()[1].getJdbcURL(), containsString("jdbc:h2:mem:DBDatabaseClusterWithConfigFile.h2"));
				assertThat(db.getDatabases()[0].getJdbcURL(), containsString("jdbc:sqlite:DBDatabaseClusterWithConfigFile.sqlite"));
			} catch (DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
				Logger.getLogger(DBDatabaseClusterTest.class
						.getName()).log(Level.SEVERE, null, ex);
				Assert.fail(ex.getMessage());
			}
			assertThat(
					db.getClusterStatus().replaceAll("[a-zA-Z]* [a-zA-Z]* [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [A-Z]{2,4} [0-9]{4}", ""),
					is("Active Databases: 2 of 2\nUnsynchronised: 0 of 2\nQuarantined Databases: 0 of 2"));
		} finally {
			db.dismantle();
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

		DBDatabaseCluster db = new DBDatabaseCluster("testYAMLFileProcessingWithFile",
				DBDatabaseCluster.Configuration.autoStart());
		try {
			try {
				db = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessingWithFile2",
						DBDatabaseCluster.Configuration.autoStart(), yamlConfigFilename);
			} catch (SecurityException | IllegalArgumentException | DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
				assertThat(ex, is(instanceOf(DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound.class
				)));
			}
			assertThat(
					db.getClusterStatus().replaceAll("[a-zA-Z]* [a-zA-Z]* [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [A-Z]{2,4} [0-9]{4}", ""),
					is("Active Databases: 0 of 0\nUnsynchronised: 0 of 0\nQuarantined Databases: 0 of 0"));

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
				db = new DBDatabaseClusterWithConfigFile("testYAMLFileProcessingWithFile",
						DBDatabaseCluster.Configuration.autoStart(), file);
				assertThat(db.getDatabases()[1].getJdbcURL(), containsString("jdbc:h2:mem:DBDatabaseClusterWithConfigFile.h2"));
				assertThat(db.getDatabases()[0].getJdbcURL(), containsString("jdbc:sqlite:DBDatabaseClusterWithConfigFile.sqlite"));
			} catch (DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound | DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster ex) {
				Logger.getLogger(DBDatabaseClusterTest.class
						.getName()).log(Level.SEVERE, null, ex);
				Assert.fail(ex.getMessage());
			}
			assertThat(
					db.getClusterStatus().replaceAll("[a-zA-Z]* [a-zA-Z]* [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [A-Z]{2,4} [0-9]{4}", ""),
					is("Active Databases: 2 of 2\nUnsynchronised: 0 of 2\nQuarantined Databases: 0 of 2"));

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
					cluster.addDatabase(soloDB2);
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
			cluster.addDatabase(soloDB2);
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
			soloDB2Settings = soloDB2.getSettings().toString();
			cluster.addDatabase(soloDB2);
			assertThat(cluster.size(), is(2));
			cluster.stop();
		}
		{
			DBDatabaseCluster cluster = new DBDatabaseCluster(
					nameOfCluster, DBDatabaseCluster.Configuration.fullyManual().withAutoConnect().withAutoStart()
			);
			System.out.println("" + cluster.getClusterStatus());
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
