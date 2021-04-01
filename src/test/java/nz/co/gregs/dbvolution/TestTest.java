/*
 * Copyright 2021 Gregory Graham.
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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class TestTest extends AbstractTest {

	public TestTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQuery1() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQuery2() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQuery3() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQuery4() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQueryShouldAlwaysWork1() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(H2MemoryDB.createANewRandomDatabase())) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQueryShouldAlwaysWork2() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(H2MemoryDB.createANewRandomDatabase())) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQueryShouldAlwaysWork3() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(H2MemoryDB.createANewRandomDatabase())) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			cluster.addDatabase(soloDB2);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQueryFAILFAILFAIL1() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			H2MemoryDB soloDB3 = H2MemoryDB.createANewRandomDatabase();
			DBDatabaseCluster otherCluster = DBDatabaseCluster.randomManualCluster(soloDB2);
			otherCluster.addDatabase(soloDB3);
			cluster.addDatabase(otherCluster);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQueryFAILFAILFAIL2() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			H2MemoryDB soloDB3 = H2MemoryDB.createANewRandomDatabase();
			DBDatabaseCluster otherCluster = DBDatabaseCluster.randomManualCluster(soloDB2);
			otherCluster.addDatabase(soloDB3);
			cluster.addDatabase(otherCluster);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

	@Test
	public synchronized void testDatabaseRemovedAfterErrorInQueryFAILFAILFAIL3() throws SQLException {
		try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {
			H2MemoryDB soloDB2 = H2MemoryDB.createANewRandomDatabase();
			H2MemoryDB soloDB3 = H2MemoryDB.createANewRandomDatabase();
			DBDatabaseCluster otherCluster = DBDatabaseCluster.randomManualCluster(soloDB2);
			otherCluster.addDatabase(soloDB3);
			cluster.addDatabase(otherCluster);

			DBQuery query = cluster.getDBQuery(new Marque());
			query.setRawSQL("blart = norn");
			try {
				cluster.setPrintSQLBeforeExecuting(true);
				List<DBQueryRow> allRows = query.getAllRows();
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
				Assert.assertThat(cluster.size(), is(1));
			}
			cluster.setPrintSQLBeforeExecuting(false);
			Assert.assertThat(cluster.size(), is(1));
		}
	}

}
