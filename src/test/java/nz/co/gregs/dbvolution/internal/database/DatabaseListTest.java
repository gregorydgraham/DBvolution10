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
package nz.co.gregs.dbvolution.internal.database;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.actions.NoOpDBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueue;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueueList;
import nz.co.gregs.dbvolution.utility.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author gregorygraham
 */
public class DatabaseListTest {

	private ClusterDetails clusterDetails;
	private DBDatabase db1;
	private DBDatabase db2;
	private DBDatabase db3;

	public DatabaseListTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() throws SQLException {
		clusterDetails = new ClusterDetails("DatabaseListTest");
		db1 = H2MemoryDB.createANewRandomDatabase();
		db2 = H2MemoryDB.createANewRandomDatabase();
		db3 = H2MemoryDB.createANewRandomDatabase();

	}

	@After
	public void tearDown() {
	}

	@Test
	public void testSize() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		// DatabaseList is empty at creation
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1);
		// adding a database increases the size
		assertThat(databaseList.size(), is(1));

		databaseList.add(db2);
		// adding another database increases the size further
		assertThat(databaseList.size(), is(2));

		databaseList.remove(db2);
		// removing a database reduces the size
		assertThat(databaseList.size(), is(1));
	}

	@Test
	public void testIsEmpty() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		// DatabaseList is empty at creation
		assertThat(databaseList.isEmpty(), is(true));

		databaseList.add(db1);
		// adding a database stops the list being empty
		assertThat(databaseList.isEmpty(), is(false));

		databaseList.add(db2);
		// adding another database keeps isEmpty the same
		assertThat(databaseList.isEmpty(), is(false));

		databaseList.remove(db1);
		// removing a database does NOT make it empty
		assertThat(databaseList.isEmpty(), is(false));

		databaseList.remove(db2);
		// removing the last database DOES empty the list
		assertThat(databaseList.isEmpty(), is(true));
	}

	@Test
	public void testContains() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);

		// DatabaseList is empty at creation
		assertThat(databaseList.contains(db1), is(false));
		assertThat(databaseList.contains(db2), is(false));

		databaseList.add(db1);
		assertThat(databaseList.contains(db1), is(true));
		assertThat(databaseList.contains(db2), is(false));

		databaseList.add(db2);
		assertThat(databaseList.contains(db1), is(true));
		assertThat(databaseList.contains(db2), is(true));

		databaseList.remove(db1);
		assertThat(databaseList.contains(db1), is(false));
		assertThat(databaseList.contains(db2), is(true));

		databaseList.remove(db2);
		assertThat(databaseList.contains(db1), is(false));
		assertThat(databaseList.contains(db2), is(false));
	}

	@Test
	public void testIterator() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);

		// DatabaseList is empty at creation
		Iterator<DBDatabase> iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(false));

		databaseList.add(db1);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(true));
		DBDatabase next = iterator.next();
		assertThat(next.getSettings().toString(), is(db1.getSettings().toString()));
		assertThat(iterator.hasNext(), is(false));

		databaseList.add(db2);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(true));
		next = iterator.next();
		assertThat(next.getSettings().toString(), isOneOf(db1.getSettings().toString(), db2.getSettings().toString()));
		assertThat(iterator.hasNext(), is(true));
		next = iterator.next();
		assertThat(next.getSettings().toString(), isOneOf(db1.getSettings().toString(), db2.getSettings().toString()));
		assertThat(iterator.hasNext(), is(false));

		databaseList.remove(db1);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(true));
		next = iterator.next();
		assertThat(next.getSettings().toString(), is(db2.getSettings().toString()));
		assertThat(iterator.hasNext(), is(false));

		databaseList.remove(db2);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(false));
	}

	@Test
	public void testToArray_0args() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);

		// DatabaseList is empty at creation
		DBDatabase[] array = databaseList.toArray();
		assertThat(array.length, is(0));

		databaseList.add(db1);
		array = databaseList.toArray();
		assertThat(array.length, is(1));
		DBDatabase next = array[0];
		assertThat(next.getSettings().toString(), is(db1.getSettings().toString()));

		databaseList.add(db2);
		array = databaseList.toArray();
		assertThat(array.length, is(2));
		next = array[0];
		assertThat(next.getSettings().toString(), isOneOf(db1.getSettings().toString(), db2.getSettings().toString()));
		next = array[1];
		assertThat(next.getSettings().toString(), isOneOf(db1.getSettings().toString(), db2.getSettings().toString()));

		databaseList.remove(db1);
		array = databaseList.toArray();
		assertThat(array.length, is(1));
		next = array[0];
		assertThat(next.getSettings().toString(), is(db2.getSettings().toString()));

		databaseList.remove(db2);
		array = databaseList.toArray();
		assertThat(array.length, is(0));
	}

	@Test
	public void testToArray_DBDatabaseArr() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);

		DBDatabase[] array = new DBDatabase[2];

		// DatabaseList is empty at creation
		databaseList.toArray(array);
		assertThat(array.length, is(2));
		assertThat(array[0], nullValue());
		assertThat(array[1], nullValue());

		databaseList.add(db1);
		databaseList.toArray(array);
		assertThat(array.length, is(2));
		assertThat(array[0].getSettings().toString(), is(db1.getSettings().toString()));
		assertThat(array[1], nullValue());

		databaseList.add(db2);
		databaseList.toArray(array);
		assertThat(array.length, is(2));
		assertThat(array[0].getSettings().toString(), isOneOf(db1.getSettings().toString(), db2.getSettings().toString()));
		assertThat(array[1].getSettings().toString(), isOneOf(db1.getSettings().toString(), db2.getSettings().toString()));

		databaseList.remove(db1);
		databaseList.toArray(array);
		assertThat(array.length, is(2));
		assertThat(array[0].getSettings().toString(), is(db2.getSettings().toString()));
		assertThat(array[1], nullValue());

		databaseList.remove(db2);
		array = databaseList.toArray(array);
		assertThat(array[0], nullValue());
		assertThat(array[1], nullValue());

	}

	@Test
	public void testAdd() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1);
		assertThat(databaseList.size(), is(1));

		databaseList.add(db2);
		assertThat(databaseList.size(), is(2));
	}

	@Test
	public void testAdd_DBDatabaseArr() throws SQLException {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		assertThat(databaseList.size(), is(3));
	}

	@Test
	public void testRemove() throws SQLException {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		assertThat(databaseList.size(), is(3));

		databaseList.remove(db1);
		assertThat(databaseList.size(), is(2));
		assertThat(Arrays.asList(databaseList.getDatabases()), containsInAnyOrder(db2, db3));

		databaseList.remove(db3);
		assertThat(databaseList.size(), is(1));
		assertThat(Arrays.asList(databaseList.getDatabases()), containsInAnyOrder(db2));

		databaseList.remove(db3);
		assertThat(databaseList.size(), is(1));
		assertThat(Arrays.asList(databaseList.getDatabases()), containsInAnyOrder(db2));

		databaseList.remove(db2);
		assertThat(databaseList.size(), is(0));

		databaseList.remove(db2);
		assertThat(databaseList.size(), is(0));
	}

	@Test
	public void testContainsAll() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		final List<DBDatabase> listToCheck = Arrays.asList(new DBDatabase[]{db1, db2});

		assertThat(databaseList.containsAll(listToCheck), is(false));

		// test contains all
		databaseList.add(db1, db2, db3);
		assertThat(databaseList.size(), is(3));
		assertThat(databaseList.containsAll(listToCheck), is(true));
		// test contains some
		databaseList.remove(db1);
		assertThat(databaseList.containsAll(listToCheck), is(false));
		// test contains none
		databaseList.remove(db2);
		assertThat(databaseList.containsAll(listToCheck), is(false));
	}

	@Test
	public void testAddAll_Collection() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		final List<DBDatabase> listToCheck = Arrays.asList(new DBDatabase[]{db1, db2});

		databaseList.addAll(listToCheck);

		assertThat(databaseList.size(), is(2));
		assertThat(databaseList.containsAll(listToCheck), is(true));
	}

	@Test
	public void testRemoveAll() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		final List<DBDatabase> listToCheck = Arrays.asList(new DBDatabase[]{db1, db2});

		databaseList.addAll(listToCheck);
		databaseList.add(db3);
		databaseList.waitUntilSynchronised();

		assertThat(databaseList.size(), is(3));
		assertThat(databaseList.containsAll(listToCheck), is(true));

		databaseList.removeAll(listToCheck);

		assertThat(databaseList.size(), is(1));
		assertThat(databaseList.containsAll(listToCheck), is(false));
		assertThat(databaseList.contains(db3), is(true));

	}

	@Test
	public void testSetReady() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);

		DBDatabase[] readyDatabases = databaseList.getReadyDatabases();
		assertThat(readyDatabases.length, is(0));

		databaseList.setReady(db1);
		readyDatabases = databaseList.getReadyDatabases();
		assertThat(readyDatabases.length, is(1));
		assertThat(readyDatabases[0], is(db1));

	}

//	@Test
//	public void testSetUnsynchronised() {
//		DatabaseList databaseList = new DatabaseList(clusterDetails);
//		assertThat(databaseList.size(), is(0));
//		databaseList.add(db1, db2, db3);
//
//		List<DBDatabase> unsynched = databaseList.getDatabasesByStatusAsList(DBDatabaseCluster.Status.UNSYNCHRONISED);
//		assertThat(unsynched.size(), is(3));
//
//		databaseList.setReady(db1);
//		databaseList.setReady(db2);
//		databaseList.setReady(db3);
//
//		unsynched = databaseList.getDatabasesByStatusAsList(DBDatabaseCluster.Status.UNSYNCHRONISED);
//		assertThat(unsynched.size(), is(0));
//		assertThat(databaseList.getReadyDatabases().length, is(3));
//
//		databaseList.setUnsynchronised(db1);
//		unsynched = databaseList.getDatabasesByStatusAsList(DBDatabaseCluster.Status.UNSYNCHRONISED);
//		assertThat(unsynched.size(), is(1));
//		assertThat(unsynched.get(0), is(db1));
//
//		databaseList.setReady(db1);
//		unsynched = databaseList.getDatabasesByStatusAsList(DBDatabaseCluster.Status.UNSYNCHRONISED);
//		assertThat(unsynched.size(), is(0));
//	}

	@Test
	public void testSetPaused() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);

		List<DBDatabase> unsynched = databaseList.getDatabasesByStatusAsList(PAUSED);
		assertThat(unsynched.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING).size(), is(3));

		databaseList.setPaused(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(PAUSED);
		assertThat(unsynched.size(), is(1));
		assertThat(unsynched.get(0), is(db1));

		databaseList.setReady(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(PAUSED);
		assertThat(unsynched.size(), is(0));
	}

	@Test
	public void testSetDead() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);

		List<DBDatabase> unsynched = databaseList.getDatabasesByStatusAsList(DEAD);
		assertThat(unsynched.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING).size(), is(3));

		databaseList.setDead(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(DEAD);
		assertThat(unsynched.size(), is(1));
		assertThat(unsynched.get(0), is(db1));

		databaseList.setReady(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(DEAD);
		assertThat(unsynched.size(), is(0));
	}

	@Test
	public void testSetQuarantined() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);

		List<DBDatabase> unsynched = databaseList.getDatabasesByStatusAsList(QUARANTINED);
		assertThat(unsynched.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING).size(), is(3));

		databaseList.setQuarantined(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(QUARANTINED);
		assertThat(unsynched.size(), is(1));
		assertThat(unsynched.get(0), is(db1));

		databaseList.setReady(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(QUARANTINED);
		assertThat(unsynched.size(), is(0));
	}

	@Test
	public void testSetUnknown() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);
		final DBDatabaseCluster.Status desiredStatus = UNKNOWN;

		List<DBDatabase> unsynched = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unsynched.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING).size(), is(3));

		databaseList.setUnknown(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unsynched.size(), is(1));
		assertThat(unsynched.get(0), is(db1));

		databaseList.setReady(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unsynched.size(), is(0));
	}

	@Test
	public void testSetProcessing() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		final DBDatabaseCluster.Status desiredStatus = PROCESSING;

		List<DBDatabase> unsynched = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unsynched.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING).size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(READY).size(), is(3));

		databaseList.setProcessing(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unsynched.size(), is(1));
		assertThat(unsynched.get(0), is(db1));

		databaseList.setReady(db1);
		unsynched = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unsynched.size(), is(0));
		assertThat(databaseList.getReadyDatabases().length, is(3));
	}
	
	@Test
	public void testGetDatabases_0args() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		DBDatabase[] databases = databaseList.getDatabases();
		assertThat(databases.length, is(0));

		databaseList.add(db1, db2, db3);

		databases = databaseList.getDatabases();
		assertThat(databases.length, is(3));
		assertThat(Arrays.asList(databases), containsInAnyOrder(db1, db2, db3));
	}

	@Test
	public void testGetStatusOf() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);

		assertThat(databaseList.getStatusOf(db1), is(PROCESSING));

		databaseList.setReady(db1);
		assertThat(databaseList.getStatusOf(db1), is(READY));
	}

	@Test
	public void testIsReady() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);

		assertThat(databaseList.isReady(db1), is(false));

		databaseList.setReady(db1);
		assertThat(databaseList.isReady(db1), is(true));

		databaseList.setUnknown(db1);
		assertThat(databaseList.isReady(db1), is(false));

		databaseList.setProcessing(db1);
		assertThat(databaseList.isReady(db1), is(false));

		databaseList.setDead(db1);
		assertThat(databaseList.isReady(db1), is(false));

		databaseList.setPaused(db1);
		assertThat(databaseList.isReady(db1), is(false));

		databaseList.setQuarantined(db1);
		assertThat(databaseList.isReady(db1), is(false));

		databaseList.setUnsynchronised(db1);
		assertThat(databaseList.isReady(db1), is(false));

		databaseList.setReady(db1);
		assertThat(databaseList.isReady(db1), is(true));
	}

	@Test
	public void testGetReadyDatabases() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);

		assertThat(databaseList.getReadyDatabases().length, is(0));

		databaseList.setReady(db1);
		assertThat(databaseList.getReadyDatabases().length, is(1));
		assertThat(Arrays.asList(databaseList.getReadyDatabases()), containsInAnyOrder(db1));

		databaseList.setReady(db2);
		assertThat(databaseList.getReadyDatabases().length, is(2));
		assertThat(Arrays.asList(databaseList.getReadyDatabases()), containsInAnyOrder(db1, db2));

		databaseList.remove(db1);
		assertThat(databaseList.getReadyDatabases().length, is(1));
		assertThat(Arrays.asList(databaseList.getReadyDatabases()), containsInAnyOrder(db2));

		databaseList.setReady(db3);
		assertThat(databaseList.getReadyDatabases().length, is(2));
		assertThat(Arrays.asList(databaseList.getReadyDatabases()), containsInAnyOrder(db2, db3));
	}

	@Test
	public void testGetDatabasesByStatus() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		DBDatabase[] databases = databaseList.getDatabasesByStatus(PAUSED, PROCESSING);
		assertThat(databases.length, is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();

		databases = databaseList.getDatabasesByStatus(READY);
		assertThat(databases.length, is(3));
		assertThat(Arrays.asList(databases), containsInAnyOrder(db1, db2, db3));

		databaseList.queueAction(db1, new NoOpDBAction(100));
		databaseList.queueAction(db2, new NoOpDBAction(100));
		databaseList.queueAction(db3, new NoOpDBAction(100));
		databases = databaseList.getDatabasesByStatus(PAUSED, READY);
		assertThat(databases.length, is(0));

		databaseList.setReady(db1);
		databaseList.setPaused(db2);

		databases = databaseList.getDatabasesByStatus(PAUSED, PROCESSING);
		assertThat(databases.length, is(2));
		assertThat(Arrays.asList(databases), containsInAnyOrder(db2, db3));

		databases = databaseList.getDatabasesByStatus(READY, PROCESSING);
		assertThat(databases.length, is(2));
		assertThat(Arrays.asList(databases), containsInAnyOrder(db1, db3));
	}

	@Test
	public void testGetReadyDatabasesList() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(db1, db2, db3);
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db2, new NoOpDBAction(10));
		databaseList.queueAction(db3, new NoOpDBAction(10));
		
		assertThat(databaseList.getReadyDatabasesList().size(), is(0));

		databaseList.setReady(db1);
		assertThat(databaseList.getReadyDatabasesList().size(), is(1));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(db1));

		databaseList.setReady(db2);
		assertThat(databaseList.getReadyDatabasesList().size(), is(2));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(db1, db2));

		databaseList.remove(db1);
		assertThat(databaseList.getReadyDatabasesList().size(), is(1));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(db2));

		databaseList.setReady(db3);
		assertThat(databaseList.getReadyDatabasesList().size(), is(2));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(db2, db3));
	}

	@Test
	public void testGetDatabasesByStatusAsList() {

		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		List<DBDatabase> databases = databaseList.getDatabasesByStatusAsList(PAUSED, PROCESSING);
		assertThat(databases.size(), is(0));

		databaseList.add(db1, db2, db3);

		databases = databaseList.getDatabasesByStatusAsList(PAUSED, PROCESSING);
		assertThat(databases.size(), is(3));
		assertThat(databases, containsInAnyOrder(db1, db2, db3));

		databases = databaseList.getDatabasesByStatusAsList(PAUSED, READY);
		assertThat(databases.size(), is(0));

		databaseList.setReady(db1);
		databaseList.setPaused(db2);

		databases = databaseList.getDatabasesByStatusAsList(PAUSED, PROCESSING);
		assertThat(databases.size(), is(2));
		assertThat(databases, containsInAnyOrder(db2, db3));

		databases = databaseList.getDatabasesByStatusAsList(READY, PROCESSING);
		assertThat(databases.size(), is(2));
		assertThat(databases, containsInAnyOrder(db1, db3));
	}

	@Test
	public void testCountReadyDatabases() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		assertThat(databaseList.countReadyDatabases(), is(0));

		databaseList.add(db1, db2, db3);
		assertThat(databaseList.countReadyDatabases(), is(0));

		databaseList.setReady(db1);
		assertThat(databaseList.countReadyDatabases(), is(1));

		databaseList.setReady(db2);
		assertThat(databaseList.countReadyDatabases(), is(2));

		databaseList.setReady(db3);
		assertThat(databaseList.countReadyDatabases(), is(3));

		databaseList.setPaused(db1);
		assertThat(databaseList.countReadyDatabases(), is(2));
	}

	@Test
	public void testCountPausedDatabases() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		assertThat(databaseList.countPausedDatabases(), is(0l));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.countPausedDatabases(), is(0l));

		databaseList.setPaused(db1);
		assertThat(databaseList.countPausedDatabases(), is(1l));

		databaseList.setPaused(db2);
		assertThat(databaseList.countPausedDatabases(), is(2l));

		databaseList.setPaused(db3);
		assertThat(databaseList.countPausedDatabases(), is(3l));

		databaseList.setReady(db1);
		assertThat(databaseList.countPausedDatabases(), is(2l));
	}

	@Test
	public void testCountDatabases() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		assertThat(databaseList.countDatabases(PAUSED), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.countDatabases(READY), is(3));
		assertThat(databaseList.countDatabases(PAUSED), is(0));

		databaseList.setPaused(db1);
		assertThat(databaseList.countDatabases(READY), is(2));
		assertThat(databaseList.countDatabases(PAUSED), is(1));

		databaseList.setPaused(db2);
		assertThat(databaseList.countDatabases(READY), is(1));
		assertThat(databaseList.countDatabases(PAUSED), is(2));

		databaseList.setPaused(db3);
		assertThat(databaseList.countDatabases(READY), is(0));
		assertThat(databaseList.countDatabases(PAUSED), is(3));

		databaseList.setReady(db1);
		assertThat(databaseList.countDatabases(READY), is(1));
		assertThat(databaseList.countDatabases(PAUSED), is(2));
	}

	@Test
	public void testClear() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		assertThat(databaseList.size(), is(3));

		databaseList.clear();
		assertThat(databaseList.size(), is(0));
	}

	@Test
	public void testAreAllReady() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		assertThat(databaseList.size(), is(3));
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.setReady(db1);
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.setReady(db2);
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.setReady(db3);
		assertThat(databaseList.areAllReady(), is(true));
		databaseList.setPaused(db1);
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.remove(db1);
		assertThat(databaseList.areAllReady(), is(true));
	}

	@Test
	public void testIsDead() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		assertThat(databaseList.isDead(db1), is(false));

		databaseList.setDead(db1);
		assertThat(databaseList.isDead(db1), is(true));

		assertThat(databaseList.isDead(db2), is(false));
		databaseList.setQuarantined(db2);
		databaseList.setQuarantined(db2);
		databaseList.setQuarantined(db2);
		databaseList.setQuarantined(db2);
		databaseList.setQuarantined(db2);
		assertThat(databaseList.isDead(db2), is(true));
	}

	@Test
	public void testGetReadyDatabase_0args() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		try {
			databaseList.getReadyDatabase();
			assertThat(false, is(true));
		} catch (NoAvailableDatabaseException none) {
			assertThat(none, isA(NoAvailableDatabaseException.class));
		} catch (Exception other) {
			assertThat(false, is(true));
		}

		databaseList.setReady(db1);
		assertThat(databaseList.getReadyDatabase(), is(db1));

		databaseList.setReady(db2);
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2));

		databaseList.setReady(db3);
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
	}

	@Test
	public void testGetReadyDatabase_int() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(db1, db2, db3);
		// action queues are started automatically so we can expect one "ready"
		// notification additionally a SynchronisationAction as preloaded so we'll 
		// need that to finish before we get the "ready" notification

		var timer = StopWatch.stopwatch();
		try {
			databaseList.getReadyDatabase(100);
			assertThat(false, is(true));
		} catch (NoAvailableDatabaseException none) {
			timer.report();
			assertThat(none, isA(NoAvailableDatabaseException.class));
			assertThat(timer.duration(), greaterThan(99l));
			assertThat(timer.duration(), lessThan(200l));
		} catch (Exception other) {
			assertThat(false, is(true));
		}
		databaseList.setReady(db1);
		assertThat(timer.time(() -> databaseList.getReadyDatabase(100)), is(db1));
		assertThat(timer.duration(), lessThan(101l));

		databaseList.setReady(db2);
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(db1, db2));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(db1, db2));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(db1, db2));
		assertThat(timer.duration(), is(lessThan(11l)));

		databaseList.setReady(db3);
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(db1, db2, db3));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(db1, db2, db3));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(db1, db2, db3));
		assertThat(timer.duration(), is(lessThan(11l)));
	}

	@Test
	public void testWaitUntilSynchronised() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.countReadyDatabases(), is(3));
		assertThat(databaseList.getReadyDatabases().length, is(3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
	}

	@Test
	public void testWaitUntilDatabaseHasSynchonized() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilDatabaseHasSynchonized(db1);
		assertThat(databaseList.getStatusOf(db1), is(READY));
		assertThat(databaseList.countReadyDatabases(), is(greaterThan(0)));
		assertThat(databaseList.getReadyDatabases().length, is(greaterThan(0)));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(db1, db2, db3));
	}

	@Test
	public void testWaitUntilDatabaseHasSynchronized() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		StopWatch timer = StopWatch.stopwatch();
		timer.time(() -> databaseList.waitUntilDatabaseHasSynchronized(db1, 10));
		assertThat(timer.duration(), is(greaterThan(10l)));
		timer.time(() -> databaseList.waitUntilDatabaseHasSynchronized(db1, 100));
//		assertThat(timer.duration(), is(greaterThan(10l)));
		assertThat(timer.duration(), is(lessThan(100l)));
	}

	@Test
	public void testQueueAction() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(db1, db2, db3);
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));

		ActionQueue[] queues = databaseList.getActionQueueList().getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		databaseList.queueAction(db2, new NoOpDBAction(10));
		databaseList.queueAction(db2, new NoOpDBAction(10));
		databaseList.queueAction(db2, new NoOpDBAction(10));
		databaseList.queueAction(db2, new NoOpDBAction(10));
		databaseList.queueAction(db2, new NoOpDBAction(10));
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(5));
		assertThat(queues[2].size(), is(0));

		databaseList.queueAction(db3, new NoOpDBAction(10));
		databaseList.queueAction(db3, new NoOpDBAction(10));
		databaseList.queueAction(db3, new NoOpDBAction(10));
		databaseList.queueAction(db3, new NoOpDBAction(10));
		databaseList.queueAction(db3, new NoOpDBAction(10));
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(5));
		assertThat(queues[2].size(), is(5));

		databaseList.setUnpaused(db1);
		databaseList.waitUntilDatabaseHasSynchonized(db1);
		queues = databaseList.getActionQueueList().getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(5));
		assertThat(queues[2].size(), is(5));

		databaseList.setProcessing(db2);
		databaseList.setProcessing(db3);
		databaseList.waitUntilDatabaseHasSynchonized(db2);
		databaseList.waitUntilDatabaseHasSynchonized(db3);
		queues = databaseList.getActionQueueList().getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

	}

	@Test
	public void testCopyFromTo() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(db1, db2, db3);
		final NoOpDBAction act1 = new NoOpDBAction(10);
		final NoOpDBAction act5 = new NoOpDBAction(10);
		databaseList.queueAction(db1, act1);
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, act5);

		ActionQueue[] queues = databaseList.getActionQueueList().getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		databaseList.copyFromTo(db1, db2);
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(5));
		assertThat(queues[2].size(), is(0));
		assertThat(queues[0].getHeadOfQueue().getAction(), is(act1));
		assertThat(queues[1].getHeadOfQueue().getAction(), is(act1));
		queues[0].getHeadOfQueue();
		queues[0].getHeadOfQueue();
		queues[0].getHeadOfQueue();
		assertThat(queues[0].getHeadOfQueue().getAction(), is(act5));
		queues[1].getHeadOfQueue();
		queues[1].getHeadOfQueue();
		queues[1].getHeadOfQueue();
		assertThat(queues[1].getHeadOfQueue().getAction(), is(act5));
	}

	@Test
	public void testGetActionQueueList() {
		DatabaseList databaseList = new DatabaseList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(db1, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(db1, db2, db3);
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));
		databaseList.queueAction(db1, new NoOpDBAction(10));

		final ActionQueueList actionQueueList = databaseList.getActionQueueList();
		ActionQueue[] queues = actionQueueList.getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));
	}
}
