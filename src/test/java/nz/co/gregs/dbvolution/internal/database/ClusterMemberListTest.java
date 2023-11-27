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
import nz.co.gregs.dbvolution.DBDatabaseClusterTest;
import nz.co.gregs.dbvolution.actions.BrokenAction;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.NoOpDBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.databases.DBDatabaseHandle;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.generic.TempTest;
import nz.co.gregs.dbvolution.internal.cluster.ActionQueue;
import nz.co.gregs.looper.StopWatch;
import nz.co.gregs.regexi.Match;
import nz.co.gregs.regexi.Regex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class ClusterMemberListTest extends AbstractTest{

	private ClusterDetails clusterDetails;
	private DBDatabase db2;
	private DBDatabase db3;
	private ClusterMemberList databaseList;

	public ClusterMemberListTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		db2 = H2MemoryDB.createANewRandomDatabase();
		db2.getSettings().setLabel("DB2");
		db3 = H2MemoryDB.createANewRandomDatabase();
		db3.getSettings().setLabel("DB3");

		clusterDetails = new ClusterDetails("DatabaseListTest");
		databaseList = clusterDetails.getMembers();//new ClusterMemberList(clusterDetails);

	}

	@After
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		db2.stop();
		db3.stop();
	}

	@Test
	public void testSize() {
		// ClusterMemberList is empty at creation
		assertThat(databaseList.size(), is(0));

		databaseList.add(database);
		databaseList.waitOnStatusChange(database, 1000, READY);
		// adding a database increases the size
		assertThat(databaseList.size(), is(1));

		databaseList.add(db2);
		databaseList.waitOnStatusChange(db2, 1000, READY);
		// adding another database increases the size further
		assertThat(databaseList.size(), is(2));

		databaseList.remove(db2);
		// removing a database reduces the size
		assertThat(databaseList.size(), is(1));
	}

	@Test
	public void testIsEmpty() {
		// ClusterMemberList is empty at creation
		assertThat(databaseList.isEmpty(), is(true));

		databaseList.add(database);
		// adding a database stops the list being empty
		assertThat(databaseList.isEmpty(), is(false));

		databaseList.add(db2);
		// adding another database keeps isEmpty the same
		assertThat(databaseList.isEmpty(), is(false));

		databaseList.remove(database);
		// removing a database does NOT make it empty
		assertThat(databaseList.isEmpty(), is(false));

		databaseList.remove(db2);
		// removing the last database DOES empty the list
		assertThat(databaseList.isEmpty(), is(true));
	}

	@Test
	public void testContains() {

		// ClusterMemberList is empty at creation
		assertThat(databaseList.contains(database), is(false));
		assertThat(databaseList.contains(db2), is(false));

		databaseList.add(database);
		assertThat(databaseList.contains(database), is(true));
		assertThat(databaseList.contains(db2), is(false));

		databaseList.add(db2);
		assertThat(databaseList.contains(database), is(true));
		assertThat(databaseList.contains(db2), is(true));

		databaseList.remove(database);
		assertThat(databaseList.contains(database), is(false));
		assertThat(databaseList.contains(db2), is(true));

		databaseList.remove(db2);
		assertThat(databaseList.contains(database), is(false));
		assertThat(databaseList.contains(db2), is(false));
	}

	@Test
	public void testIterator() {

		// ClusterMemberList is empty at creation
		Iterator<ClusterMember> iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(false));

		databaseList.add(database);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(true));
		ClusterMember next = iterator.next();
		assertThat(next.getDatabase().getSettings().toString(), is(database.getSettings().toString()));
		assertThat(iterator.hasNext(), is(false));

		databaseList.add(db2);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(true));
		next = iterator.next();
		assertThat(next.getDatabase().getSettings().toString(), isOneOf(database.getSettings().toString(), db2.getSettings().toString()));
		assertThat(iterator.hasNext(), is(true));
		next = iterator.next();
		assertThat(next.getDatabase().getSettings().toString(), isOneOf(database.getSettings().toString(), db2.getSettings().toString()));
		assertThat(iterator.hasNext(), is(false));

		databaseList.remove(database);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(true));
		next = iterator.next();
		assertThat(next.getDatabase().getSettings().toString(), is(db2.getSettings().toString()));
		assertThat(iterator.hasNext(), is(false));

		databaseList.remove(db2);
		iterator = databaseList.iterator();
		assertThat(iterator.hasNext(), is(false));
	}

	@Test
	public void testToArray_0args() {

		// ClusterMemberList is empty at creation
		ClusterMember[] array = databaseList.toArray();
		assertThat(array.length, is(0));

		databaseList.add(database);
		array = databaseList.toArray();
		assertThat(array.length, is(1));
		ClusterMember next = array[0];
		assertThat(next.getDatabase().getSettings().toString(), is(database.getSettings().toString()));

		databaseList.add(db2);
		array = databaseList.toArray();
		assertThat(array.length, is(2));
		next = array[0];
		assertThat(next.getDatabase().getSettings().toString(), isOneOf(database.getSettings().toString(), db2.getSettings().toString()));
		next = array[1];
		assertThat(next.getDatabase().getSettings().toString(), isOneOf(database.getSettings().toString(), db2.getSettings().toString()));

		databaseList.remove(database);
		array = databaseList.toArray();
		assertThat(array.length, is(1));
		next = array[0];
		assertThat(next.getDatabase().getSettings().toString(), is(db2.getSettings().toString()));

		databaseList.remove(db2);
		array = databaseList.toArray();
		assertThat(array.length, is(0));
	}

	@Test
	public void testToArray_DBDatabaseArr() {

		// ClusterMemberList is empty at creation
		ClusterMember[] array = databaseList.toArray();
		assertThat(array.length, is(0));

		databaseList.add(database);
		array = databaseList.toArray();
		assertThat(array.length, is(1));
		assertThat(array[0].getDatabase().getSettings().toString(), is(database.getSettings().toString()));

		databaseList.add(db2);
		array = databaseList.toArray();
		assertThat(array.length, is(2));
		assertThat(array[0].getDatabase().getSettings().toString(), isOneOf(database.getSettings().toString(), db2.getSettings().toString()));
		assertThat(array[1].getDatabase().getSettings().toString(), isOneOf(database.getSettings().toString(), db2.getSettings().toString()));

		databaseList.remove(database);
		array = databaseList.toArray();
		assertThat(array.length, is(1));
		assertThat(array[0].getDatabase().getSettings().toString(), is(db2.getSettings().toString()));

		databaseList.remove(db2);
		array = databaseList.toArray();
		assertThat(array.length, is(0));

	}

	@Test
	public void testAdd() {
		assertThat(databaseList.size(), is(0));

		databaseList.add(database);
		assertThat(databaseList.size(), is(1));

		databaseList.add(db2);
		assertThat(databaseList.size(), is(2));
	}

	@Test
	public void testAdd_DBDatabaseArr() throws SQLException {
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		assertThat(databaseList.size(), is(3));
	}

	@Test
	public void testRemove() throws SQLException {
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		assertThat(databaseList.size(), is(3));

		databaseList.remove(database);
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
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		final List<DBDatabase> listToCheck = Arrays.asList(new DBDatabase[]{database, db2});

		assertThat(databaseList.containsAll(listToCheck), is(false));

		// test contains all
		databaseList.add(database, db2, db3);
		assertThat(databaseList.size(), is(3));
		assertThat(databaseList.containsAll(listToCheck), is(true));
		// test contains some
		databaseList.remove(database);
		assertThat(databaseList.containsAll(listToCheck), is(false));
		// test contains none
		databaseList.remove(db2);
		assertThat(databaseList.containsAll(listToCheck), is(false));
	}

	@Test
	public void testAddAll_Collection() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		final List<DBDatabase> listToCheck = Arrays.asList(new DBDatabase[]{database, db2});

		databaseList.addAll(listToCheck);

		assertThat(databaseList.size(), is(2));
		assertThat(databaseList.containsAll(listToCheck), is(true));
	}

	@Test
	public void testRemoveAll() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		final List<DBDatabase> listToCheck = Arrays.asList(new DBDatabase[]{database, db2});

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
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);
		databaseList.queueAction(database, new NoOpDBAction(2000));
		databaseList.queueAction(db2, new NoOpDBAction(2000));
		databaseList.queueAction(db3, new NoOpDBAction(2000));

		DBDatabase[] readyDatabases = databaseList.getReadyDatabases();
		assertThat(readyDatabases.length, is(0));

		databaseList.setReady(database);
		readyDatabases = databaseList.getReadyDatabases();
		assertThat(readyDatabases.length, is(1));
		assertThat(readyDatabases[0], is(database));

	}

	@Test
	public void testSetPaused() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);
		databaseList.queueAction(database, new NoOpDBAction(2000));
		databaseList.queueAction(db2, new NoOpDBAction(2000));
		databaseList.queueAction(db3, new NoOpDBAction(2000));

		List<DBDatabase> paused = databaseList.getDatabasesByStatusAsList(PAUSED);
		assertThat(paused.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING, SYNCHRONIZING).size(), is(3));

		databaseList.setPaused(database);
		paused = databaseList.getDatabasesByStatusAsList(PAUSED);
		assertThat(paused.size(), is(1));
		assertThat(paused.get(0), is(database));

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		paused = databaseList.getDatabasesByStatusAsList(PAUSED);
		assertThat(paused.size(), is(0));
	}

	@Test
	public void testSetDead() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);

		List<DBDatabase> dead = databaseList.getDatabasesByStatusAsList(DEAD);
		assertThat(dead.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING, READY, SYNCHRONIZING).size(), is(3));

		databaseList.setDead(database);
		dead = databaseList.getDatabasesByStatusAsList(DEAD);
		assertThat(dead.size(), is(1));
		assertThat(dead.get(0), is(database));

		databaseList.setPaused(database);
		databaseList.setProcessing(database);
		databaseList.setReady(database);
		dead = databaseList.getDatabasesByStatusAsList(DEAD);
		assertThat(dead.size(), is(0));
	}

	@Test
	public void testSetQuarantined() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);

		List<DBDatabase> quarantined = databaseList.getDatabasesByStatusAsList(QUARANTINED);
		assertThat(quarantined.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING, READY, SYNCHRONIZING).size(), is(3));

		databaseList.setQuarantined(database);
		quarantined = databaseList.getDatabasesByStatusAsList(QUARANTINED);
		assertThat(quarantined.size(), is(1));
		assertThat(quarantined.get(0), is(database));

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		quarantined = databaseList.getDatabasesByStatusAsList(QUARANTINED);
		assertThat(quarantined.size(), is(0));
	}

	@Test
	public void testSetUnknown() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);
		final DBDatabaseCluster.Status desiredStatus = UNKNOWN;

		List<DBDatabase> unknown = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unknown.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING, READY, SYNCHRONIZING).size(), is(3));

		databaseList.setUnknown(database);
		unknown = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unknown.size(), is(1));
		assertThat(unknown.get(0), is(database));

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		unknown = databaseList.getDatabasesByStatusAsList(desiredStatus);
		assertThat(unknown.size(), is(0));
	}

	@Test
	public void testSetProcessing() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();

		List<DBDatabase> processingMembers = databaseList.getDatabasesByStatusAsList(PROCESSING);
		assertThat(processingMembers.size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(PROCESSING).size(), is(0));
		assertThat(databaseList.getDatabasesByStatusAsList(READY).size(), is(3));

		databaseList.setProcessing(database);
		processingMembers = databaseList.getDatabasesByStatusAsList(PROCESSING);
		assertThat(processingMembers.size(), is(1));
		assertThat(processingMembers.get(0), is(database));

		databaseList.setReady(database);
		processingMembers = databaseList.getDatabasesByStatusAsList(PROCESSING);
		assertThat(processingMembers.size(), is(0));
		assertThat(databaseList.getReadyDatabases().length, is(3));
	}

	@Test
	public void testGetDatabases_0args() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		DBDatabase[] databases = databaseList.getDatabases();
		assertThat(databases.length, is(0));

		databaseList.add(database, db2, db3);

		databases = databaseList.getDatabases();
		assertThat(databases.length, is(3));
		assertThat(Arrays.asList(databases), containsInAnyOrder(database, db2, db3));
	}

	@Test
	public void testGetStatusOf() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);

		assertThat(databaseList.getStatusOf(database), isOneOf(PROCESSING, READY, SYNCHRONIZING));

		databaseList.setReady(database);
		assertThat(databaseList.getStatusOf(database), is(READY));
	}

	@Test
	public void testIsReady() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilDatabaseHasSynchonized(database);

		assertThat(databaseList.isReady(database), is(true));

		databaseList.setUnknown(database);
		assertThat(databaseList.isReady(database), is(false));

		databaseList.setProcessing(database);
		assertThat(databaseList.isReady(database), is(false));

		databaseList.setDead(database);
		assertThat(databaseList.isReady(database), is(false));

		databaseList.setPaused(database);
		assertThat(databaseList.isReady(database), is(false));

		databaseList.setQuarantined(database);
		assertThat(databaseList.isReady(database), is(false));

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		assertThat(databaseList.isReady(database), is(true));
	}

	@Test
	public void testGetReadyDatabases() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.getReadyDatabases().length, is(3));

		databaseList.setPaused(db3);
		assertThat(databaseList.getReadyDatabases().length, is(2));
		assertThat(Arrays.asList(databaseList.getReadyDatabases()), containsInAnyOrder(database, db2));

		databaseList.setPaused(db2);
		assertThat(databaseList.getReadyDatabases().length, is(1));
		assertThat(Arrays.asList(databaseList.getReadyDatabases()), containsInAnyOrder(database));

		databaseList.remove(database);
		assertThat(databaseList.getReadyDatabases().length, is(0));

		databaseList.setProcessing(db3);
		databaseList.setReady(db3);
		assertThat(databaseList.getReadyDatabases().length, is(1));
		assertThat(Arrays.asList(databaseList.getReadyDatabases()), containsInAnyOrder(db3));
	}

	@Test
	public void testGetDatabasesByStatus() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		DBDatabase[] databases = databaseList.getDatabasesByStatus(PAUSED, PROCESSING);
		assertThat(databases.length, is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();

		databases = databaseList.getDatabasesByStatus(READY);
		assertThat(databases.length, is(3));
		assertThat(Arrays.asList(databases), containsInAnyOrder(database, db2, db3));

		databaseList.queueAction(database, new NoOpDBAction(2000));
		databaseList.queueAction(db2, new NoOpDBAction(2000));
		databaseList.queueAction(db3, new NoOpDBAction(2000));
		databases = databaseList.getDatabasesByStatus(PAUSED, READY);
		assertThat(databases.length, is(0));

		databaseList.setReady(database);
		databaseList.setPaused(db2);

		databases = databaseList.getDatabasesByStatus(PAUSED, PROCESSING);
		assertThat(databases.length, is(2));
		assertThat(Arrays.asList(databases), containsInAnyOrder(db2, db3));

		databases = databaseList.getDatabasesByStatus(READY, PROCESSING);
		assertThat(databases.length, is(2));
		assertThat(Arrays.asList(databases), containsInAnyOrder(database, db3));
	}

	@Test
	public void testGetReadyDatabasesList() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));
		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(database, db2, db3);
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(db2, new NoOpDBAction(10));
		databaseList.queueAction(db3, new NoOpDBAction(10));

		assertThat(databaseList.getReadyDatabasesList().size(), is(0));

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		assertThat(databaseList.getReadyDatabasesList().size(), is(1));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(database));

		databaseList.setProcessing(db2);
		databaseList.setReady(db2);
		assertThat(databaseList.getReadyDatabasesList().size(), is(2));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(database, db2));

		databaseList.remove(database);
		assertThat(databaseList.getReadyDatabasesList().size(), is(1));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(db2));

		databaseList.setProcessing(db3);
		databaseList.setReady(db3);
		assertThat(databaseList.getReadyDatabasesList().size(), is(2));
		assertThat(databaseList.getReadyDatabasesList(), containsInAnyOrder(db2, db3));
	}

	@Test
	public void testGetDatabasesByStatusAsList() {

//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		List<DBDatabase> databases = databaseList.getDatabasesByStatusAsList(PAUSED, PROCESSING);
		assertThat(databases.size(), is(0));

		databaseList.add(database, db2, db3);

		databases = databaseList.getDatabasesByStatusAsList(PAUSED, PROCESSING, READY, SYNCHRONIZING);
		assertThat(databases.size(), is(3));
		assertThat(databases, containsInAnyOrder(database, db2, db3));

		databases = databaseList.getDatabasesByStatusAsList(PAUSED);
		assertThat(databases.size(), is(0));

		databaseList.setReady(database);
		databaseList.setPaused(db2);
		databaseList.setProcessing(db3);

		databases = databaseList.getDatabasesByStatusAsList(PAUSED, PROCESSING, SYNCHRONIZING);
		assertThat(databases.size(), is(2));
		assertThat(databases, containsInAnyOrder(db2, db3));

		databases = databaseList.getDatabasesByStatusAsList(READY, PROCESSING, SYNCHRONIZING);
		assertThat(databases.size(), is(2));
		assertThat(databases, containsInAnyOrder(database, db3));
	}

	@Test
	public void testCountReadyDatabases() {
		assertThat(databaseList.size(), is(0));

		assertThat(databaseList.countReadyDatabases(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.countReadyDatabases(), is(3));

		databaseList.setPaused(database);
		assertThat(databaseList.countReadyDatabases(), is(2));

		databaseList.setPaused(db2);
		assertThat(databaseList.countReadyDatabases(), is(1));

		databaseList.setPaused(db3);
		assertThat(databaseList.countReadyDatabases(), is(0));

		databaseList.setProcessing(database);
		databaseList.setReady(database);

		databaseList.setProcessing(db2);
		databaseList.setReady(db2);
		assertThat(databaseList.countReadyDatabases(), is(2));
	}

	@Test
	public void testCountPausedDatabases() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		assertThat(databaseList.countPausedDatabases(), is(0l));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.countPausedDatabases(), is(0l));

		databaseList.setPaused(database);
		assertThat(databaseList.countPausedDatabases(), is(1l));

		databaseList.setPaused(db2);
		assertThat(databaseList.countPausedDatabases(), is(2l));

		databaseList.setPaused(db3);
		assertThat(databaseList.countPausedDatabases(), is(3l));

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		assertThat(databaseList.countPausedDatabases(), is(2l));
	}

	@Test
	public void testCountDatabases() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		assertThat(databaseList.countDatabases(PAUSED), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.countDatabases(READY), is(3));
		assertThat(databaseList.countDatabases(PAUSED), is(0));

		databaseList.setPaused(database);
		assertThat(databaseList.countDatabases(READY), is(2));
		assertThat(databaseList.countDatabases(PAUSED), is(1));

		databaseList.setPaused(db2);
		assertThat(databaseList.countDatabases(READY), is(1));
		assertThat(databaseList.countDatabases(PAUSED), is(2));

		databaseList.setPaused(db3);
		assertThat(databaseList.countDatabases(READY), is(0));
		assertThat(databaseList.countDatabases(PAUSED), is(3));

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		assertThat(databaseList.countDatabases(READY), is(1));
		assertThat(databaseList.countDatabases(PAUSED), is(2));
	}

	@Test
	public void testClear() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		assertThat(databaseList.size(), is(3));

		databaseList.clear();
		assertThat(databaseList.size(), is(0));
	}

	@Test
	public void testAreAllReady() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		assertThat(databaseList.size(), is(3));
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.setReady(database);
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.setReady(db2);
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.setReady(db3);
		assertThat(databaseList.areAllReady(), is(true));
		databaseList.setPaused(database);
		assertThat(databaseList.areAllReady(), is(false));
		databaseList.remove(database);
		assertThat(databaseList.areAllReady(), is(true));
	}

	@Test
	public void testIsDead() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		assertThat(databaseList.isDead(database), is(false));

		databaseList.setDead(database);
		assertThat(databaseList.isDead(database), is(true));

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
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.setPaused(database, db2, db3);
		try {
			databaseList.getReadyDatabase();
			assertThat(false, is(true));
		} catch (NoAvailableDatabaseException none) {
			assertThat(none, isA(NoAvailableDatabaseException.class));
		} catch (Exception other) {
			assertThat(false, is(true));
		}

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		assertThat(databaseList.getReadyDatabase(), is(database));

		databaseList.setProcessing(db2);
		databaseList.setReady(db2);
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2));

		databaseList.setProcessing(db3);
		databaseList.setReady(db3);
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
	}

	@Test
	public void testGetReadyDatabase_int() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(database, db2, db3);
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

		databaseList.setProcessing(database);
		databaseList.setReady(database);
		assertThat(timer.time(() -> databaseList.getReadyDatabase(100)), is(database));
		assertThat(timer.duration(), lessThan(101l));

		databaseList.setProcessing(db2);
		databaseList.setReady(db2);
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(database, db2));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(database, db2));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(database, db2));
		assertThat(timer.duration(), is(lessThan(11l)));

		databaseList.setProcessing(db3);
		databaseList.setReady(db3);
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(database, db2, db3));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(database, db2, db3));
		assertThat(timer.duration(), is(lessThan(11l)));
		assertThat(timer.time(() -> databaseList.getReadyDatabase(10)), isOneOf(database, db2, db3));
		assertThat(timer.duration(), is(lessThan(11l)));
	}

	@Test
	public void testWaitUntilSynchronised() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		assertThat(databaseList.countReadyDatabases(), is(3));
		assertThat(databaseList.getReadyDatabases().length, is(3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
	}

	@Test
	public void testWaitUntilDatabaseHasSynchonized() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilDatabaseHasSynchonized(database);
		assertThat(databaseList.getStatusOf(database), is(READY));
		assertThat(databaseList.countReadyDatabases(), is(greaterThan(0)));
		assertThat(databaseList.getReadyDatabases().length, is(greaterThan(0)));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
		assertThat(databaseList.getReadyDatabase(), isOneOf(database, db2, db3));
	}

	@Test
	public void testWaitUntilDatabaseHasSynchronized() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		StopWatch timer = StopWatch.stopwatch();
		timer.time(() -> databaseList.waitUntilDatabaseHasSynchronized(database, 10));
		assertThat(timer.duration(), is(greaterThanOrEqualTo(10l)));
		timer.time(() -> databaseList.waitUntilDatabaseHasSynchronized(database, 100));
		assertThat(timer.duration(), is(lessThan(101l)));
	}

	@Test
	public void testQueueAction() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised(10000);
		databaseList.setPaused(database, db2, db3);
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));

		ActionQueue[] queues = databaseList.getActionQueues(database, db2, db3);
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

		databaseList.setUnpaused(database);
		databaseList.waitUntilDatabaseHasSynchonized(database);
		queues = databaseList.getActionQueues(database, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(5));
		assertThat(queues[2].size(), is(5));

		databaseList.setProcessing(db2);
		databaseList.setProcessing(db3);
		databaseList.waitUntilDatabaseHasSynchonized(db2);
		databaseList.waitUntilDatabaseHasSynchonized(db3);
		queues = databaseList.getActionQueues(database, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

	}

	@Test
	public void testCopyFromTo() {
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(database, db2, db3);
		final NoOpDBAction act1 = new NoOpDBAction(10);
		final NoOpDBAction act5 = new NoOpDBAction(10);
		databaseList.queueAction(database, act1);
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, act5);

		ActionQueue[] queues = databaseList.getActionQueues(database, db2, db3);
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		databaseList.copyFromTo(database, db2);
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
//		ClusterMemberList databaseList = new ClusterMemberList(clusterDetails);
		assertThat(databaseList.size(), is(0));

		databaseList.add(database, db2, db3);
		databaseList.waitUntilSynchronised();
		databaseList.setPaused(database, db2, db3);
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));
		databaseList.queueAction(database, new NoOpDBAction(10));

		final ActionQueue[] queues = databaseList.getActionQueues(database, db2, db3);
		assertThat(queues[0].size(), is(5));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));
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
	public void testDatabaseRemovedFromClusterWillResynchronise() throws SQLException {
		Regex allActiveRegex = Regex
				.multiline()
				.literal("READY Databases: 2 of 2").newline()
				.anyCharacter().oneOrMore().literal("QUARANTINED Databases: 0 of 2").endRegex();
		Regex only1Active = Regex
				.multiline()
				.namedCapture("ready").literal("READY Databases: 1 of 2").endNamedCapture()
				.anyCharacter().oneOrMore().space().namedCapture("other").word().literal(" Databases: 1 of 2").endNamedCapture().endRegex();

		try {
			try (DBDatabaseCluster cluster = DBDatabaseCluster.randomCluster(DBDatabaseCluster.Configuration.autoReconnect(), database)) {
				H2MemorySettingsBuilder dbBuilder = new H2MemorySettingsBuilder().setLabel("Member 2").setDatabaseName("Member2");
				var member2 = new BrokenDatabase(dbBuilder.getDBDatabase());
				cluster.setPrintSQLBeforeExecuting(true);
				assertThat(cluster.addDatabaseAndWait(member2), is(true));
				assertThat(cluster.size(), is(2));

				var statuses = cluster.getClusterStatusSnapshot();
				assertThat(statuses.getMembers().size(), is(2));
				for (ClusterDetails.MemberSnapshot member : statuses.getMembers()) {
					assertThat(member.status, is(DBDatabaseCluster.Status.READY));
				}

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
				member2.useBrokenBehaviour = false;
				assertThat(cluster.size(), is(1));

				clusterStatus = cluster.getClusterStatus();
				System.out.println(clusterStatus);
				for (Match match : only1Active.getAllMatches(clusterStatus)) {
					System.out.println("MATCH: " + match);
				}
				System.out.println("CAPTURE ready: " + only1Active.getAllNamedCapturesOfFirstMatchWithinString(clusterStatus).get("ready"));
				System.out.println("CAPTURE other: " + only1Active.getAllNamedCapturesOfFirstMatchWithinString(clusterStatus).get("other"));
				only1Active.testAgainst(clusterStatus);
				assertTrue(only1Active.matchesWithinString(clusterStatus));
				
				assertTrue(cluster.waitUntilSynchronised(5000));
				
				statuses = cluster.getClusterStatusSnapshot();
				assertThat(statuses.getMembers().size(), is(2));
				assertThat(statuses.getByStatus(DBDatabaseCluster.Status.READY).size(), is(2));
				for (ClusterDetails.MemberSnapshot member : statuses.getMembers()) {
					assertThat(member.status, is(DBDatabaseCluster.Status.READY));
				}
				for (ClusterDetails.MemberSnapshot member : statuses.getByDatabase(database, member2)) {
					assertThat(member.status, is(DBDatabaseCluster.Status.READY));
				}
			}
		} catch (SQLException ex) {
			Logger.getLogger(TempTest.class.getName()).log(Level.SEVERE, null, ex);
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
