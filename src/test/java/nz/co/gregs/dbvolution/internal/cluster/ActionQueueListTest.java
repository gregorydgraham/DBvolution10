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
package nz.co.gregs.dbvolution.internal.cluster;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.NoOpDBAction;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.internal.database.DatabaseList;
import nz.co.gregs.dbvolution.utility.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.hamcrest.MatcherAssert;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author gregorygraham
 */
public class ActionQueueListTest {

	private static ClusterDetails clusterDetails;
	private DatabaseList databaseList;

	public ActionQueueListTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
		clusterDetails = new ClusterDetails("ActionQueueListTest");
		databaseList = new DatabaseList(clusterDetails);
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testAdd_DBDatabase() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		MatcherAssert.assertThat("AQL should start off empty", actionQueueList.size(), is(0));
		final H2MemoryDB createANewRandomDatabase = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(createANewRandomDatabase);
		MatcherAssert.assertThat("AQL should have 1 database now", actionQueueList.size(), is(1));
		MatcherAssert.assertThat("AQL should have 1 database now", actionQueueList.size(), is(1));
		actionQueueList.add(H2MemoryDB.createANewRandomDatabase());
		MatcherAssert.assertThat("AQL should have 2 databases now", actionQueueList.size(), is(2));
		actionQueueList.add(H2MemoryDB.createANewRandomDatabase());
		MatcherAssert.assertThat("AQL should have 3 databases now", actionQueueList.size(), is(3));
	}

	@Test
	public void testAdd_DBDatabaseArr() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		MatcherAssert.assertThat("AQL should start off empty", actionQueueList.size(), is(0));

		actionQueueList.add(
				H2MemoryDB.createANewRandomDatabase(),
				H2MemoryDB.createANewRandomDatabase(),
				H2MemoryDB.createANewRandomDatabase()
		);
		MatcherAssert.assertThat("AQL should have 3 databases now", actionQueueList.size(), is(3));
	}

	@Test
	public void testRemove() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		H2MemoryDB db = H2MemoryDB.createANewRandomDatabase();
		ActionQueue removed = actionQueueList.remove(db);
		MatcherAssert.assertThat(removed, is(nullValue()));
		actionQueueList.add(db);
		removed = actionQueueList.remove(db);
		MatcherAssert.assertThat(removed, is(not(nullValue())));
		MatcherAssert.assertThat(removed.hasStarted(), is(false));
		removed = actionQueueList.remove(db);
		MatcherAssert.assertThat(removed, is(nullValue()));
	}

	@Test
	public void testClear() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		MatcherAssert.assertThat("AQL should start off empty", actionQueueList.size(), is(0));
		actionQueueList.add(H2MemoryDB.createANewRandomDatabase());
		actionQueueList.add(H2MemoryDB.createANewRandomDatabase());
		actionQueueList.add(H2MemoryDB.createANewRandomDatabase());
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));

		actionQueueList.clear();
		MatcherAssert.assertThat("AQL should again be empty", actionQueueList.size(), is(0));
	}

	@Test
	public void testWaitUntilAllAreEmpty() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1);
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db2);
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		ActionQueue[] queuesForDatabases = actionQueueList.getQueueForDatabase(db1, db2, db3);
		ActionQueue queue1 = queuesForDatabases[0];
		ActionQueue queue2 = queuesForDatabases[1];
		ActionQueue queue3 = queuesForDatabases[2];

		final long delay = 50l;
		StopWatch timer = StopWatch.stopwatch();
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(delay));
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(delay));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(true));
		MatcherAssert.assertThat(queue2.isEmpty(), is(true));
		MatcherAssert.assertThat(queue3.isEmpty(), is(true));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(delay * 2)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));
	}

	@Test
	public void testwaitUntilAQueueIsReady_0args() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		actionQueueList.pause(db2, db3);

		final long actionDelay = 50l;
		final int numberOfActions = 2;
		final long expectedMinimumDelay = actionDelay * numberOfActions;

		StopWatch timer = StopWatch.stopwatch();
		for (int i = 0; i < numberOfActions; i++) {
			actionQueueList.queueActionForAllDatabases(new NoOpDBAction(actionDelay));
		}
		actionQueueList.waitUntilAQueueIsReady();
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(true));
		MatcherAssert.assertThat(queue2.isEmpty(), is(false));
		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(expectedMinimumDelay)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		actionQueueList.pause(db1);
		actionQueueList.queueAction(db1, new NoOpDBAction());
		timer = StopWatch.start();
		actionQueueList.unpause(db2);
		actionQueueList.waitUntilAQueueIsReady();
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(false));
		MatcherAssert.assertThat(queue2.isEmpty(), is(true));
		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(expectedMinimumDelay)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		actionQueueList.pause(db2);
		actionQueueList.queueAction(db2, new NoOpDBAction());
		timer = StopWatch.start();
		actionQueueList.unpause(db3);
		actionQueueList.waitUntilAQueueIsReady();
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(false));
		MatcherAssert.assertThat(queue2.isEmpty(), is(false));
		MatcherAssert.assertThat(queue3.isEmpty(), is(true));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(expectedMinimumDelay)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));
	}

	@Test
	public void testwaitUntilAQueueIsReady_int() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		actionQueueList.pause(db1, db2, db3);

		// need waitTime to be large because the code actually takes a while to run
		// so have a big delay, and multiple it by a ridiculous scale
		// to ensure good results: 500 * 10 works well
		final long actionDelay = 500l;
		final long waitTime = actionDelay * 10;
		final long reallyShortWait = actionDelay / 50 + 1;// really short
		final int numberOfActions = 2;
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(actionDelay));
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(actionDelay));
		StopWatch timer = StopWatch.stopwatch();
		// test that it times out
		actionQueueList.waitUntilAQueueIsReady(reallyShortWait);
		timer.stop();
		timer.report();
		MatcherAssert.assertThat(queue1.isEmpty(), is(false));
		MatcherAssert.assertThat(queue2.isEmpty(), is(false));
		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(queue1.size(), is(2));
		MatcherAssert.assertThat(queue2.size(), is(2));
		MatcherAssert.assertThat(queue3.size(), is(2));
		MatcherAssert.assertThat(timer.duration(), is(greaterThanOrEqualTo(reallyShortWait)));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(actionDelay)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		timer = StopWatch.start();
		actionQueueList.unpause(db2);
		// test that it completes
		boolean success = actionQueueList.waitUntilAQueueIsReady(waitTime);
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(success, is(true));
		MatcherAssert.assertThat(queue1.isEmpty(), is(false));
		MatcherAssert.assertThat(queue2.isEmpty(), is(true));
		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(queue1.size(), is(2));
		MatcherAssert.assertThat(queue2.size(), is(0));
		MatcherAssert.assertThat(queue3.size(), is(2));
		MatcherAssert.assertThat(timer.duration(), is(greaterThanOrEqualTo(actionDelay * numberOfActions)));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(waitTime)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		// test that it returns immediately in the already-ready case
		timer = StopWatch.start();
		actionQueueList.unpause(db3);
		actionQueueList.waitUntilAQueueIsReady(actionDelay * 3);
		timer.stop();
		actionQueueList.pauseAll();

		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(queue1.isEmpty(), is(false));
		MatcherAssert.assertThat(queue2.isEmpty(), is(true));
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(timer.duration(), is(lessThan(actionDelay)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));
	}

	@Test
	public void testWaitUntilReady_DBDatabase() throws SQLException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		assertThat(queue1.isEmpty(), is(true));
		assertThat(queue2.isEmpty(), is(true));
		assertThat(queue3.isEmpty(), is(true));
		actionQueueList.pause(db1, db2, db3);

		// check that it returns immediately if the queue is already READY
		StopWatch timer = StopWatch.stopwatch();
		actionQueueList.waitUntilReady(db1);
		timer.stop();
		MatcherAssert.assertThat(timer.duration(), is(lessThan(100l)));

		// Check that it returns after the queue has emptied
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(20), new NoOpDBAction(30));
		timer.restart();
		actionQueueList.unpause(db1);
		actionQueueList.waitUntilReady(db1);
		timer.stop();
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(50l)));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(5000l)));
	}

	@Test
	public void testWaitUntilReady_DBDatabase_long() throws SQLException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		assertThat(queue1.isEmpty(), is(true));
		assertThat(queue2.isEmpty(), is(true));
		assertThat(queue3.isEmpty(), is(true));
		actionQueueList.pause(db1, db2, db3);

		// check that it returns immediately if the queue is already READY
		StopWatch timer = StopWatch.stopwatch();
		actionQueueList.waitUntilReady(db1, 200l);
		timer.stop();
		MatcherAssert.assertThat(timer.duration(), is(lessThan(200l)));

		// Check that it returns after the queue has emptied
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(20), new NoOpDBAction(30));
		timer.restart();
		actionQueueList.unpause(db1);
		actionQueueList.waitUntilReady(db1, 500l);
		timer.stop();
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(50l)));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(600l)));

		// Check that it returns if the actions take too long
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(2000), new NoOpDBAction(3000));
		timer.restart();
		actionQueueList.unpause(db1);
		actionQueueList.waitUntilReady(db1, 500l);
		timer.stop();
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(50l)));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(600l)));
	}

	@Test
	public void testPause() throws SQLException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		assertThat(queue1.isEmpty(), is(true));
		assertThat(queue2.isEmpty(), is(true));
		assertThat(queue3.isEmpty(), is(true));

		assertThat(queue1.isPaused(), is(false));
		assertThat(queue2.isPaused(), is(false));
		assertThat(queue3.isPaused(), is(false));

		actionQueueList.pause(db1);
		assertThat(queue1.isPaused(), is(true));
		assertThat(queue2.isPaused(), is(false));
		assertThat(queue3.isPaused(), is(false));

		actionQueueList.pause(db2, db3);
		assertThat(queue1.isPaused(), is(true));
		assertThat(queue2.isPaused(), is(true));
		assertThat(queue3.isPaused(), is(true));
	}

	@Test
	public void testUnpause() throws SQLException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		assertThat(queue1.isEmpty(), is(true));
		assertThat(queue2.isEmpty(), is(true));
		assertThat(queue3.isEmpty(), is(true));

		assertThat(queue1.isPaused(), is(false));
		assertThat(queue2.isPaused(), is(false));
		assertThat(queue3.isPaused(), is(false));

		actionQueueList.pause(db1, db2, db3);
		assertThat(queue1.isPaused(), is(true));
		assertThat(queue2.isPaused(), is(true));
		assertThat(queue3.isPaused(), is(true));

		actionQueueList.unpause(db1);
		assertThat(queue1.isPaused(), is(false));
		assertThat(queue2.isPaused(), is(true));
		assertThat(queue3.isPaused(), is(true));

		actionQueueList.unpause(db2, db3);
		assertThat(queue1.isPaused(), is(false));
		assertThat(queue2.isPaused(), is(false));
		assertThat(queue3.isPaused(), is(false));

	}

	@Test
	public void testCopyFromTo() throws SQLException, InterruptedException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];

		actionQueueList.pause(db1);
		actionQueueList.pause(db2);

		queue1.add(new NoOpDBAction(50), new NoOpDBAction(100), new NoOpDBAction(200));
		assertThat(queue1.size(), is(3));
		assertThat(queue2.size(), is(0));
		assertThat(queue3.size(), is(0));

		actionQueueList.copyFromTo(db1, db2);
		assertThat(queue1.size(), is(3));
		assertThat(queue2.size(), is(3));
		assertThat(queue3.size(), is(0));

		actionQueueList.copyFromTo(db1, db3);
		// check that it's copied
		assertThat(queue3.size(), is(greaterThan(0)));
		// check that it's the only active queue
		actionQueueList.waitUntilAQueueIsReady(2500);
		assertThat(queue1.size(), is(3));
		assertThat(queue2.size(), is(3));
		assertThat(queue3.size(), is(0));

	}

	@Test
	public void testStart() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1);
		assertThat(actionQueueList.getQueueForDatabase(db1).hasStarted(), is(true));
		actionQueueList.stop(db1);
		StopWatch.sleepFor(10); // give it time to work
		assertThat(actionQueueList.getQueueForDatabase(db1).hasStarted(), is(false));
		actionQueueList.start(db1);
		assertThat(actionQueueList.getQueueForDatabase(db1).hasStarted(), is(true));

		actionQueueList.getQueueForDatabase(db2);
		assertThat(actionQueueList.getQueueForDatabase(db2).hasStarted(), is(true));
		actionQueueList.start(db2);
		assertThat(actionQueueList.getQueueForDatabase(db1).hasStarted(), is(true));
		actionQueueList.stop(db2);
		StopWatch.sleepFor(10); // give it time to work
		assertThat(actionQueueList.getQueueForDatabase(db2).hasStarted(), is(false));
	}

	@Test
	public void testStop() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1);
		assertThat(actionQueueList.getQueueForDatabase(db1).hasStarted(), is(true));
		actionQueueList.stop(db1);
		assertThat(actionQueueList.getQueueForDatabase(db1).hasStarted(), is(false));
		actionQueueList.start(db1);
		assertThat(actionQueueList.getQueueForDatabase(db1).hasStarted(), is(true));
	}

	@Test
	public void testSize() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		assertThat(actionQueueList.size(), is(0));
		actionQueueList.add(db1);
		assertThat(actionQueueList.size(), is(1));
		actionQueueList.getQueueForDatabase(db1);
		assertThat(actionQueueList.size(), is(1));
		actionQueueList.getQueueForDatabase(db2);
		assertThat(actionQueueList.size(), is(2));
		actionQueueList.add(db3);
		assertThat(actionQueueList.size(), is(3));
		actionQueueList.remove(db1);
		assertThat(actionQueueList.size(), is(2));

	}

	@Test
	public void testQueueActionForAllDatabases_DBAction() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2, db3);

		NoOpDBAction act1 = new NoOpDBAction();
		NoOpDBAction act2 = new NoOpDBAction();
		NoOpDBAction act3 = new NoOpDBAction();

		ActionQueue[] queues = actionQueueList.getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		actionQueueList.queueActionForAllDatabases(act1);
		assertThat(queues[0].size(), is(1));
		assertThat(queues[1].size(), is(1));
		assertThat(queues[2].size(), is(1));

		actionQueueList.queueActionForAllDatabases(act2);
		assertThat(queues[0].size(), is(2));
		assertThat(queues[1].size(), is(2));
		assertThat(queues[2].size(), is(2));

		actionQueueList.queueActionForAllDatabases(act3);
		assertThat(queues[0].size(), is(3));
		assertThat(queues[1].size(), is(3));
		assertThat(queues[2].size(), is(3));

	}

	@Test
	public void testQueueActionForAllDatabases_DBActionArr() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2, db3);

		NoOpDBAction act1 = new NoOpDBAction();
		NoOpDBAction act2 = new NoOpDBAction();
		NoOpDBAction act3 = new NoOpDBAction();

		ActionQueue[] queues = actionQueueList.getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		actionQueueList.queueActionForAllDatabases(act1, act2, act3);
		assertThat(queues[0].size(), is(3));
		assertThat(queues[1].size(), is(3));
		assertThat(queues[2].size(), is(3));

	}

	@Test
	public void testWaitUntilAllQueuesAreEmpty() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2, db3);

		NoOpDBAction act1 = new NoOpDBAction(50);
		NoOpDBAction act2 = new NoOpDBAction(50);
		NoOpDBAction act3 = new NoOpDBAction(50);

		ActionQueue[] queues = actionQueueList.getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		actionQueueList.queueActionForAllDatabases(act1, act2, act3);
		assertThat(queues[0].size(), is(3));
		assertThat(queues[1].size(), is(3));
		assertThat(queues[2].size(), is(3));

		actionQueueList.unpause(db1, db2, db3);
		actionQueueList.waitUntilAllQueuesAreEmpty();
		actionQueueList.pause(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));
	}

	@Test
	public void testWaitUntilAQueueIsReady_0args() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2, db3);

		NoOpDBAction actFast = new NoOpDBAction(50);
		NoOpDBAction actSlow = new NoOpDBAction(1000);

		ActionQueue[] queues = actionQueueList.getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		actionQueueList.queueAction(db1, actFast, actFast, actFast);
		actionQueueList.queueAction(db2, actSlow, actSlow, actSlow);
		actionQueueList.queueAction(db3, actSlow, actSlow, actSlow);
		assertThat(queues[0].size(), is(3));
		assertThat(queues[1].size(), is(3));
		assertThat(queues[2].size(), is(3));

		actionQueueList.unpause(db1, db2, db3);
		actionQueueList.waitUntilAQueueIsReady();
		actionQueueList.pause(db1, db2, db3);
		// AssertThat one of the queues is size zero
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(greaterThan(0)));
		assertThat(queues[2].size(), is(greaterThan(0)));
	}

	@Test
	public void testWaitUntilAQueueIsReady_long() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2, db3);

		NoOpDBAction actFast = new NoOpDBAction(50);
		NoOpDBAction actSlow = new NoOpDBAction(1000);

		ActionQueue[] queues = actionQueueList.getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(0));
		assertThat(queues[2].size(), is(0));

		actionQueueList.queueAction(db1, actFast, actFast, actFast);
		actionQueueList.queueAction(db2, actSlow, actSlow, actSlow, actSlow, actSlow);
		actionQueueList.queueAction(db3, actSlow, actSlow, actSlow, actSlow, actSlow);
		assertThat(queues[0].size(), is(3));
		assertThat(queues[1].size(), is(5));
		assertThat(queues[2].size(), is(5));

		StopWatch timer = StopWatch.stopwatch();
		actionQueueList.unpause(db1, db2, db3);
		actionQueueList.waitUntilAQueueIsReady(2);
		actionQueueList.pause(db1, db2, db3);
		timer.report();
		// AssertThat one of the queues is size zero
		assertThat(queues[0].size(), is(greaterThan(0)));
		assertThat(queues[1].size(), is(greaterThan(0)));
		assertThat(queues[2].size(), is(greaterThan(0)));
		assertThat(timer.duration(), is(lessThan(1000l)));

		timer.restart();
		actionQueueList.unpause(db1, db2, db3);
		actionQueueList.waitUntilAQueueIsReady(2500);
		actionQueueList.pause(db1, db2, db3);
		timer.report();
		// AssertThat one of the queues is size zero
		assertThat(queues[0].size(), is(0));
		assertThat(queues[1].size(), is(greaterThan(0)));
		assertThat(queues[2].size(), is(greaterThan(0)));
		assertThat(timer.duration(), is(lessThan(2500l)));
	}

	@Test
	public void testNotifyAQueueIsReady() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2, db3);

		Runnable runnable = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueueList.notifyAQueueIsEmpty(db1);
		};
		Thread thread = new Thread(runnable, "notify ready");

		StopWatch timer = StopWatch.stopwatch();
		thread.start();
		actionQueueList.waitUntilAQueueIsReady();
		timer.stop();
		assertThat(timer.duration(), is(greaterThan(100l)));
		assertThat(timer.duration(), is(lessThan(1000l)));
	}

	@Test
	public void testPause_DBDatabase() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db3);

		NoOpDBAction actFast = new NoOpDBAction(5);
		NoOpDBAction actSlow = new NoOpDBAction(1000);

		actionQueueList.queueAction(db1, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db2, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db3, actFast, actFast, actFast, actFast, actFast);

		StopWatch timer = StopWatch.stopwatch();
		actionQueueList.waitUntilReady(db3, 100l);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(greaterThan(99l)));
		assertThat(timer.duration(), is(lessThan(200l)));
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(5));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(0));

		timer.restart();
		actionQueueList.unpause(db3);
		actionQueueList.waitUntilReady(db3, 100l);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(lessThan(100l)));
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(0));
	}

	@Test
	public void testUnpause_DBDatabase() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db3);

		NoOpDBAction actFast = new NoOpDBAction(5);
		NoOpDBAction actSlow = new NoOpDBAction(1000);

		actionQueueList.queueAction(db1, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db2, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db3, actFast, actFast, actFast, actFast, actFast);

		StopWatch timer = StopWatch.stopwatch();
		final long delay = 100l;
		actionQueueList.waitUntilReady(db3, delay);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(greaterThan(delay - 1)));
		assertThat(timer.duration(), is(lessThan(delay * 2)));
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(5));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(0));

		timer.restart();
		actionQueueList.unpause(db3);
		actionQueueList.waitUntilReady(db3, delay);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(lessThan(delay - 1)));
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(0));
	}

	@Test
	public void testUnpause_DBDatabaseArr() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2);

		NoOpDBAction actFast = new NoOpDBAction(5);
		NoOpDBAction actSlow = new NoOpDBAction(1000);

		actionQueueList.queueAction(db1, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db2, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db3, actFast, actFast, actFast, actFast, actFast);

		StopWatch timer = StopWatch.stopwatch();
		final long delay = 100l;
		actionQueueList.waitUntilReady(db1, delay);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(greaterThanOrEqualTo(delay)));
		assertThat(timer.duration(), is(lessThan(delay * 2)));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(5));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(5));
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(0));

		timer.restart();
		actionQueueList.unpause(db1, db2);
		actionQueueList.waitUntilReady(db1, delay * 10);// DON'T want a timeout
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(lessThan(delay * 2))); // DIDN'T timeout?
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(0));
	}

	@Test
	public void testGetKey() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		String key1 = actionQueueList.getKey(db1);
		String key2 = actionQueueList.getKey(db2);
		String key3 = actionQueueList.getKey(db3);

		assertThat(key1, allOf(not(key2), not(key3)));
		assertThat(key2, allOf(not(key1), not(key3)));
		assertThat(key3, allOf(not(key1), not(key2)));

		assertThat(key1, is(actionQueueList.getKey(db1)));
		assertThat(key2, is(actionQueueList.getKey(db2)));
		assertThat(key3, is(actionQueueList.getKey(db3)));
	}

	@Test
	public void testGetQueueForDatabase_DBDatabase() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);

		ActionQueue queue1 = actionQueueList.getQueueForDatabase(db1);
		ActionQueue queue2 = actionQueueList.getQueueForDatabase(db2);
		ActionQueue queue3 = actionQueueList.getQueueForDatabase(db3);
		assertThat(queue1.getDatabase().getSettings().toString(), is(db1.getSettings().toString()));
		assertThat(queue2.getDatabase().getSettings().toString(), is(db2.getSettings().toString()));
		assertThat(queue3.getDatabase().getSettings().toString(), is(db3.getSettings().toString()));
	}

	@Test
	public void testPause_DBDatabaseArr() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(db1, db2, db3);
		actionQueueList.pause(db1, db2, db3);

		NoOpDBAction actFast = new NoOpDBAction(5);
		NoOpDBAction actSlow = new NoOpDBAction(1000);

		actionQueueList.queueAction(db1, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db2, actFast, actFast, actFast, actFast, actFast);
		actionQueueList.queueAction(db3, actFast, actFast, actFast, actFast, actFast);

		StopWatch timer = StopWatch.stopwatch();
		final long delay = 100l;
		actionQueueList.waitUntilReady(db3, delay);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(greaterThanOrEqualTo(delay)));
		assertThat(timer.duration(), is(lessThan(delay * 2)));
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(5));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(5));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(5));

		timer.restart();
		actionQueueList.unpause(db1, db2, db3);
		actionQueueList.waitUntilAllQueuesAreEmpty();
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(lessThan(delay)));
		assertThat(actionQueueList.getQueueForDatabase(db3).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db1).size(), is(0));
		assertThat(actionQueueList.getQueueForDatabase(db2).size(), is(0));
	}

	@Test
	public void testGetQueueForDatabase_DBDatabaseArr() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);

		ActionQueue[] queues = actionQueueList.getQueueForDatabase(db1, db2, db3);
		assertThat(queues[0].getDatabase().getSettings().toString(), is(db1.getSettings().toString()));
		assertThat(queues[1].getDatabase().getSettings().toString(), is(db2.getSettings().toString()));
		assertThat(queues[2].getDatabase().getSettings().toString(), is(db3.getSettings().toString()));
	}

	@Test
	public void testQueueAction_DBDatabase_DBAction() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		MatcherAssert.assertThat("AQL should start off empty", actionQueueList.size(), is(0));

		H2MemoryDB database = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(database);
		final ActionQueue queue = actionQueueList.getQueueForDatabase(database);
		queue.waitUntilEmpty();
		queue.stopReader();

		// can insert the first action
		NoOpDBAction noOpDBAction = new NoOpDBAction(100l);
		actionQueueList.queueAction(database, noOpDBAction);
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		ActionMessage headOfQueue = queue.getHeadOfQueue();
		DBAction action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(false));

		// can insert another action
		noOpDBAction = new NoOpDBAction(100l);
		actionQueueList.queueAction(database, noOpDBAction);
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		headOfQueue = queue.getHeadOfQueue();
		action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(false));

		// can insert 2 actions at once
		noOpDBAction = new NoOpDBAction(100l);
		NoOpDBAction noOpDBAction2 = new NoOpDBAction(100l);
		actionQueueList.queueAction(database, noOpDBAction);
		actionQueueList.queueAction(database, noOpDBAction2);
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		headOfQueue = queue.getHeadOfQueue();
		action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		headOfQueue = queue.getHeadOfQueue();
		action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction2));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(false));
	}

	@Test
	public void testQueueAction_DBDatabase_DBActionArr() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		MatcherAssert.assertThat("AQL should start off empty", actionQueueList.size(), is(0));

		H2MemoryDB database = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(database);
		final ActionQueue queue = actionQueueList.getQueueForDatabase(database);
		queue.waitUntilEmpty();
		queue.stopReader();

		// can insert 2 actions at once
		NoOpDBAction noOpDBAction = new NoOpDBAction(100l);
		NoOpDBAction noOpDBAction2 = new NoOpDBAction(100l);
		actionQueueList.queueAction(database, noOpDBAction);
		actionQueueList.queueAction(database, noOpDBAction2);
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		ActionMessage headOfQueue = queue.getHeadOfQueue();
		DBAction action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		headOfQueue = queue.getHeadOfQueue();
		action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction2));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(false));

		// can insert 2 more actions
		noOpDBAction = new NoOpDBAction(100l);
		noOpDBAction2 = new NoOpDBAction(100l);
		actionQueueList.queueAction(database, noOpDBAction);
		actionQueueList.queueAction(database, noOpDBAction2);
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		headOfQueue = queue.getHeadOfQueue();
		action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		headOfQueue = queue.getHeadOfQueue();
		action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction2));
		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(false));
	}

	@Test
	public void testUnpauseAll() throws SQLException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		assertThat(queue1.isEmpty(), is(true));
		assertThat(queue2.isEmpty(), is(true));
		assertThat(queue3.isEmpty(), is(true));

		assertThat(queue1.isPaused(), is(false));
		assertThat(queue2.isPaused(), is(false));
		assertThat(queue3.isPaused(), is(false));

		actionQueueList.pauseAll();
		assertThat(queue1.isPaused(), is(true));
		assertThat(queue2.isPaused(), is(true));
		assertThat(queue3.isPaused(), is(true));

		actionQueueList.unpauseAll();
		assertThat(queue1.isPaused(), is(false));
		assertThat(queue2.isPaused(), is(false));
		assertThat(queue3.isPaused(), is(false));
	}

	@Test
	public void testPauseAll() throws SQLException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails, databaseList);
		final H2MemoryDB db1 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db2 = H2MemoryDB.createANewRandomDatabase();
		final H2MemoryDB db3 = H2MemoryDB.createANewRandomDatabase();

		actionQueueList.add(db1, db2, db3);
		MatcherAssert.assertThat("AQL should now have 3 queues", actionQueueList.size(), is(3));
		actionQueueList.waitUntilAllQueuesAreEmpty();
		final ActionQueue[] queue = actionQueueList.getQueueForDatabase(db1, db2, db3);
		final ActionQueue queue1 = queue[0];
		final ActionQueue queue2 = queue[1];
		final ActionQueue queue3 = queue[2];
		assertThat(queue1.isEmpty(), is(true));
		assertThat(queue2.isEmpty(), is(true));
		assertThat(queue3.isEmpty(), is(true));

		assertThat(queue1.isPaused(), is(false));
		assertThat(queue2.isPaused(), is(false));
		assertThat(queue3.isPaused(), is(false));

		actionQueueList.pauseAll();
		assertThat(queue1.isPaused(), is(true));
		assertThat(queue2.isPaused(), is(true));
		assertThat(queue3.isPaused(), is(true));
	}
}
