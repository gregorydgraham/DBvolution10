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
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.NoOpDBAction;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.utility.Timer;
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
		clusterDetails = new ClusterDetails("ActionQueueTest");
	}

	@After
	public void tearDown() {
//		database.close();
	}

	@Test
	public void testAdd() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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
	public void testQueueAction() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
		MatcherAssert.assertThat("AQL should start off empty", actionQueueList.size(), is(0));

		H2MemoryDB database = H2MemoryDB.createANewRandomDatabase();
		actionQueueList.add(database);
		final ActionQueue queue = actionQueueList.getQueueForDatabase(database);
		queue.waitUntilEmpty();
		queue.stop();

		final NoOpDBAction noOpDBAction = new NoOpDBAction(100l);
		actionQueueList.queueAction(database, noOpDBAction);

		MatcherAssert.assertThat(queue.hasActionsAvailable(), is(true));
		final ActionMessage headOfQueue = queue.getHeadOfQueue();
		final DBAction action = headOfQueue.getAction();
		MatcherAssert.assertThat(action, is(noOpDBAction));
	}

	@Test
	public void testRemove() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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
		Timer timer = Timer.timer();
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
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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

		final long delay = 50l;
		Timer timer = Timer.timer();
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(delay));
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(delay));
		actionQueueList.waitUntilAQueueIsReady();
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(true));
		MatcherAssert.assertThat(queue2.isEmpty(), is(false));
		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(delay * 2)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		timer = Timer.start();
		actionQueueList.unpause(db2);
		actionQueueList.waitUntilAQueueIsReady();
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(true));
		MatcherAssert.assertThat(queue2.isEmpty(), is(true));
		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(delay * 2)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		timer = Timer.start();
		actionQueueList.unpause(db3);
		actionQueueList.waitUntilAQueueIsReady();
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(true));
		MatcherAssert.assertThat(queue2.isEmpty(), is(true));
		MatcherAssert.assertThat(queue3.isEmpty(), is(true));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(delay * 2)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));
	}

	@Test
	public void testwaitUntilAQueueIsReady_int() throws SQLException {
		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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
		final long delay = 500l;
		final long waitTime = delay * 10;
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(delay));
		actionQueueList.queueActionForAllDatabases(new NoOpDBAction(delay));
		Timer timer = Timer.timer();
		// test that it times out
		actionQueueList.waitUntilAQueueIsReady(10l);
		timer.stop();
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(false));
		MatcherAssert.assertThat(queue2.isEmpty(), is(false));
		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		MatcherAssert.assertThat(queue1.size(), is(2));
		MatcherAssert.assertThat(queue2.size(), is(2));
		MatcherAssert.assertThat(queue3.size(), is(2));
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(10l)));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(delay)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		timer = Timer.start();
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
		MatcherAssert.assertThat(timer.duration(), is(greaterThan(delay * 2)));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(waitTime)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));

		timer = Timer.start();
		actionQueueList.unpause(db3);
		// test that it returns immediately in the already-ready case
		actionQueueList.waitUntilAQueueIsReady(delay * 3);
		timer.stop();

		MatcherAssert.assertThat(queue3.isEmpty(), is(false));
		System.out.println("DURATION: " + timer.duration());
		MatcherAssert.assertThat(queue1.isEmpty(), is(false));
		MatcherAssert.assertThat(queue2.isEmpty(), is(true));
		MatcherAssert.assertThat(timer.duration(), is(lessThan(delay)));
		MatcherAssert.assertThat("AQL should still have 3 queues", actionQueueList.size(), is(3));
	}

	@Test
	public void testWaitUntilReady_DBDatabase() throws SQLException {

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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
		Timer timer = Timer.timer();
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

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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
		Timer timer = Timer.timer();
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

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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

		ActionQueueList actionQueueList = new ActionQueueList(clusterDetails);
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

}
