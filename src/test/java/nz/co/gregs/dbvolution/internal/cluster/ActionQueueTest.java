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

import nz.co.gregs.dbvolution.actions.NoOpDBAction;
import java.sql.SQLException;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.internal.database.ClusterMember;
import nz.co.gregs.dbvolution.internal.database.DatabaseList;
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
public class ActionQueueTest {

	private static DBDatabase database;
	private static ClusterDetails clusterDetails;
	private static ClusterMember member;

	public ActionQueueTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
		try {
			database = new H2MemoryDB();
		} catch (SQLException exc) {
			exc.printStackTrace();
		}
		clusterDetails = new ClusterDetails("ActionQueueTest");
		member = new ClusterMember(clusterDetails, new DatabaseList(clusterDetails), database);
	}

	@After
	public void tearDown() {
		database.stop();
	}

	@Test
	public void testStart() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
	}

	@Test
	public void testAdd_DBAction() throws SQLException {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(10000l));
		assertThat(actionQueue.isEmpty(), is(false));
	}

	@Test
	public void testGetHeadOfQueue() throws Exception {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		final NoOpDBAction action = new NoOpDBAction(10000l);
		actionQueue.add(action);
		assertThat(actionQueue.isEmpty(), is(false));
		ActionMessage got = actionQueue.getHeadOfQueue();
		assertThat(got.getAction(), is(action));
	}

	@Test
	public void testIsEmpty() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		actionQueue.getHeadOfQueue();
		assertThat(actionQueue.isEmpty(), is(true));
	}

	@Test
	public void testStop() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		actionQueue.stopReader();
		try {
			Thread.sleep(100);
		} catch (InterruptedException ex) {
			Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
		}
		assertThat(actionQueue.hasStarted(), is(false));
	}

	@Test
	public void testGetDatabase() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		System.out.println("DATABASE1: " + database.getSettings());
		System.out.println("DATABASE2: " + actionQueue.getDatabase().getSettings());
		assertThat(actionQueue.getDatabase(), is(database));
	}

	@Test
	public void testWaitUntilEmpty_0args() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		StopWatch timer = StopWatch.stopwatch();
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isEmpty(), is(false));
		actionQueue.waitUntilEmpty();
		actionQueue.pause();
		timer.stop();
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(timer.duration(), is(greaterThan(300l)));
		assertThat(timer.duration(), is(lessThan(2000l)));
	}

	@Test
	public void testWaitUntilEmpty_long() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		StopWatch timer = StopWatch.start();
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isEmpty(), is(false));
		actionQueue.waitUntilEmpty(100);
		assertThat(actionQueue.isEmpty(), is(false));
		actionQueue.waitUntilEmpty(10000);
		timer.stop();
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(timer.duration(), is(greaterThan(300l)));
		assertThat(timer.duration(), is(lessThan(10000l)));
	}

	@Test
	public void testWaitUntilActionsAvailable() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isEmpty(), is(true));
		Instant then = Instant.now();
		actionQueue.waitUntilActionsAvailable(201l);
		Instant now = Instant.now();
		assertThat(now.toEpochMilli() - then.toEpochMilli(), greaterThan(200l));

		Runnable addThreeSlowActions = () -> {
			try {
				Thread.sleep(100l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.add(new NoOpDBAction(100l));
			actionQueue.add(new NoOpDBAction(100l));
			actionQueue.add(new NoOpDBAction(100l));
		};
		Thread addThread = new Thread(addThreeSlowActions, "Add 3 Actions");
		addThread.start();
		actionQueue.pause();
		actionQueue.waitUntilActionsAvailable();
		assertThat(actionQueue.hasActionsAvailable(), is(true));
		assertThat(actionQueue.isEmpty(), is(false));
	}

	@Test
	public void testWaitUntilUnpause() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isPaused(), is(false));
		actionQueue.pause();

		Runnable unpauseActionQueue = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.unpause();
		};
		Thread unpauseThread = new Thread(unpauseActionQueue, "Unpause actionQueue");
		unpauseThread.start();
		StopWatch timer = StopWatch.start();
		actionQueue.waitUntilUnpause(1000l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(false));
		assertThat(timer.duration(), greaterThan(100l));
	}

	@Test
	public void testNotifyUnpause() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isPaused(), is(false));

		Runnable unpauseActionQueue = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.notifyUNPAUSED();
		};
		Thread unpauseThread = new Thread(unpauseActionQueue, "Unpause actionQueue");
		unpauseThread.start();
		StopWatch timer = StopWatch.start();
		actionQueue.waitUntilUnpause(1000l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(false));
		assertThat(timer.duration(), greaterThan(100l));
	}

	@Test
	public void testNotifyQueueIsEmpty() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.add(new NoOpDBAction(10000l));

		Runnable notifyEmptyRunner = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.notifyQueueIsEmpty();
		};
		Thread emptyingThread = new Thread(notifyEmptyRunner, "Unpause actionQueue");
		StopWatch timer = StopWatch.start();
		emptyingThread.start();
		actionQueue.waitUntilEmpty(10000l);
		timer.end();
		assertThat(actionQueue.isEmpty(), is(false)); // because we didn't remove anything
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(1000l));
	}

	@Test
	public void testWaitUntilReady() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));

		actionQueue.add(new NoOpDBAction(5));
		actionQueue.add(new NoOpDBAction(5));
		actionQueue.add(new NoOpDBAction(5));
		actionQueue.add(new NoOpDBAction(5));
		actionQueue.add(new NoOpDBAction(5));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasActionsAvailable(), is(true));

		StopWatch timer = StopWatch.start();
		actionQueue.waitUntilReady(10000l);
		timer.stop();
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasActionsAvailable(), is(false));
		System.out.println("DURATION: " + timer.duration());
		assertThat(timer.duration(), greaterThan(25l));
		assertThat(timer.duration(), lessThan(10000l));
	}

	@Test
	public void testPause() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isPaused(), is(false));
		actionQueue.pause();
		assertThat(actionQueue.isPaused(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));

		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		StopWatch timer = StopWatch.start();
		actionQueue.waitUntilUnpause(101l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(true));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(timer.duration(), greaterThan(100l));// proof that it timed out
	}

	@Test
	public void testUnpause() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isPaused(), is(false));
		actionQueue.pause();

		Runnable unpauseActionQueue = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.unpause();
		};
		Thread unpauseThread = new Thread(unpauseActionQueue, "Unpause actionQueue");
		StopWatch timer = StopWatch.start();
		unpauseThread.start();
		actionQueue.waitUntilUnpause(1000l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(false));
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(1000l));
	}

	@Test
	public void testClear() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(10000l));
		actionQueue.add(new NoOpDBAction(10000l));
		actionQueue.add(new NoOpDBAction(10000l));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasActionsAvailable(), is(true));

		actionQueue.clear();
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasActionsAvailable(), is(false));
	}

	@Test
	public void testAddAll() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		final NoOpDBAction act1 = new NoOpDBAction(10000l);
		final NoOpDBAction act2 = new NoOpDBAction(10000l);
		final NoOpDBAction act3 = new NoOpDBAction(10000l);
		ActionQueue templateQueue = new ActionQueue(database, 100, member);
		templateQueue.add(act1);
		templateQueue.add(act2);
		templateQueue.add(act3);

		actionQueue.addAll(templateQueue);
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasActionsAvailable(), is(true));
		assertThat(actionQueue.getHeadOfQueue().getAction(), is(act1));
		assertThat(actionQueue.getHeadOfQueue().getAction(), is(act2));
		assertThat(actionQueue.getHeadOfQueue().getAction(), is(act3));
	}

	@Test
	public void testHasStarted() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.hasStarted(), is(false));

		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));

		actionQueue.stopReader();
		assertThat(actionQueue.hasStarted(), is(false));
	}

	@Test
	public void testSize() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.size(), is(0));

		actionQueue.pause();
		actionQueue.add(new NoOpDBAction(), new NoOpDBAction(), new NoOpDBAction());
		assertThat(actionQueue.size(), is(3));
	}

	@Test
	public void testAdd_DBActionArr() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(10000l), new NoOpDBAction(), new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.size(), is(3));
	}

	@Test
	public void testWaitUntilActionsAvailable_0args() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		Runnable runnable = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.add(new NoOpDBAction(100));
		};
		Thread addingActionsThread = new Thread(runnable, "add actions");

		StopWatch timer = StopWatch.stopwatch();
		addingActionsThread.start();
		actionQueue.waitUntilActionsAvailable();
		timer.stop();
		assertThat(timer.duration(), is(greaterThan(100l)));
		assertThat(timer.duration(), is(lessThan(1000l)));
	}

	@Test
	public void testWaitUntilActionsAvailable_long() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		Runnable runnable = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.add(new NoOpDBAction(100));
		};
		Thread notifyThread = new Thread(runnable, "add actions");

		StopWatch timer = StopWatch.stopwatch();
		notifyThread.start();
		actionQueue.waitUntilActionsAvailable(10);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(greaterThan(10l)));
		assertThat(timer.duration(), is(lessThan(1000l)));
		assertThat(actionQueue.size(), is(0));

		timer.restart();
		actionQueue.waitUntilActionsAvailable(1000);
		timer.stop();
		timer.report();
		assertThat(timer.duration(), is(greaterThan(0l)));
		assertThat(timer.duration(), is(lessThan(1000l)));
		assertThat(actionQueue.size(), is(1));
	}

	@Test
	public void testWaitUntilReady_0args() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isReady(), is(true));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		assertThat(actionQueue.isReady(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		StopWatch timer = StopWatch.stopwatch();
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isReady(), is(false));
		actionQueue.waitUntilReady();
		actionQueue.pause();
		timer.stop();
		assertThat(actionQueue.isReady(), is(true));
		assertThat(timer.duration(), is(greaterThan(300l)));
		assertThat(timer.duration(), is(lessThan(2000l)));
	}

	@Test
	public void testWaitUntilReady_long() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isReady(), is(true));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		assertThat(actionQueue.isReady(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		StopWatch timer = StopWatch.start();
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isReady(), is(false));
		actionQueue.waitUntilReady(100);
		assertThat(actionQueue.isReady(), is(false));
		actionQueue.waitUntilReady(10000);
		timer.stop();
		assertThat(actionQueue.isReady(), is(true));
		assertThat(timer.duration(), is(greaterThan(300l)));
		assertThat(timer.duration(), is(lessThan(10000l)));
	}

	@Test
	public void testClose() {

		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.hasStarted(), is(true));
		actionQueue.close();
		try {
			Thread.sleep(100);
		} catch (InterruptedException ex) {
			Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
		}
		assertThat(actionQueue.hasStarted(), is(false));
	}

	@Test
	public void testHasActionsAvailable() throws InterruptedException {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.hasActionsAvailable(), is(false));
		actionQueue.add(new NoOpDBAction(), new NoOpDBAction(), new NoOpDBAction());
		assertThat(actionQueue.hasActionsAvailable(), is(true));
		assertThat(actionQueue.size(), is(3));
		actionQueue.startReader();
		Thread.sleep(50l);
		assertThat(actionQueue.hasActionsAvailable(), is(false));

	}

	@Test
	public void testNotifyPAUSED() {

		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		assertThat(actionQueue.isPaused(), is(true));
		actionQueue.add(new NoOpDBAction(10000l));

		Runnable notifyPauseRunner = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.notifyPAUSED();
		};
		Thread emptyingThread = new Thread(notifyPauseRunner, "Pause actionQueue (but not really)");
		StopWatch timer = StopWatch.start();
		emptyingThread.start();
		actionQueue.waitUntilPaused(10000l);
		timer.end();
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(1000l));
	}

	@Test
	public void testNotifyUNPAUSED() {

		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		assertThat(actionQueue.isPaused(), is(true));
		actionQueue.add(new NoOpDBAction(10000l));

		Runnable notifyUnpauseRunner = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.notifyUNPAUSED();
		};
		Thread emptyingThread = new Thread(notifyUnpauseRunner, "Unpause actionQueue (but not really)");
		StopWatch timer = StopWatch.start();
		emptyingThread.start();
		actionQueue.waitUntilUnpaused(10000l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(true)); // because we didn't actual pause it
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(10000l));
	}

	@Test
	public void testIsPaused() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isPaused(), is(true));
		actionQueue.unpause();
		assertThat(actionQueue.isPaused(), is(false));		
		actionQueue.pause();
		assertThat(actionQueue.isPaused(), is(true));

	}

	@Test
	public void testIsReady() {
		ActionQueue actionQueue = new ActionQueue(database, 100, member);

		assertThat(actionQueue.isReady(), is(true));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isReady(), is(false));
		actionQueue.getHeadOfQueue();
		assertThat(actionQueue.isReady(), is(true));
	}

	@Test
	public void testWaitUntilPaused() {

		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		assertThat(actionQueue.isPaused(), is(true));
		actionQueue.startReader();
		assertThat(actionQueue.isPaused(), is(false));

		Runnable runner = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.pause();
		};
		Thread emptyingThread = new Thread(runner, "Pause actionQueue");
		StopWatch timer = StopWatch.start();
		emptyingThread.start();
		actionQueue.waitUntilPaused(10000l);
		timer.end();
		timer.report();
		assertThat(actionQueue.isPaused(), is(true));
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(10000l));
	}

	@Test
	public void testWaitUntilUnpaused() {

		ActionQueue actionQueue = new ActionQueue(database, 100, member);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.startReader();
		assertThat(actionQueue.isPaused(), is(false));
		actionQueue.pause();
		assertThat(actionQueue.isPaused(), is(true));

		Runnable runner = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.unpause();
		};
		Thread thread = new Thread(runner, "Unpause actionQueue ");
		StopWatch timer = StopWatch.start();
		thread.start();
		actionQueue.waitUntilUnpaused(10000l);
		timer.end();
		timer.report();
		assertThat(actionQueue.isPaused(), is(false));
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(10000l));
	}

}
