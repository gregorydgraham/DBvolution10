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
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.internal.database.ClusterDetails;
import nz.co.gregs.dbvolution.utility.Timer;
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

	private static H2MemoryDB database;
	private static ClusterDetails clusterDetails;
	private static ActionQueueList list;

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
		list = clusterDetails.getMembers().getActionQueueList();
	}

	@After
	public void tearDown() {
		database.close();
	}

	@Test
	public void testStart() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);
		
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
		assertThat(actionQueue.hasStarted(), is(true));
	}

	@Test
	public void testAdd_DBAction() throws SQLException {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);
		
		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(10000l));
		assertThat(actionQueue.isEmpty(), is(false));
	}

	@Test
	public void testGetHeadOfQueue() throws Exception {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);
		
		assertThat(actionQueue.isEmpty(), is(true));
		final NoOpDBAction action = new NoOpDBAction(10000l);
		actionQueue.add(action);
		assertThat(actionQueue.isEmpty(), is(false));
		ActionMessage got = actionQueue.getHeadOfQueue();
		assertThat(got.getAction(), is(action));
	}

	@Test
	public void testIsEmpty() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		actionQueue.getHeadOfQueue();
		assertThat(actionQueue.isEmpty(), is(true));
	}

	@Test
	public void testStop() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
		assertThat(actionQueue.hasStarted(), is(true));
		actionQueue.stop();
		try {
			Thread.sleep(100);
		} catch (InterruptedException ex) {
			Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
		}
		assertThat(actionQueue.hasStarted(), is(false));
	}

	@Test
	public void testGetDatabase() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		System.out.println("DATABASE1: " + database.getSettings());
		System.out.println("DATABASE2: " + actionQueue.getDatabase().getSettings());
		assertThat(actionQueue.getDatabase(), is(database));
	}

	@Test
	public void testWaitUntilEmpty_0args() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		Timer timer = Timer.timer();
		actionQueue.start();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isEmpty(), is(false));
		actionQueue.waitUntilEmpty();
		timer.stop();
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(timer.duration(), is(greaterThan(300l)));
		assertThat(timer.duration(), is(lessThan(2000l)));
	}

	@Test
	public void testWaitUntilEmpty_long() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		actionQueue.add(new NoOpDBAction(100l));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasStarted(), is(false));
		Timer timer = Timer.start();
		actionQueue.start();
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
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
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
		actionQueue.waitUntilActionsAvailable();
		assertThat(actionQueue.hasActionsAvailable(), is(true));
		assertThat(actionQueue.isEmpty(), is(false));
	}

	@Test
	public void testWaitUntilUnpause() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
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
		Timer timer = Timer.start();
		actionQueue.waitUntilUnpause(1000l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(false));
		assertThat(timer.duration(), greaterThan(100l));
	}

	@Test
	public void testNotifyUnpause() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
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
		Timer timer = Timer.start();
		actionQueue.waitUntilUnpause(1000l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(false));
		assertThat(timer.duration(), greaterThan(100l));
	}

	@Test
	public void testNotifyQueueIsEmpty() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);
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
		Timer timer = Timer.start();
		emptyingThread.start();
		actionQueue.waitUntilEmpty(10000l);
		timer.end();
		assertThat(actionQueue.isEmpty(), is(false)); // because we didn't remove anything
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(1000l));
	}

	@Test
	public void testWaitUntilReady() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
		assertThat(actionQueue.hasStarted(), is(true));

		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasActionsAvailable(), is(true));

		Timer timer = Timer.start();
		actionQueue.waitUntilReady(10000l);
		timer.stop();
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasActionsAvailable(), is(false));
		assertThat(timer.duration(), greaterThan(299l));
		assertThat(timer.duration(), lessThan(10000l));
	}

	@Test
	public void testNotifyReady() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.add(new NoOpDBAction(100l));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(actionQueue.hasActionsAvailable(), is(true));

		Runnable notifyReadyRunnable = () -> {
			try {
				Thread.sleep(101l);
			} catch (InterruptedException ex) {
				Logger.getLogger(ActionQueueTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			actionQueue.notifyQueueIsReady();
		};
		Thread notifyReadyThread = new Thread(notifyReadyRunnable, "Unpause actionQueue");
		notifyReadyThread.start();
		Timer timer = Timer.start();
		actionQueue.waitUntilReady(1000l);
		timer.end();
		assertThat(actionQueue.isEmpty(), is(false)); // because we haven't started the AQ
		assertThat(timer.duration(), greaterThan(100l)); // because the notify thread waits for 100ms
		assertThat(timer.duration(), lessThan(1000l)); // because the wait didn't timeout
	}

	@Test
	public void testPause() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);
		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
		assertThat(actionQueue.hasStarted(), is(true));
		assertThat(actionQueue.isPaused(), is(false));
		actionQueue.pause();
		assertThat(actionQueue.isPaused(), is(true));
		assertThat(actionQueue.hasStarted(), is(true));// because paused and alive are different

		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		actionQueue.add(new NoOpDBAction());
		assertThat(actionQueue.isEmpty(), is(false));
		Timer timer = Timer.start();
		actionQueue.waitUntilUnpause(101l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(true));
		assertThat(actionQueue.isEmpty(), is(false));
		assertThat(timer.duration(), greaterThan(100l));// proof that it timed out
	}

	@Test
	public void testUnpause() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		assertThat(actionQueue.hasStarted(), is(false));
		actionQueue.start();
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
		Timer timer = Timer.start();
		unpauseThread.start();
		actionQueue.waitUntilUnpause(1000l);
		timer.end();
		assertThat(actionQueue.isPaused(), is(false));
		assertThat(timer.duration(), greaterThan(100l));
		assertThat(timer.duration(), lessThan(1000l));
	}

	@Test
	public void testClear() {
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

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
		ActionQueue actionQueue = new ActionQueue(database, clusterDetails, 100, list);

		assertThat(actionQueue.isEmpty(), is(true));
		final NoOpDBAction act1 = new NoOpDBAction(10000l);
		final NoOpDBAction act2 = new NoOpDBAction(10000l);
		final NoOpDBAction act3 = new NoOpDBAction(10000l);
		ActionQueue templateQueue = new ActionQueue(database, clusterDetails, 100, list);
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

}
